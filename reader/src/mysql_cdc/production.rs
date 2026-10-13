use std::{sync::Arc, time::Duration};

use relus_common::{
    cdc::{BufferedCdcRow, CdcMessage, CdcRowMessage},
    PipelineMessage,
};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

use super::{
    checkpoint::{acknowledge_and_checkpoint, CheckpointIdentity, RedbCheckpointStore},
    error::{ReplicationError, ReplicationErrorKind},
    transaction::TransactionAssembler,
    MySqlReplicationClient, TableRule,
};

pub struct ForwardReplicationOptions<'a> {
    pub checkpoint_store: Arc<RedbCheckpointStore>,
    pub checkpoint_id: &'a str,
    pub checkpoint_identity: &'a CheckpointIdentity,
    pub table_rules: &'a [TableRule],
    pub byte_capacity: Arc<Semaphore>,
    pub max_message_bytes: usize,
    pub heartbeat_timeout: Duration,
    pub sender: mpsc::Sender<anyhow::Result<PipelineMessage>>,
    pub cancellation: &'a CancellationToken,
}

pub async fn forward_replication_client<C: MySqlReplicationClient + ?Sized>(
    client: &mut C,
    options: ForwardReplicationOptions<'_>,
) -> Result<usize, ReplicationError> {
    let ForwardReplicationOptions {
        checkpoint_store,
        checkpoint_id,
        checkpoint_identity,
        table_rules,
        byte_capacity,
        max_message_bytes,
        heartbeat_timeout,
        sender,
        cancellation,
    } = options;
    checkpoint_store
        .validate_identity(checkpoint_id, checkpoint_identity)
        .map_err(checkpoint_error)?;
    let mut sequence = checkpoint_store
        .load(checkpoint_id)
        .map_err(checkpoint_error)?
        .map(|checkpoint| checkpoint.sequence)
        .unwrap_or(0);
    let mut assembler = TransactionAssembler::default();
    let mut forwarded = 0;
    client.connect().await?;

    let forwarding = async {
        loop {
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => break,
                result = tokio::time::timeout(heartbeat_timeout, client.next_event()) => {
                    result.map_err(|_| ReplicationError::new(
                        ReplicationErrorKind::HeartbeatTimeout,
                        true,
                        "replication heartbeat timeout elapsed",
                    ))??
                },
            };
            let Some(event) = next else {
                if client.is_finite_stream() {
                    break;
                }
                return Err(ReplicationError::new(
                    ReplicationErrorKind::Network,
                    true,
                    "replication stream ended before cancellation",
                ));
            };

            for message in assembler.push(event)? {
                match message {
                    CdcMessage::Row(envelope) => {
                        let Some(rule) = selected_rule(
                            table_rules,
                            &envelope.change.database,
                            &envelope.change.table,
                        )?
                        else {
                            continue;
                        };
                        let column_mapping = match &rule.columns {
                            super::ColumnSelection::All(_) => Default::default(),
                            super::ColumnSelection::Mapping(mapping) => mapping.clone(),
                        };
                        let message = CdcRowMessage {
                            envelope,
                            target_table: rule.target.clone(),
                            column_mapping,
                        };
                        send_bounded_row(
                            &sender,
                            Arc::clone(&byte_capacity),
                            max_message_bytes,
                            message,
                        )
                        .await?;
                        forwarded += 1;
                    }
                    CdcMessage::Commit(commit) => {
                        acknowledge_and_checkpoint(
                            &checkpoint_store,
                            checkpoint_id,
                            checkpoint_identity,
                            &mut sequence,
                            &sender,
                            cancellation,
                            commit,
                        )
                        .await
                        .map_err(checkpoint_error)?;
                    }
                    CdcMessage::TableMap { .. }
                    | CdcMessage::Rotate { .. }
                    | CdcMessage::Heartbeat { .. } => {}
                }
            }
        }
        Ok(forwarded)
    }
    .await;

    let close_result = tokio::time::timeout(Duration::from_secs(5), client.close())
        .await
        .map_err(|_| shutdown_error("replication connection close timed out"))
        .and_then(|result| result);
    match (forwarding, close_result) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(forwarded), Ok(())) => Ok(forwarded),
    }
}

pub(super) async fn send_bounded_row(
    sender: &mpsc::Sender<anyhow::Result<PipelineMessage>>,
    byte_capacity: Arc<Semaphore>,
    max_message_bytes: usize,
    message: CdcRowMessage,
) -> Result<(), ReplicationError> {
    let message_bytes = serde_json::to_vec(&message)
        .map_err(|error| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                format!("serialize CDC buffer message: {error}"),
            )
        })?
        .len();
    if message_bytes > max_message_bytes {
        return Err(ReplicationError::new(
            ReplicationErrorKind::Configuration,
            false,
            format!("CDC message size {message_bytes} exceeds buffer.bytes {max_message_bytes}"),
        ));
    }
    let permits = u32::try_from(message_bytes).map_err(|_| {
        ReplicationError::new(
            ReplicationErrorKind::Configuration,
            false,
            "CDC message exceeds the supported byte-buffer range",
        )
    })?;
    let byte_permit = byte_capacity
        .acquire_many_owned(permits)
        .await
        .map_err(|_| shutdown_error("CDC byte buffer closed"))?;
    sender
        .send(Ok(PipelineMessage::CdcRow(BufferedCdcRow::new(
            message,
            byte_permit,
        ))))
        .await
        .map_err(|_| shutdown_error("CDC pipeline closed while sending a row"))
}

pub(super) fn selected_rule<'a>(
    rules: &'a [TableRule],
    database: &str,
    table: &str,
) -> Result<Option<&'a TableRule>, ReplicationError> {
    let mut matches = rules.iter().filter(|rule| {
        let candidate = if rule.include.contains('.') {
            format!("{database}.{table}")
        } else {
            table.to_owned()
        };
        glob_matches(&rule.include, &candidate)
    });
    let first = matches.next();
    if matches.next().is_some() {
        return Err(ReplicationError::new(
            ReplicationErrorKind::Configuration,
            false,
            format!("multiple table rules match {database}.{table}"),
        ));
    }
    Ok(first)
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let mut previous = vec![false; value.len() + 1];
    previous[0] = true;
    for token in pattern {
        let mut current = vec![false; value.len() + 1];
        if *token == b'*' {
            current[0] = previous[0];
            for index in 1..=value.len() {
                current[index] = current[index - 1] || previous[index];
            }
        } else {
            for index in 1..=value.len() {
                current[index] =
                    previous[index - 1] && (*token == b'?' || *token == value[index - 1]);
            }
        }
        previous = current;
    }
    previous[value.len()]
}

fn checkpoint_error(error: anyhow::Error) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Checkpoint, false, error.to_string())
}

fn shutdown_error(message: &'static str) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Shutdown, false, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matching_is_anchored_and_supports_question_mark() {
        assert!(glob_matches("orders_*", "orders_2026"));
        assert!(glob_matches("order?", "orders"));
        assert!(!glob_matches("orders", "sales.orders"));
        assert!(!glob_matches("orders_*", "archived_orders_2026"));
    }
}
