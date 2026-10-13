use std::sync::Arc;

use relus_common::cdc::{
    CdcEnvelope, CdcEventId, CdcMessage, CdcOffset, RowChange, TransactionCommit,
    CDC_SCHEMA_VERSION,
};

use super::error::{ReplicationError, ReplicationErrorKind};

#[derive(Debug, Clone, PartialEq)]
pub enum RawReplicationEvent {
    Begin {
        transaction_id: String,
        offset: CdcOffset,
    },
    Row {
        timestamp_seconds: u32,
        offset: CdcOffset,
        rows: Vec<RowChange>,
    },
    Commit {
        offset: CdcOffset,
    },
    Rotate {
        file: String,
        position: u64,
        offset: CdcOffset,
    },
    TableMap {
        database: String,
        table: String,
        table_id: u64,
        offset: CdcOffset,
    },
    SchemaChanged {
        database: String,
        table: String,
        offset: CdcOffset,
    },
    Heartbeat {
        offset: CdcOffset,
    },
    Harmless,
    Unsupported {
        event_type: u8,
    },
}

#[derive(Debug, Default)]
pub struct TransactionAssembler {
    active: Option<ActiveTransaction>,
}

#[derive(Debug)]
struct ActiveTransaction {
    transaction_id: Arc<str>,
    start_offset: CdcOffset,
    next_event_index: u64,
    emitted_rows: u64,
}

impl TransactionAssembler {
    pub fn has_active_transaction(&self) -> bool {
        self.active.is_some()
    }

    pub fn push(
        &mut self,
        event: RawReplicationEvent,
    ) -> Result<Vec<CdcMessage>, ReplicationError> {
        match event {
            RawReplicationEvent::Begin {
                transaction_id,
                offset,
            } => {
                if self.active.is_some() {
                    return Err(protocol_error("received a nested transaction boundary"));
                }
                self.active = Some(ActiveTransaction {
                    transaction_id: transaction_id.into(),
                    start_offset: offset,
                    next_event_index: 0,
                    emitted_rows: 0,
                });
                Ok(Vec::new())
            }
            RawReplicationEvent::Row {
                timestamp_seconds,
                offset,
                rows,
            } => self.emit_rows(timestamp_seconds, offset, rows),
            RawReplicationEvent::Commit { offset } => self.commit(offset),
            RawReplicationEvent::TableMap {
                database,
                table,
                table_id,
                offset,
            } => Ok(vec![CdcMessage::TableMap {
                schema_version: CDC_SCHEMA_VERSION,
                event_id: control_event_id("table_map", &offset),
                database,
                table,
                table_id,
                offset,
            }]),
            RawReplicationEvent::SchemaChanged {
                database, table, ..
            } => Err(ReplicationError::new(
                ReplicationErrorKind::SchemaDrift,
                false,
                format!("schema changed for {database}.{table}"),
            )),
            RawReplicationEvent::Rotate {
                file,
                position,
                offset,
            } => Ok(vec![CdcMessage::Rotate {
                schema_version: CDC_SCHEMA_VERSION,
                event_id: control_event_id("rotate", &offset),
                file,
                position,
                offset,
            }]),
            RawReplicationEvent::Heartbeat { offset } => Ok(vec![CdcMessage::Heartbeat {
                schema_version: CDC_SCHEMA_VERSION,
                event_id: control_event_id("heartbeat", &offset),
                offset,
            }]),
            RawReplicationEvent::Harmless => Ok(Vec::new()),
            RawReplicationEvent::Unsupported { event_type } => Err(ReplicationError::new(
                ReplicationErrorKind::UnsupportedEvent,
                false,
                format!("unsupported binlog event type {event_type}"),
            )),
        }
    }

    fn emit_rows(
        &mut self,
        timestamp_seconds: u32,
        offset: CdcOffset,
        rows: Vec<RowChange>,
    ) -> Result<Vec<CdcMessage>, ReplicationError> {
        if self.active.is_none() {
            let transaction_id = offset.transaction_id();
            self.active = Some(ActiveTransaction {
                transaction_id: transaction_id.into(),
                start_offset: offset.clone(),
                next_event_index: 0,
                emitted_rows: 0,
            });
        }

        let active = self.active.as_mut().ok_or_else(|| {
            protocol_error("transaction state was unavailable while processing rows")
        })?;
        for change in &rows {
            validate_full_row_image(change)?;
        }
        let event_index = active.next_event_index;
        active.next_event_index += 1;
        let offset = Arc::new(offset);
        let event_id_prefix: Arc<str> =
            stable_event_id_prefix(&active.transaction_id, &offset, event_index).into();

        let messages = rows
            .into_iter()
            .enumerate()
            .map(|(row_index, change)| {
                active.emitted_rows += 1;
                let row_index = row_index as u64;
                let event_id = CdcEventId::row(Arc::clone(&event_id_prefix), row_index);
                CdcMessage::Row(CdcEnvelope {
                    schema_version: CDC_SCHEMA_VERSION,
                    event_id,
                    transaction_id: Arc::clone(&active.transaction_id),
                    transaction_event_index: event_index,
                    row_index,
                    event_timestamp_seconds: timestamp_seconds,
                    offset: Arc::clone(&offset),
                    change,
                })
            })
            .collect();
        Ok(messages)
    }

    fn commit(&mut self, offset: CdcOffset) -> Result<Vec<CdcMessage>, ReplicationError> {
        let active = self
            .active
            .take()
            .ok_or_else(|| protocol_error("received COMMIT without an active transaction"))?;
        let transaction_id: Arc<str> = if active.transaction_id.is_empty() {
            active.start_offset.transaction_id().into()
        } else {
            active.transaction_id
        };
        let offset = Arc::new(offset);
        Ok(vec![CdcMessage::Commit(TransactionCommit {
            schema_version: CDC_SCHEMA_VERSION,
            event_id: control_event_id("commit", &offset),
            transaction_id,
            offset,
            event_count: active.emitted_rows,
        })])
    }
}

fn validate_full_row_image(change: &RowChange) -> Result<(), ReplicationError> {
    let valid = match change.operation {
        relus_common::cdc::CdcOperation::Snapshot => {
            change.before.is_none() && change.after.is_some()
        }
        relus_common::cdc::CdcOperation::Insert => {
            change.before.is_none() && change.after.is_some()
        }
        relus_common::cdc::CdcOperation::Update => {
            change.before.is_some() && change.after.is_some()
        }
        relus_common::cdc::CdcOperation::Delete => {
            change.before.is_some() && change.after.is_none()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(ReplicationError::new(
            ReplicationErrorKind::Decode,
            false,
            "row event does not contain the required FULL before/after image",
        ))
    }
}

fn control_event_id(event_type: &str, offset: &CdcOffset) -> String {
    format!("{event_type}:{}:{}", offset.file(), offset.position())
}

fn stable_event_id_prefix(transaction_id: &str, offset: &CdcOffset, event_index: u64) -> String {
    match offset {
        CdcOffset::Gtid { .. } => format!("{transaction_id}:{event_index}:"),
        CdcOffset::Position { file, position } => {
            format!("{file}:{position}:{event_index}:")
        }
    }
}

fn protocol_error(message: impl Into<String>) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Protocol, false, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use relus_common::cdc::{CdcOperation, CdcRow, CdcValue};

    fn row(database: &str, table: &str, id: u64) -> RowChange {
        let mut after = CdcRow::new();
        after.insert("id".to_owned(), CdcValue::Unsigned(id));
        RowChange {
            database: Arc::from(database),
            table: Arc::from(table),
            operation: CdcOperation::Insert,
            key: CdcRow::from([("id".to_owned(), CdcValue::Unsigned(id))]),
            before: None,
            after: Some(after),
        }
    }

    #[test]
    fn gtid_replay_produces_stable_ids_and_one_commit_boundary() -> Result<(), ReplicationError> {
        let offset = CdcOffset::Gtid {
            resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-41".to_owned(),
            transaction_gtid: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:42".to_owned()),
            file: "mysql-bin.000007".to_owned(),
            position: 120,
        };
        let run = || {
            let mut assembler = TransactionAssembler::default();
            assembler.push(RawReplicationEvent::Begin {
                transaction_id: offset.transaction_id(),
                offset: offset.clone(),
            })?;
            let mut messages = assembler.push(RawReplicationEvent::Row {
                timestamp_seconds: 1_700_000_000,
                offset: offset.clone(),
                rows: vec![row("sales", "orders", 1), row("sales", "items", 2)],
            })?;
            messages.extend(assembler.push(RawReplicationEvent::Commit {
                offset: offset.clone().with_position("mysql-bin.000007", 180),
            })?);
            Ok::<_, ReplicationError>(messages)
        };

        let first = run()?;
        let replay = run()?;
        assert_eq!(first, replay);
        assert_eq!(first.len(), 3);
        assert!(matches!(&first[2], CdcMessage::Commit(commit) if commit.event_count == 2));
        Ok(())
    }

    #[test]
    fn file_position_event_id_is_stable_without_gtid() -> Result<(), ReplicationError> {
        let offset = CdcOffset::Position {
            file: "mysql-bin.000003".to_owned(),
            position: 456,
        };
        let mut assembler = TransactionAssembler::default();
        let messages = assembler.push(RawReplicationEvent::Row {
            timestamp_seconds: 123,
            offset,
            rows: vec![row("sales", "orders", 1)],
        })?;

        let CdcMessage::Row(envelope) = &messages[0] else {
            panic!("expected row envelope");
        };
        assert_eq!(envelope.event_id.to_string(), "mysql-bin.000003:456:0:0");
        let serialized = serde_json::to_value(&messages[0]).map_err(|error| {
            ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
        })?;
        assert_eq!(
            serialized["event_id"],
            serde_json::Value::String("mysql-bin.000003:456:0:0".to_owned())
        );
        Ok(())
    }

    #[test]
    fn unknown_semantic_event_fails_explicitly() {
        let result = TransactionAssembler::default()
            .push(RawReplicationEvent::Unsupported { event_type: 255 });
        let Err(error) = result else {
            panic!("unknown events must not be ignored");
        };
        assert_eq!(error.kind, ReplicationErrorKind::UnsupportedEvent);
        assert!(!error.retryable);
    }

    #[test]
    fn update_requires_full_before_and_after_images() {
        let change = RowChange {
            database: Arc::from("sales"),
            table: Arc::from("orders"),
            operation: CdcOperation::Update,
            key: CdcRow::new(),
            before: None,
            after: Some(CdcRow::new()),
        };
        let result = TransactionAssembler::default().push(RawReplicationEvent::Row {
            timestamp_seconds: 123,
            offset: CdcOffset::Position {
                file: "mysql-bin.000003".to_owned(),
                position: 456,
            },
            rows: vec![change],
        });
        assert!(matches!(
            result,
            Err(ReplicationError {
                kind: ReplicationErrorKind::Decode,
                ..
            })
        ));
    }

    #[test]
    fn multi_table_transaction_preserves_row_order_until_commit() -> Result<(), ReplicationError> {
        let offset = CdcOffset::Position {
            file: "mysql-bin.000004".to_owned(),
            position: 100,
        };
        let mut assembler = TransactionAssembler::default();
        assembler.push(RawReplicationEvent::Begin {
            transaction_id: "tx-1".to_owned(),
            offset: offset.clone(),
        })?;
        let messages = assembler.push(RawReplicationEvent::Row {
            timestamp_seconds: 123,
            offset: offset.clone(),
            rows: vec![row("sales", "orders", 1), row("sales", "items", 2)],
        })?;
        let CdcMessage::Row(first) = &messages[0] else {
            panic!("expected first row");
        };
        let CdcMessage::Row(second) = &messages[1] else {
            panic!("expected second row");
        };
        assert_eq!(first.change.table.as_ref(), "orders");
        assert_eq!(second.change.table.as_ref(), "items");
        assert_eq!(first.transaction_event_index, 0);
        assert_eq!(second.row_index, 1);
        let commit = assembler.push(RawReplicationEvent::Commit {
            offset: offset.with_position("mysql-bin.000004", 180),
        })?;
        assert!(matches!(&commit[0], CdcMessage::Commit(value) if value.event_count == 2));
        Ok(())
    }

    #[test]
    fn large_transaction_emits_rows_incrementally_and_commits_only_at_boundary(
    ) -> Result<(), ReplicationError> {
        let offset = CdcOffset::Position {
            file: "mysql-bin.000005".to_owned(),
            position: 100,
        };
        let mut assembler = TransactionAssembler::default();
        assembler.push(RawReplicationEvent::Begin {
            transaction_id: "large-tx".to_owned(),
            offset: offset.clone(),
        })?;
        for id in 0..100_000 {
            let messages = assembler.push(RawReplicationEvent::Row {
                timestamp_seconds: 123,
                offset: offset.clone(),
                rows: vec![row(
                    "sales",
                    if id % 2 == 0 { "orders" } else { "items" },
                    id,
                )],
            })?;
            assert_eq!(messages.len(), 1);
            assert!(assembler.has_active_transaction());
        }
        let commit = assembler.push(RawReplicationEvent::Commit {
            offset: offset.with_position("mysql-bin.000005", 200),
        })?;
        assert!(matches!(&commit[0], CdcMessage::Commit(value) if value.event_count == 100_000));
        assert!(!assembler.has_active_transaction());
        Ok(())
    }

    #[test]
    fn schema_changes_stop_without_advancing_transaction_state() {
        let mut assembler = TransactionAssembler::default();
        let result = assembler.push(RawReplicationEvent::SchemaChanged {
            database: "sales".to_owned(),
            table: "orders".to_owned(),
            offset: CdcOffset::Position {
                file: "mysql-bin.000006".to_owned(),
                position: 300,
            },
        });
        assert!(matches!(
            result,
            Err(ReplicationError {
                kind: ReplicationErrorKind::SchemaDrift,
                ..
            })
        ));
        assert!(!assembler.has_active_transaction());
    }
}
