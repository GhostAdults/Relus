use std::{collections::BTreeMap, path::Path, sync::Arc};

use anyhow::{bail, Context, Result};
use redb::{Database, TableDefinition};
use relus_common::{
    cdc::{cdc_commit_ack_channel, CdcOffset, TransactionCommit},
    PipelineMessage,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const CHECKPOINT_FORMAT_VERSION: u32 = 2;
const CHECKPOINTS: TableDefinition<&str, &[u8]> = TableDefinition::new("mysql_cdc_checkpoints_v2");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointIdentity {
    pub source_id: String,
    pub server_uuid: String,
    pub database: String,
    pub table_rules_fingerprint: String,
    pub schema_fingerprint: String,
}

impl CheckpointIdentity {
    pub fn new(
        source_id: impl Into<String>,
        server_uuid: impl Into<String>,
        database: impl Into<String>,
        table_rules_fingerprint: impl Into<String>,
        schema_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            source_id: source_id.into(),
            server_uuid: server_uuid.into(),
            database: database.into(),
            table_rules_fingerprint: table_rules_fingerprint.into(),
            schema_fingerprint: schema_fingerprint.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.source_id.trim().is_empty()
            || self.server_uuid.trim().is_empty()
            || self.database.trim().is_empty()
            || self.table_rules_fingerprint.trim().is_empty()
            || self.schema_fingerprint.trim().is_empty()
        {
            bail!("MySQL CDC checkpoint identity fields must not be empty");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcCheckpoint {
    pub format_version: u32,
    pub checkpoint_id: String,
    pub identity: CheckpointIdentity,
    pub confirmed_offset: Arc<CdcOffset>,
    pub sequence: u64,
}

impl CdcCheckpoint {
    pub fn new(
        checkpoint_id: impl Into<String>,
        identity: CheckpointIdentity,
        confirmed_offset: Arc<CdcOffset>,
        sequence: u64,
    ) -> Self {
        Self {
            format_version: CHECKPOINT_FORMAT_VERSION,
            checkpoint_id: checkpoint_id.into(),
            identity,
            confirmed_offset,
            sequence,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.format_version != CHECKPOINT_FORMAT_VERSION {
            bail!(
                "unsupported MySQL CDC checkpoint format_version {}",
                self.format_version
            );
        }
        if self.checkpoint_id.trim().is_empty() {
            bail!("MySQL CDC checkpoint_id must not be empty");
        }
        self.identity.validate()?;
        Ok(())
    }
}

pub struct RedbCheckpointStore {
    database: Database,
}

impl RedbCheckpointStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "create MySQL CDC checkpoint directory `{}`",
                    parent.display()
                )
            })?;
        }
        let database = Database::create(path)
            .with_context(|| format!("open MySQL CDC checkpoint store `{}`", path.display()))?;
        let transaction = database.begin_write()?;
        transaction.open_table(CHECKPOINTS)?;
        transaction.commit()?;
        Ok(Self { database })
    }

    pub fn load(&self, checkpoint_id: &str) -> Result<Option<CdcCheckpoint>> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(CHECKPOINTS)?;
        let Some(value) = table.get(checkpoint_id)? else {
            return Ok(None);
        };
        let checkpoint: CdcCheckpoint =
            serde_json::from_slice(value.value()).context("decode MySQL CDC checkpoint")?;
        checkpoint.validate()?;
        Ok(Some(checkpoint))
    }

    pub fn save(&self, checkpoint: &CdcCheckpoint) -> Result<()> {
        checkpoint.validate()?;
        let mut checkpoint = checkpoint.clone();
        checkpoint.confirmed_offset = Arc::new(normalize_confirmed_offset(
            checkpoint.confirmed_offset.as_ref(),
        )?);
        if let Some(existing) = self.load(&checkpoint.checkpoint_id)? {
            if existing.identity != checkpoint.identity {
                bail!("checkpoint identity mismatch: the existing checkpoint belongs to a different source configuration");
            }
            if checkpoint.sequence <= existing.sequence {
                bail!(
                    "checkpoint sequence {} must be greater than the persisted sequence {}",
                    checkpoint.sequence,
                    existing.sequence
                );
            }
        }
        let encoded = serde_json::to_vec(&checkpoint).context("encode MySQL CDC checkpoint")?;
        let transaction = self.database.begin_write()?;
        {
            let mut table = transaction.open_table(CHECKPOINTS)?;
            table.insert(checkpoint.checkpoint_id.as_str(), encoded.as_slice())?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn validate_source(&self, checkpoint_id: &str, source_id: &str) -> Result<()> {
        if let Some(checkpoint) = self.load(checkpoint_id)? {
            if checkpoint.identity.source_id != source_id {
                bail!(
                    "checkpoint source_id mismatch: expected `{source_id}` for checkpoint `{checkpoint_id}`"
                );
            }
        }
        Ok(())
    }

    pub fn validate_identity(
        &self,
        checkpoint_id: &str,
        identity: &CheckpointIdentity,
    ) -> Result<()> {
        if let Some(checkpoint) = self.load(checkpoint_id)? {
            if &checkpoint.identity != identity {
                bail!("checkpoint identity mismatch: server, database, table rules, or schema changed");
            }
        }
        Ok(())
    }
}

pub(crate) async fn acknowledge_and_checkpoint(
    store: &RedbCheckpointStore,
    checkpoint_id: &str,
    identity: &CheckpointIdentity,
    sequence: &mut u64,
    sender: &mpsc::Sender<anyhow::Result<PipelineMessage>>,
    cancellation: &CancellationToken,
    commit: TransactionCommit,
) -> Result<Arc<CdcOffset>> {
    let (acknowledger, acknowledgement) = cdc_commit_ack_channel(commit);
    sender
        .send(Ok(PipelineMessage::CdcCommit(acknowledger)))
        .await
        .context("CDC pipeline closed while sending a transaction commit")?;
    let confirmed = tokio::select! {
        biased;
        () = cancellation.cancelled() => bail!("MySQL CDC transaction acknowledgement was cancelled"),
        result = acknowledgement => result.context("CDC sink dropped the transaction acknowledgement")?,
    };
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("checkpoint sequence overflow"))?;
    store.save(&CdcCheckpoint::new(
        checkpoint_id,
        identity.clone(),
        Arc::clone(&confirmed.offset),
        *sequence,
    ))?;
    Ok(confirmed.offset)
}

fn normalize_confirmed_offset(offset: &CdcOffset) -> Result<CdcOffset> {
    match offset {
        CdcOffset::Position { .. } => Ok(offset.clone()),
        CdcOffset::Gtid {
            resume_set,
            transaction_gtid,
            file,
            position,
        } => {
            let resume_set = match transaction_gtid {
                Some(transaction_gtid) => merge_gtid_set(resume_set, transaction_gtid)?,
                None => resume_set.clone(),
            };
            Ok(CdcOffset::Gtid {
                resume_set,
                transaction_gtid: None,
                file: file.clone(),
                position: *position,
            })
        }
    }
}

fn merge_gtid_set(resume_set: &str, transaction_gtid: &str) -> Result<String> {
    let mut by_source = BTreeMap::<String, Vec<(u64, u64)>>::new();
    for sid in resume_set
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .chain(std::iter::once(transaction_gtid))
    {
        sid.parse::<mysql_async::Sid>()
            .with_context(|| format!("invalid GTID set entry `{sid}`"))?;
        let (source, intervals) = split_gtid_entry(sid)?;
        by_source.entry(source).or_default().extend(intervals);
    }

    let mut merged_sources = Vec::with_capacity(by_source.len());
    for (source, mut intervals) in by_source {
        intervals.sort_unstable();
        let mut merged = Vec::<(u64, u64)>::new();
        for (start, end) in intervals {
            if let Some((_, previous_end)) = merged.last_mut() {
                if start <= previous_end.saturating_add(1) {
                    *previous_end = (*previous_end).max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }
        let intervals = merged
            .into_iter()
            .map(|(start, end)| {
                if start == end {
                    start.to_string()
                } else {
                    format!("{start}-{end}")
                }
            })
            .collect::<Vec<_>>()
            .join(":");
        merged_sources.push(format!("{source}:{intervals}"));
    }
    Ok(merged_sources.join(","))
}

fn split_gtid_entry(entry: &str) -> Result<(String, Vec<(u64, u64)>)> {
    let mut parts = entry.split(':');
    let uuid = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("GTID entry is missing its source UUID"))?;
    let second = parts
        .next()
        .ok_or_else(|| anyhow::anyhow!("GTID entry is missing its transaction interval"))?;
    let (source, interval_parts): (String, Vec<&str>) =
        if second.as_bytes().first().is_some_and(u8::is_ascii_digit) {
            (
                uuid.to_owned(),
                std::iter::once(second).chain(parts).collect(),
            )
        } else {
            (format!("{uuid}:{second}"), parts.collect())
        };
    let intervals = interval_parts
        .into_iter()
        .map(|interval| {
            let (start, end) = interval
                .split_once('-')
                .map_or((interval, interval), |(start, end)| (start, end));
            let start = start
                .parse::<u64>()
                .context("invalid GTID interval start")?;
            let end = end.parse::<u64>().context("invalid GTID interval end")?;
            if start == 0 || start > end {
                bail!("invalid GTID interval range");
            }
            Ok((start, end))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((source, intervals))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmed_gtid_is_merged_into_the_resume_set() -> Result<()> {
        let offset = CdcOffset::Gtid {
            resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-41".to_owned(),
            transaction_gtid: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:42".to_owned()),
            file: "mysql-bin.000001".to_owned(),
            position: 120,
        };
        assert_eq!(
            normalize_confirmed_offset(&offset)?,
            CdcOffset::Gtid {
                resume_set: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-42".to_owned(),
                transaction_gtid: None,
                file: "mysql-bin.000001".to_owned(),
                position: 120,
            }
        );
        Ok(())
    }
}
