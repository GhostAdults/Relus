use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::DateTime;

use crate::{
    envelope::CDC_SCHEMA_VERSION, CdcEnvelope, CdcOffset, CdcOperation, CdcValue, CheckpointPhase,
    PersistentTestCheckpoint, ReplicationError, ReplicationErrorKind, RowChange,
    TestCheckpointStore,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupMode {
    Initial { table: String },
    Earliest,
    Latest,
    Specific(CdcOffset),
    Timestamp(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogTransactionBoundary {
    pub start_offset: CdcOffset,
    pub commit_offset: CdcOffset,
    pub started_at_utc_seconds: i64,
    pub committed_at_utc_seconds: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRow {
    pub key: BTreeMap<String, CdcValue>,
    pub values: BTreeMap<String, CdcValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCompletion {
    pub database: String,
    pub table: String,
    pub captured_offset: CdcOffset,
    pub row_count: u64,
}

#[async_trait]
pub trait StartupOffsetProvider: Send {
    async fn earliest_offset(&mut self) -> Result<CdcOffset, ReplicationError>;
    async fn latest_offset(&mut self) -> Result<CdcOffset, ReplicationError>;
    async fn validate_specific_offset(
        &mut self,
        offset: &CdcOffset,
    ) -> Result<(), ReplicationError>;
    async fn transaction_boundaries(
        &mut self,
    ) -> Result<Vec<BinlogTransactionBoundary>, ReplicationError>;
}

#[async_trait]
pub trait InitialSnapshotSource: Send {
    async fn begin_consistent_snapshot(
        &mut self,
        table: &str,
    ) -> Result<Box<dyn SnapshotCursor>, ReplicationError>;

    async fn next_incremental(
        &mut self,
        start_offset: &CdcOffset,
    ) -> Result<Option<CdcEnvelope>, ReplicationError>;
}

#[async_trait]
pub trait SnapshotCursor: Send {
    fn database(&self) -> &str;
    fn table(&self) -> &str;
    fn engine(&self) -> &str;
    fn unique_key_columns(&self) -> &[String];
    fn unique_key_is_stable(&self) -> bool;
    fn uses_stable_key_pagination(&self) -> bool;
    fn captured_offset(&self) -> &CdcOffset;
    async fn next_row(&mut self) -> Result<Option<SnapshotRow>, ReplicationError>;
}

#[async_trait]
pub trait SnapshotSink: Send {
    fn supports_idempotent_snapshot(&self) -> bool;
    async fn write_snapshot(&mut self, envelope: &CdcEnvelope) -> Result<(), ReplicationError>;
    async fn write_incremental(&mut self, envelope: &CdcEnvelope) -> Result<(), ReplicationError>;
    async fn acknowledge_snapshot(
        &mut self,
        completion: &SnapshotCompletion,
    ) -> Result<(), ReplicationError>;
}

pub async fn prepare_startup<P, S, C>(
    source: &mut P,
    sink: &mut S,
    checkpoints: &mut C,
    mode: StartupMode,
) -> Result<CdcOffset, ReplicationError>
where
    P: StartupOffsetProvider + InitialSnapshotSource,
    S: SnapshotSink,
    C: TestCheckpointStore,
{
    if let Some(checkpoint) = checkpoints.load().await? {
        return Ok(checkpoint.confirmed_offset);
    }

    match mode {
        StartupMode::Initial { table } => {
            run_initial_snapshot(source, sink, checkpoints, &table).await
        }
        StartupMode::Earliest => source.earliest_offset().await,
        StartupMode::Latest => source.latest_offset().await,
        StartupMode::Specific(offset) => {
            source.validate_specific_offset(&offset).await?;
            Ok(offset)
        }
        StartupMode::Timestamp(timestamp) => {
            let offset = resolve_timestamp(source, &timestamp).await?;
            checkpoints
                .save(&PersistentTestCheckpoint::new(
                    1,
                    offset.clone(),
                    CheckpointPhase::Streaming,
                )?)
                .await?;
            Ok(offset)
        }
    }
}

async fn resolve_timestamp<P: StartupOffsetProvider>(
    source: &mut P,
    timestamp: &str,
) -> Result<CdcOffset, ReplicationError> {
    let requested = DateTime::parse_from_rfc3339(timestamp).map_err(|error| {
        ReplicationError::new(
            ReplicationErrorKind::InvalidOffset,
            false,
            format!("startup timestamp must be offset-aware RFC 3339: {error}"),
        )
    })?;
    let requested_seconds = requested.timestamp();
    let boundaries = source.transaction_boundaries().await?;
    let Some(earliest_start) = boundaries
        .iter()
        .map(|boundary| boundary.started_at_utc_seconds)
        .min()
    else {
        return Err(invalid_offset("no retained transactions are available"));
    };
    if requested_seconds < earliest_start {
        return Err(ReplicationError::new(
            ReplicationErrorKind::BinlogPurged,
            false,
            "startup timestamp predates the earliest retained transaction",
        ));
    }
    boundaries
        .into_iter()
        .filter(|boundary| boundary.committed_at_utc_seconds >= requested_seconds)
        .min_by_key(|boundary| boundary.committed_at_utc_seconds)
        .map(|boundary| boundary.start_offset)
        .ok_or_else(|| invalid_offset("no transaction boundary exists at or after the timestamp"))
}

async fn run_initial_snapshot<P, S, C>(
    source: &mut P,
    sink: &mut S,
    checkpoints: &mut C,
    table: &str,
) -> Result<CdcOffset, ReplicationError>
where
    P: StartupOffsetProvider + InitialSnapshotSource,
    S: SnapshotSink,
    C: TestCheckpointStore,
{
    if !sink.supports_idempotent_snapshot() {
        return Err(configuration_error(
            "initial snapshot requires an idempotent snapshot sink",
        ));
    }
    let mut cursor = source.begin_consistent_snapshot(table).await?;
    if !cursor.engine().eq_ignore_ascii_case("innodb") {
        return Err(configuration_error(
            "initial snapshot requires an InnoDB table",
        ));
    }
    if cursor.unique_key_columns().is_empty() {
        return Err(configuration_error(
            "initial snapshot requires a stable unique key",
        ));
    }
    if !cursor.unique_key_is_stable() {
        return Err(configuration_error(
            "initial snapshot requires a primary key or non-null unique index",
        ));
    }
    if !cursor.uses_stable_key_pagination() {
        return Err(configuration_error(
            "initial snapshot must paginate by the stable unique key",
        ));
    }

    let captured_offset = cursor.captured_offset().clone();
    source.validate_specific_offset(&captured_offset).await?;
    let database = cursor.database().to_owned();
    let source_table = cursor.table().to_owned();
    let unique_key_columns = cursor.unique_key_columns().to_vec();
    let mut row_count = 0_u64;

    while let Some(row) = cursor.next_row().await? {
        validate_snapshot_key(&row, &unique_key_columns)?;
        let event_id = snapshot_event_id(&database, &source_table, &row.key)?;
        sink.write_snapshot(&CdcEnvelope {
            schema_version: CDC_SCHEMA_VERSION,
            event_id,
            transaction_id: format!("snapshot:{database}:{source_table}"),
            transaction_event_index: row_count,
            row_index: 0,
            event_timestamp_seconds: 0,
            offset: captured_offset.clone(),
            change: RowChange {
                database: database.clone(),
                table: source_table.clone(),
                operation: CdcOperation::Snapshot,
                key: row.key,
                before: None,
                after: Some(row.values),
            },
        })
        .await?;
        row_count += 1;
    }

    let completion = SnapshotCompletion {
        database,
        table: source_table,
        captured_offset: captured_offset.clone(),
        row_count,
    };
    sink.acknowledge_snapshot(&completion).await?;
    source.validate_specific_offset(&captured_offset).await?;
    checkpoints
        .save(&PersistentTestCheckpoint::new(
            1,
            captured_offset.clone(),
            CheckpointPhase::SnapshotComplete,
        )?)
        .await?;
    while let Some(envelope) = source.next_incremental(&captured_offset).await? {
        sink.write_incremental(&envelope).await?;
    }
    Ok(captured_offset)
}

fn validate_snapshot_key(
    row: &SnapshotRow,
    unique_key_columns: &[String],
) -> Result<(), ReplicationError> {
    if unique_key_columns.iter().all(|column| {
        row.key
            .get(column)
            .is_some_and(|value| !matches!(value, CdcValue::Null))
    }) {
        Ok(())
    } else {
        Err(configuration_error(
            "snapshot row does not contain every stable unique-key column",
        ))
    }
}

fn snapshot_event_id(
    database: &str,
    table: &str,
    key: &BTreeMap<String, CdcValue>,
) -> Result<String, ReplicationError> {
    let key = serde_json::to_string(key).map_err(|error| {
        ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
    })?;
    Ok(format!("snapshot:{database}:{table}:{key}"))
}

fn configuration_error(message: &'static str) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Configuration, false, message)
}

fn invalid_offset(message: &'static str) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::InvalidOffset, false, message)
}
