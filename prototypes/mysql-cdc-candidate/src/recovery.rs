use std::{path::PathBuf, time::Duration};

use async_trait::async_trait;
use mysql_async::{GnoInterval, Sid};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::{
    CdcEnvelope, CdcMessage, CdcOffset, MySqlAsyncReplicationClient, MySqlConnectionConfig,
    MySqlReplicationClient, ReplicationError, ReplicationErrorKind, TransactionAssembler,
    TransactionCommit,
};

pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointPhase {
    SnapshotComplete,
    Streaming,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentTestCheckpoint {
    pub format_version: u32,
    pub sequence: u64,
    pub confirmed_offset: CdcOffset,
    pub phase: CheckpointPhase,
}

impl PersistentTestCheckpoint {
    pub fn new(
        sequence: u64,
        offset: CdcOffset,
        phase: CheckpointPhase,
    ) -> Result<Self, ReplicationError> {
        Ok(Self {
            format_version: CHECKPOINT_FORMAT_VERSION,
            sequence,
            confirmed_offset: confirmed_resume_offset(offset)?,
            phase,
        })
    }
}

#[async_trait]
pub trait TestCheckpointStore: Send {
    async fn load(&self) -> Result<Option<PersistentTestCheckpoint>, ReplicationError>;
    async fn save(&mut self, checkpoint: &PersistentTestCheckpoint)
        -> Result<(), ReplicationError>;

    async fn save_interrupted(
        &mut self,
        _checkpoint: &PersistentTestCheckpoint,
    ) -> Result<(), ReplicationError> {
        Err(failpoint_error("failpoint during checkpoint write"))
    }
}

pub struct FileCheckpointStore {
    path: PathBuf,
}

#[async_trait]
pub trait RecoveryClientFactory: Send {
    async fn connect(
        &mut self,
        start_offset: CdcOffset,
    ) -> Result<Box<dyn MySqlReplicationClient>, ReplicationError>;
}

pub struct MySqlRecoveryClientFactory {
    config: MySqlConnectionConfig,
}

impl MySqlRecoveryClientFactory {
    pub fn new(config: MySqlConnectionConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RecoveryClientFactory for MySqlRecoveryClientFactory {
    async fn connect(
        &mut self,
        start_offset: CdcOffset,
    ) -> Result<Box<dyn MySqlReplicationClient>, ReplicationError> {
        let mut config = self.config.clone();
        config.start_offset = start_offset;
        let mut client = MySqlAsyncReplicationClient::new(config);
        client.connect().await?;
        Ok(Box::new(client))
    }
}

#[async_trait]
pub trait PrototypeSink: Send {
    async fn write(&mut self, envelope: &CdcEnvelope) -> Result<(), ReplicationError>;
    async fn acknowledge(&mut self, commit: &TransactionCommit) -> Result<(), ReplicationError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFailpoint {
    BeforeSinkWrite,
    AfterSinkWriteBeforeCheckpoint,
    DuringCheckpoint,
    DuringLargeTransaction { after_rows: u64 },
    AfterSnapshotBeforeIncrementalHandoff,
}

#[derive(Debug, Clone, Copy)]
pub struct RecoveryOptions {
    pub max_reconnects: u32,
    pub reconnect_delay: Duration,
    pub heartbeat_timeout: Duration,
}

impl Default for RecoveryOptions {
    fn default() -> Self {
        Self {
            max_reconnects: 3,
            reconnect_delay: Duration::from_millis(10),
            heartbeat_timeout: Duration::from_secs(45),
        }
    }
}

pub async fn run_recovery_loop<F, S, C>(
    factory: &mut F,
    sink: &mut S,
    checkpoints: &mut C,
    initial_offset: CdcOffset,
    options: RecoveryOptions,
    failpoint: Option<RecoveryFailpoint>,
) -> Result<(), ReplicationError>
where
    F: RecoveryClientFactory,
    S: PrototypeSink,
    C: TestCheckpointStore,
{
    let loaded = checkpoints.load().await?;
    if loaded.as_ref().is_some_and(|checkpoint| {
        checkpoint.phase == CheckpointPhase::SnapshotComplete
            && failpoint == Some(RecoveryFailpoint::AfterSnapshotBeforeIncrementalHandoff)
    }) {
        return Err(failpoint_error(
            "failpoint after snapshot before incremental handoff",
        ));
    }
    let mut confirmed_offset = loaded
        .as_ref()
        .map(|checkpoint| checkpoint.confirmed_offset.clone())
        .unwrap_or(initial_offset);
    let mut checkpoint_sequence = loaded.map_or(0, |checkpoint| checkpoint.sequence);
    let mut reconnects = 0;

    loop {
        let mut client = match factory.connect(confirmed_offset.clone()).await {
            Ok(client) => client,
            Err(error) if should_reconnect(&error, reconnects, options.max_reconnects) => {
                reconnects += 1;
                wait_before_reconnect(options.reconnect_delay, reconnects).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        let mut assembler = TransactionAssembler::default();
        let mut transaction_rows = 0_u64;

        loop {
            let event =
                match tokio::time::timeout(options.heartbeat_timeout, client.next_event()).await {
                    Ok(Ok(Some(event))) => event,
                    Ok(Ok(None))
                        if client.is_finite_stream() && !assembler.has_active_transaction() =>
                    {
                        client.close().await?;
                        return Ok(());
                    }
                    Ok(Ok(None)) => {
                        let message = if assembler.has_active_transaction() {
                            "replication stream ended during an incomplete transaction"
                        } else {
                            "replication stream ended unexpectedly"
                        };
                        reconnect_connected_client(
                            client.as_mut(),
                            &mut reconnects,
                            options,
                            ReplicationError::new(ReplicationErrorKind::Network, false, message),
                        )
                        .await?;
                        break;
                    }
                    Ok(Err(error)) => {
                        if should_reconnect(&error, reconnects, options.max_reconnects) {
                            reconnect_connected_client(
                                client.as_mut(),
                                &mut reconnects,
                                options,
                                error,
                            )
                            .await?;
                            break;
                        }
                        return Err(error);
                    }
                    Err(_) => {
                        reconnect_connected_client(
                            client.as_mut(),
                            &mut reconnects,
                            options,
                            ReplicationError::new(
                                ReplicationErrorKind::HeartbeatTimeout,
                                false,
                                "replication heartbeat timeout exceeded reconnect limit",
                            ),
                        )
                        .await?;
                        break;
                    }
                };

            for message in assembler.push(event)? {
                match message {
                    CdcMessage::Row(envelope) => {
                        if failpoint == Some(RecoveryFailpoint::BeforeSinkWrite) {
                            return Err(failpoint_error("failpoint before sink write"));
                        }
                        sink.write(&envelope).await?;
                        transaction_rows += 1;
                        if matches!(
                            failpoint,
                            Some(RecoveryFailpoint::DuringLargeTransaction { after_rows })
                                if transaction_rows >= after_rows
                        ) {
                            return Err(failpoint_error(
                                "failpoint during large transaction transfer",
                            ));
                        }
                    }
                    CdcMessage::Commit(commit) => {
                        sink.acknowledge(&commit).await?;
                        if failpoint == Some(RecoveryFailpoint::AfterSinkWriteBeforeCheckpoint) {
                            return Err(failpoint_error(
                                "failpoint after sink write before checkpoint",
                            ));
                        }
                        checkpoint_sequence += 1;
                        let checkpoint = PersistentTestCheckpoint::new(
                            checkpoint_sequence,
                            commit.offset.as_ref().clone(),
                            CheckpointPhase::Streaming,
                        )?;
                        if failpoint == Some(RecoveryFailpoint::DuringCheckpoint) {
                            return checkpoints.save_interrupted(&checkpoint).await;
                        }
                        checkpoints.save(&checkpoint).await?;
                        confirmed_offset = checkpoint.confirmed_offset;
                        transaction_rows = 0;
                        reconnects = 0;
                    }
                    CdcMessage::Heartbeat { .. }
                    | CdcMessage::TableMap { .. }
                    | CdcMessage::Rotate { .. } => {}
                }
            }
        }
    }
}

fn should_reconnect(error: &ReplicationError, reconnects: u32, limit: u32) -> bool {
    error.retryable && reconnects < limit
}

async fn wait_before_reconnect(base_delay: Duration, reconnect_number: u32) {
    if base_delay.is_zero() {
        return;
    }
    let exponent = reconnect_number.saturating_sub(1).min(16);
    let exponential = base_delay.saturating_mul(1_u32 << exponent);
    let jitter_percent = 80
        + std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
            % 41;
    tokio::time::sleep(exponential.saturating_mul(jitter_percent) / 100).await;
}

async fn reconnect_connected_client(
    client: &mut dyn MySqlReplicationClient,
    reconnects: &mut u32,
    options: RecoveryOptions,
    exhausted_error: ReplicationError,
) -> Result<(), ReplicationError> {
    if *reconnects >= options.max_reconnects {
        return Err(exhausted_error);
    }
    *reconnects += 1;
    let _ = client.close().await;
    wait_before_reconnect(options.reconnect_delay, *reconnects).await;
    Ok(())
}

impl FileCheckpointStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait]
impl TestCheckpointStore for FileCheckpointStore {
    async fn load(&self) -> Result<Option<PersistentTestCheckpoint>, ReplicationError> {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(checkpoint_error(error.to_string())),
        };
        let checkpoint = serde_json::from_slice::<PersistentTestCheckpoint>(&bytes)
            .map_err(|error| checkpoint_error(error.to_string()))?;
        if checkpoint.format_version != CHECKPOINT_FORMAT_VERSION {
            return Err(checkpoint_error(format!(
                "unsupported checkpoint format version {}",
                checkpoint.format_version
            )));
        }
        Ok(Some(checkpoint))
    }

    async fn save(
        &mut self,
        checkpoint: &PersistentTestCheckpoint,
    ) -> Result<(), ReplicationError> {
        let temp_path = self.write_temporary(checkpoint).await?;
        tokio::fs::rename(&temp_path, &self.path)
            .await
            .map_err(|error| checkpoint_error(error.to_string()))?;
        Ok(())
    }

    async fn save_interrupted(
        &mut self,
        checkpoint: &PersistentTestCheckpoint,
    ) -> Result<(), ReplicationError> {
        self.write_temporary(checkpoint).await?;
        Err(failpoint_error("failpoint during checkpoint write"))
    }
}

impl FileCheckpointStore {
    async fn write_temporary(
        &self,
        checkpoint: &PersistentTestCheckpoint,
    ) -> Result<PathBuf, ReplicationError> {
        if let Some(current) = self.load().await? {
            if checkpoint.sequence <= current.sequence {
                return Err(checkpoint_error(
                    "checkpoint sequence must advance monotonically",
                ));
            }
        }
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| checkpoint_error(error.to_string()))?;
        }
        let bytes = serde_json::to_vec_pretty(checkpoint)
            .map_err(|error| checkpoint_error(error.to_string()))?;
        let temp_path = self.path.with_extension("json.tmp");
        let mut file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|error| checkpoint_error(error.to_string()))?;
        file.write_all(&bytes)
            .await
            .map_err(|error| checkpoint_error(error.to_string()))?;
        file.sync_all()
            .await
            .map_err(|error| checkpoint_error(error.to_string()))?;
        Ok(temp_path)
    }
}

fn confirmed_resume_offset(offset: CdcOffset) -> Result<CdcOffset, ReplicationError> {
    match offset {
        CdcOffset::Position { .. } => Ok(offset),
        CdcOffset::Gtid {
            resume_set,
            transaction_gtid,
            file,
            position,
        } => {
            let resume_set = append_transaction_gtid(&resume_set, transaction_gtid.as_deref())?;
            Ok(CdcOffset::Gtid {
                resume_set,
                transaction_gtid: None,
                file,
                position,
            })
        }
    }
}

fn append_transaction_gtid(
    resume_set: &str,
    transaction_gtid: Option<&str>,
) -> Result<String, ReplicationError> {
    let Some(transaction_gtid) = transaction_gtid else {
        return Ok(resume_set.to_owned());
    };
    let transaction = transaction_gtid
        .parse::<Sid<'static>>()
        .map_err(|error| checkpoint_error(error.to_string()))?;
    let mut existing = resume_set
        .split(',')
        .filter(|part| !part.is_empty())
        .map(str::parse::<Sid<'static>>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| checkpoint_error(error.to_string()))?;
    if let Some(sid) = existing
        .iter_mut()
        .find(|sid| sid.uuid() == transaction.uuid() && sid.tag() == transaction.tag())
    {
        let mut intervals = sid.intervals().to_vec();
        intervals.extend(transaction.intervals().iter().copied());
        intervals.sort_by_key(GnoInterval::start);
        let mut merged: Vec<GnoInterval> = Vec::new();
        for interval in intervals {
            if let Some(last) = merged.last_mut() {
                if interval.start() <= last.end() {
                    *last = GnoInterval::new(last.start(), last.end().max(interval.end()));
                    continue;
                }
            }
            merged.push(interval);
        }
        *sid = sid.clone().with_intervals(merged);
    } else {
        existing.push(transaction);
    }
    Ok(existing
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(","))
}

fn checkpoint_error(message: impl Into<String>) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Checkpoint, false, message)
}

fn failpoint_error(message: &'static str) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Failpoint, false, message)
}
