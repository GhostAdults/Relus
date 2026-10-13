use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context, Result};
use chrono::DateTime;
use futures::StreamExt;
use mysql_async::{prelude::Queryable, Conn, Row};
use relus_common::cdc::{
    CdcEnvelope, CdcEventId, CdcOffset, CdcOperation, CdcRow, CdcRowMessage, CdcSnapshotTable,
    CdcSnapshotTargetPolicy, RowChange, SinkRequirements, TransactionCommit, CDC_SCHEMA_VERSION,
};
use relus_common::{connection::TlsConfig, job_config::JobConfig, PipelineMessage};
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    DataReaderJob, DataReaderTask, JsonStream, PipelineStream, ReadTask, SplitReaderResult,
    StreamMode,
};

use super::{
    adapter::{MySqlAsyncReplicationClient, MySqlConnectionConfig as AdapterConfig, TlsMode},
    checkpoint::{acknowledge_and_checkpoint, CheckpointIdentity},
    forward_replication_client,
    production::{selected_rule, send_bounded_row},
    rdbms_mysql_util::{
        discover_snapshot_tables as discover_snapshot_table_metadata, ensure_binlog_exists,
        fingerprint_table_rules, load_source_metadata, mysql_value_to_cdc, query_earliest_position,
        query_master_position, ReplicationTableSchema, SnapshotTable,
    },
    ColumnSelection, ForwardReplicationOptions, MySqlCdcConfig, RedbCheckpointStore, StartupMode,
};

pub struct MySqlCdcReader {
    config: Arc<MySqlCdcConfig>,
    checkpoint_store: Arc<RedbCheckpointStore>,
    cancellation: CancellationToken,
}

impl MySqlCdcReader {
    pub fn init(job: Arc<JobConfig>) -> Result<Self> {
        let config = Arc::new(MySqlCdcConfig::from_job(&job)?);
        let checkpoint_store = Arc::new(
            RedbCheckpointStore::open(&config.checkpoint.path)
                .context("open MySQL CDC checkpoint store")?,
        );
        checkpoint_store
            .validate_source(&config.checkpoint.id, &config.source_id)
            .context("validate MySQL CDC checkpoint source")?;
        Ok(Self {
            config,
            checkpoint_store,
            cancellation: CancellationToken::new(),
        })
    }

    async fn configured_start_offset(&self, identity: &CheckpointIdentity) -> Result<CdcOffset> {
        self.checkpoint_store
            .validate_identity(&self.config.checkpoint.id, identity)
            .context("validate MySQL CDC checkpoint identity")?;
        if let Some(checkpoint) = self
            .checkpoint_store
            .load(&self.config.checkpoint.id)
            .context("load MySQL CDC checkpoint")?
        {
            return Ok((*checkpoint.confirmed_offset).clone());
        }
        match self.config.startup.mode {
            StartupMode::Specific => self.config.startup.offset.clone().ok_or_else(|| {
                anyhow::anyhow!("specific startup requires source.config.startup.offset")
            }),
            StartupMode::Latest => self.query_server_position(false).await,
            StartupMode::Earliest => self.query_server_position(true).await,
            StartupMode::Initial => Ok(CdcOffset::Position {
                file: String::new(),
                position: 4,
            }),
            StartupMode::Timestamp => self.resolve_timestamp().await,
        }
    }

    async fn resolve_timestamp(&self) -> Result<CdcOffset> {
        let timestamp = self.config.startup.timestamp.as_deref().ok_or_else(|| {
            anyhow::anyhow!("timestamp startup requires source.config.startup.timestamp")
        })?;
        let requested = DateTime::parse_from_rfc3339(timestamp)
            .context("startup timestamp must be offset-aware RFC 3339")?
            .timestamp();
        let earliest = self.query_server_position(true).await?;
        let latest = self.query_server_position(false).await?;
        let mut client = MySqlAsyncReplicationClient::new(self.adapter_config(earliest.clone())?);
        client.connect().await.map_err(anyhow::Error::new)?;
        let located = async {
            let mut transaction_start = None;
            let mut transaction_matches = false;
            let mut saw_timestamp = false;
            loop {
                let event = tokio::time::timeout(Duration::from_secs(30), client.next_event())
                    .await
                    .context("timed out while locating the startup timestamp")?
                    .map_err(anyhow::Error::new)?
                    .ok_or_else(|| {
                        anyhow::anyhow!("replication stream ended during timestamp lookup")
                    })?;
                match event {
                    super::RawReplicationEvent::Begin { offset, .. } => {
                        transaction_start = Some(offset);
                        transaction_matches = false;
                    }
                    super::RawReplicationEvent::Row {
                        timestamp_seconds,
                        offset,
                        ..
                    } => {
                        if !saw_timestamp {
                            saw_timestamp = true;
                            if requested < i64::from(timestamp_seconds) {
                                bail!(
                                    "startup timestamp predates the earliest retained transaction"
                                );
                            }
                        }
                        transaction_start.get_or_insert(offset);
                        if i64::from(timestamp_seconds) >= requested {
                            transaction_matches = true;
                        }
                    }
                    super::RawReplicationEvent::Commit { offset } => {
                        if transaction_matches {
                            return transaction_start.take().ok_or_else(|| {
                                anyhow::anyhow!(
                                    "timestamp locator lost the transaction start boundary"
                                )
                            });
                        }
                        transaction_start = None;
                        if offset_reached(&offset, &latest) {
                            bail!(
                                "no retained transaction exists at or after the startup timestamp"
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
        .await;
        let closed = tokio::time::timeout(Duration::from_secs(5), client.close())
            .await
            .context("timed out while closing the timestamp locator")?
            .map_err(anyhow::Error::new);
        match (located, closed) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(offset), Ok(())) => Ok(offset),
        }
    }

    async fn query_server_position(&self, earliest: bool) -> Result<CdcOffset> {
        let config = self.adapter_config(CdcOffset::Position {
            file: String::new(),
            position: 4,
        })?;
        let mut connection = Conn::new(config.mysql_opts()?)
            .await
            .map_err(|error| anyhow::anyhow!("resolve MySQL CDC startup offset: {error}"))?;
        let offset = if earliest {
            query_earliest_position(&mut connection).await?
        } else {
            query_master_position(&mut connection).await?
        };
        connection.disconnect().await?;
        Ok(offset)
    }

    fn adapter_config(&self, start_offset: CdcOffset) -> Result<AdapterConfig> {
        Ok(AdapterConfig {
            host: self.config.connection.host.clone(),
            port: self.config.connection.port,
            username: self.config.connection.username.clone(),
            password: self.config.connection.password.resolve()?,
            database: self.config.connection.database.clone(),
            server_id: self.config.replication.server_id,
            start_offset,
            tls: tls_mode(&self.config.tls, &self.config.connection.host),
            protocol_compression: self.config.replication.protocol_compression,
            heartbeat_interval: Duration::from_secs(self.config.heartbeat.interval_seconds),
            expected_server_uuid: None,
            expected_schema_fingerprint: None,
        })
    }

    async fn checkpoint_identity(&self) -> Result<CheckpointIdentity> {
        let config = self.adapter_config(CdcOffset::Position {
            file: String::new(),
            position: 4,
        })?;
        let mut connection = Conn::new(config.mysql_opts()?)
            .await
            .context("connect for MySQL CDC checkpoint identity")?;
        let metadata = load_source_metadata(&mut connection, &self.config.connection.database)
            .await
            .context("discover MySQL CDC checkpoint identity")?;
        connection
            .disconnect()
            .await
            .context("close MySQL CDC checkpoint identity connection")?;
        Ok(CheckpointIdentity::new(
            &self.config.source_id,
            metadata.server_uuid,
            &self.config.connection.database,
            fingerprint_table_rules(&self.config.tables),
            metadata.schema_fingerprint,
        ))
    }
}

#[async_trait::async_trait]
impl DataReaderJob for MySqlCdcReader {
    async fn split(&self, _reader_threads: usize) -> Result<SplitReaderResult> {
        Ok(SplitReaderResult {
            total_records: 0,
            tasks: vec![ReadTask {
                task_id: 0,
                conn: serde_json::Value::Null,
                query_sql: None,
                offset: 0,
                limit: 0,
            }],
            stream_mode: StreamMode::Streaming,
        })
    }

    fn description(&self) -> String {
        format!("MySqlCdcReader (source_id: {})", self.config.source_id)
    }

    fn sink_requirements(&self) -> SinkRequirements {
        SinkRequirements {
            transactional_writes: false,
            idempotent_snapshot: self.config.startup.mode == StartupMode::Initial,
            idempotent_cdc: true,
            supports_multi_table: true,
        }
    }
}

#[async_trait::async_trait]
impl DataReaderTask for MySqlCdcReader {
    async fn read_data(&self, _task: &ReadTask) -> Result<JsonStream> {
        bail!("mysql_cdc emits transaction-aware pipeline messages, not a JSON row stream")
    }

    async fn read_pipeline(&self, _task: &ReadTask) -> Result<Option<PipelineStream>> {
        let checkpoint_identity = Arc::new(self.checkpoint_identity().await?);
        let checkpoint_exists = self
            .checkpoint_store
            .load(&self.config.checkpoint.id)
            .context("load MySQL CDC checkpoint")?
            .is_some();
        let start_offset = self.configured_start_offset(&checkpoint_identity).await?;
        let config = Arc::clone(&self.config);
        let checkpoint_store = Arc::clone(&self.checkpoint_store);
        let cancellation = self.cancellation.child_token();
        let (sender, receiver) = mpsc::channel::<Result<PipelineMessage>>(config.buffer.events);
        let byte_capacity = Arc::new(Semaphore::new(config.buffer.bytes));
        let mut adapter_config = self.adapter_config(start_offset.clone())?;
        adapter_config.expected_server_uuid = Some(checkpoint_identity.server_uuid.clone());
        adapter_config.expected_schema_fingerprint =
            Some(checkpoint_identity.schema_fingerprint.clone());

        tokio::spawn(async move {
            let mut next_offset = start_offset;
            if config.startup.mode == StartupMode::Initial && !checkpoint_exists {
                match run_initial_snapshot(
                    &config,
                    &adapter_config,
                    &checkpoint_identity,
                    Arc::clone(&checkpoint_store),
                    Arc::clone(&byte_capacity),
                    &sender,
                    &cancellation,
                )
                .await
                {
                    Ok(snapshot_offset) => next_offset = snapshot_offset,
                    Err(_) if cancellation.is_cancelled() => return,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
            let mut attempt = 0_u32;
            loop {
                if cancellation.is_cancelled() {
                    break;
                }
                let mut client = MySqlAsyncReplicationClient::new(AdapterConfig {
                    start_offset: next_offset.clone(),
                    ..adapter_config.clone()
                });
                let result = forward_replication_client(
                    &mut client,
                    ForwardReplicationOptions {
                        checkpoint_store: Arc::clone(&checkpoint_store),
                        checkpoint_id: &config.checkpoint.id,
                        checkpoint_identity: &checkpoint_identity,
                        table_rules: &config.tables,
                        byte_capacity: Arc::clone(&byte_capacity),
                        max_message_bytes: config.buffer.bytes,
                        heartbeat_timeout: Duration::from_secs(config.heartbeat.timeout_seconds),
                        sender: sender.clone(),
                        cancellation: &cancellation,
                    },
                )
                .await;
                match result {
                    Ok(_) if cancellation.is_cancelled() => break,
                    Ok(_) => {
                        let _ = sender
                            .send(Err(anyhow::anyhow!(
                                "MySQL replication stream ended unexpectedly"
                            )))
                            .await;
                        break;
                    }
                    Err(error) if error.retryable => {
                        attempt = attempt.saturating_add(1);
                        if config
                            .retry
                            .max_attempts
                            .is_some_and(|maximum| attempt > maximum)
                        {
                            let _ = sender.send(Err(anyhow::Error::new(error))).await;
                            break;
                        }
                        if let Ok(Some(checkpoint)) = checkpoint_store.load(&config.checkpoint.id) {
                            next_offset = (*checkpoint.confirmed_offset).clone();
                        }
                        let exponent = attempt.saturating_sub(1).min(16);
                        let multiplier = 1_u64 << exponent;
                        let delay = config
                            .retry
                            .initial_delay_ms
                            .saturating_mul(multiplier)
                            .min(config.retry.max_delay_ms);
                        tokio::select! {
                            () = cancellation.cancelled() => break,
                            () = tokio::time::sleep(Duration::from_millis(delay)) => {}
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(anyhow::Error::new(error))).await;
                        break;
                    }
                }
            }
        });

        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        Ok(Some(Box::pin(stream)))
    }

    fn shutdown(&self) {
        self.cancellation.cancel();
    }
}

fn tls_mode(config: &TlsConfig, host: &str) -> TlsMode {
    match config {
        TlsConfig::Disabled => TlsMode::Disabled,
        TlsConfig::Encrypted => TlsMode::Encrypted,
        TlsConfig::Required {
            ca_path,
            server_name,
        } => TlsMode::Required {
            ca_path: ca_path.clone(),
            server_name: server_name.clone().unwrap_or_else(|| host.to_owned()),
        },
        TlsConfig::Mutual {
            ca_path,
            server_name,
            client_cert_path,
            client_key_path,
        } => TlsMode::Mutual {
            ca_path: ca_path.clone(),
            server_name: server_name.clone().unwrap_or_else(|| host.to_owned()),
            client_cert_path: client_cert_path.clone(),
            client_key_path: client_key_path.clone(),
        },
    }
}

fn offset_reached(current: &CdcOffset, target: &CdcOffset) -> bool {
    current.file() > target.file()
        || (current.file() == target.file() && current.position() >= target.position())
}

async fn run_initial_snapshot(
    config: &MySqlCdcConfig,
    adapter_config: &AdapterConfig,
    checkpoint_identity: &CheckpointIdentity,
    checkpoint_store: Arc<RedbCheckpointStore>,
    byte_capacity: Arc<Semaphore>,
    sender: &mpsc::Sender<Result<PipelineMessage>>,
    cancellation: &CancellationToken,
) -> Result<CdcOffset> {
    let mut connection = Conn::new(adapter_config.mysql_opts()?)
        .await
        .context("connect for MySQL initial snapshot")?;
    let captured_offset = match begin_consistent_snapshot(&mut connection).await {
        Ok(offset) => offset,
        Err(error) => {
            let _ = connection.disconnect().await;
            return Err(error);
        }
    };

    let snapshot_result = stream_snapshot_rows(
        &mut connection,
        config,
        checkpoint_identity,
        &captured_offset,
        Arc::clone(&byte_capacity),
        sender,
        cancellation,
    )
    .await;

    let event_count = match snapshot_result {
        Ok(event_count) => {
            if let Err(error) = ensure_binlog_exists(&mut connection, captured_offset.file()).await
            {
                let _ = connection.query_drop("ROLLBACK").await;
                let _ = connection.disconnect().await;
                return Err(error);
            }
            if let Err(error) = connection.query_drop("COMMIT").await {
                let _ = connection.query_drop("ROLLBACK").await;
                let _ = connection.disconnect().await;
                return Err(error).context("commit MySQL initial snapshot transaction");
            }
            event_count
        }
        Err(error) => {
            let _ = connection.query_drop("ROLLBACK").await;
            let _ = connection.disconnect().await;
            return Err(error);
        }
    };
    connection
        .disconnect()
        .await
        .context("close MySQL initial snapshot connection")?;

    let offset = Arc::new(captured_offset.clone());
    let transaction_id: Arc<str> = format!(
        "snapshot:{}:{}:{}",
        config.source_id,
        captured_offset.file(),
        captured_offset.position()
    )
    .into();
    let commit = TransactionCommit {
        schema_version: CDC_SCHEMA_VERSION,
        event_id: format!(
            "snapshot_commit:{}:{}",
            captured_offset.file(),
            captured_offset.position()
        ),
        transaction_id,
        offset,
        event_count,
    };
    let mut sequence = 0;
    acknowledge_and_checkpoint(
        &checkpoint_store,
        &config.checkpoint.id,
        checkpoint_identity,
        &mut sequence,
        sender,
        cancellation,
        commit,
    )
    .await
    .context("persist acknowledged MySQL snapshot checkpoint")?;
    Ok(captured_offset)
}

async fn begin_consistent_snapshot(connection: &mut Conn) -> Result<CdcOffset> {
    connection
        .query_drop("FLUSH TABLES WITH READ LOCK")
        .await
        .context("acquire MySQL snapshot read lock")?;
    let start_result = async {
        connection
            .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .context("set MySQL snapshot isolation")?;
        connection
            .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await
            .context("start MySQL consistent snapshot")?;
        query_master_position(connection).await
    }
    .await;
    let unlock_result = connection
        .query_drop("UNLOCK TABLES")
        .await
        .context("release MySQL snapshot read lock");
    match (start_result, unlock_result) {
        (Ok(offset), Ok(())) => Ok(offset),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

async fn stream_snapshot_rows(
    connection: &mut Conn,
    config: &MySqlCdcConfig,
    checkpoint_identity: &CheckpointIdentity,
    captured_offset: &CdcOffset,
    byte_capacity: Arc<Semaphore>,
    sender: &mpsc::Sender<Result<PipelineMessage>>,
    cancellation: &CancellationToken,
) -> Result<u64> {
    let metadata = load_source_metadata(connection, &config.connection.database).await?;
    if metadata.server_uuid != checkpoint_identity.server_uuid {
        bail!("MySQL server identity changed before the initial snapshot");
    }
    if metadata.schema_fingerprint != checkpoint_identity.schema_fingerprint {
        bail!("MySQL schema changed before the initial snapshot");
    }
    let tables = discover_snapshot_tables(connection, config).await?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(u64::from(u32::MAX)) as u32;
    let transaction_id: Arc<str> = format!(
        "snapshot:{}:{}:{}",
        config.source_id,
        captured_offset.file(),
        captured_offset.position()
    )
    .into();
    let offset = Arc::new(captured_offset.clone());
    let database = quote_identifier(&config.connection.database)?;
    let mut event_count = 0_u64;

    for table in tables {
        if cancellation.is_cancelled() {
            bail!("MySQL initial snapshot was cancelled");
        }
        let rule = selected_rule(&config.tables, &config.connection.database, &table.name)
            .map_err(anyhow::Error::new)?
            .ok_or_else(|| anyhow::anyhow!("snapshot table no longer matches a table rule"))?;
        let schema = metadata
            .schemas
            .get(&(config.connection.database.clone(), table.name.clone()))
            .ok_or_else(|| anyhow::anyhow!("snapshot table is absent from schema discovery"))?;
        let policy = match rule.initial_target_policy {
            super::InitialTargetPolicy::RequireEmpty => CdcSnapshotTargetPolicy::RequireEmpty,
            super::InitialTargetPolicy::Upsert => CdcSnapshotTargetPolicy::Upsert,
            super::InitialTargetPolicy::TruncateThenLoad => {
                bail!("truncate_then_load passed configuration validation unexpectedly")
            }
        };
        sender
            .send(Ok(PipelineMessage::CdcSnapshotTable(CdcSnapshotTable {
                source_database: config.connection.database.clone(),
                source_table: table.name.clone(),
                target_table: rule.target.clone(),
                policy,
            })))
            .await
            .context("CDC pipeline closed while preparing a snapshot table")?;
        let table_name = quote_identifier(&table.name)?;
        let ordering = table
            .key_columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let query = format!("SELECT * FROM {database}.{table_name} ORDER BY {ordering}");
        let mut result = connection
            .query_iter(query)
            .await
            .with_context(|| format!("scan snapshot table `{}`", table.name))?;
        {
            let mut rows = result
                .stream::<Row>()
                .await
                .with_context(|| format!("open snapshot stream for table `{}`", table.name))?
                .ok_or_else(|| anyhow::anyhow!("snapshot query returned no result set"))?;
            loop {
                let row = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => bail!("MySQL initial snapshot was cancelled"),
                    row = rows.next() => row,
                };
                let Some(row) = row else { break };
                let row = row.with_context(|| format!("read snapshot table `{}`", table.name))?;
                let (values, key) = snapshot_row(row, &table.key_columns, schema)?;
                let event_id = snapshot_event_id(&config.connection.database, &table.name, &key)?;
                let column_mapping = match &rule.columns {
                    ColumnSelection::All(_) => BTreeMap::new(),
                    ColumnSelection::Mapping(mapping) => mapping.clone(),
                };
                let message = CdcRowMessage {
                    envelope: CdcEnvelope {
                        schema_version: CDC_SCHEMA_VERSION,
                        event_id: CdcEventId::from(event_id),
                        transaction_id: Arc::clone(&transaction_id),
                        transaction_event_index: event_count,
                        row_index: 0,
                        event_timestamp_seconds: timestamp,
                        offset: Arc::clone(&offset),
                        change: RowChange {
                            database: Arc::from(config.connection.database.as_str()),
                            table: Arc::from(table.name.as_str()),
                            operation: CdcOperation::Snapshot,
                            key,
                            before: None,
                            after: Some(values),
                        },
                    },
                    target_table: rule.target.clone(),
                    column_mapping,
                };
                send_bounded_row(
                    sender,
                    Arc::clone(&byte_capacity),
                    config.buffer.bytes,
                    message,
                )
                .await
                .map_err(anyhow::Error::new)?;
                event_count = event_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("snapshot event count overflow"))?;
            }
        }
        result
            .drop_result()
            .await
            .with_context(|| format!("finish snapshot table `{}`", table.name))?;
    }
    Ok(event_count)
}

async fn discover_snapshot_tables(
    connection: &mut Conn,
    config: &MySqlCdcConfig,
) -> Result<Vec<SnapshotTable>> {
    let tables = discover_snapshot_table_metadata(connection, &config.connection.database).await?;
    let mut selected = Vec::new();
    for table in tables {
        if selected_rule(&config.tables, &config.connection.database, &table.name)
            .map_err(anyhow::Error::new)?
            .is_none()
        {
            continue;
        }
        if !table.engine.eq_ignore_ascii_case("innodb") {
            bail!(
                "initial snapshot requires InnoDB; table {} uses {}",
                table.name,
                table.engine
            );
        }
        quote_identifier(&table.name)?;
        if table.key_columns.is_empty() {
            bail!(
                "initial snapshot requires a primary key or a non-null unique key for table {}",
                table.name
            );
        }
        for column in &table.key_columns {
            quote_identifier(column)?;
        }
        selected.push(table);
    }
    if selected.is_empty() {
        bail!("no source tables matched source.config.tables for the initial snapshot");
    }
    Ok(selected)
}

fn snapshot_row(
    row: Row,
    key_columns: &[String],
    schema: &ReplicationTableSchema,
) -> Result<(CdcRow, CdcRow)> {
    let columns = row
        .columns_ref()
        .iter()
        .map(|column| Arc::<str>::from(column.name_str().as_ref()))
        .collect::<Vec<_>>();
    let values = row.unwrap();
    let mut output = Vec::with_capacity(values.len());
    let mut key = Vec::with_capacity(key_columns.len());
    for (ordinal, (column, value)) in columns.into_iter().zip(values).enumerate() {
        let column_schema = schema.columns.get(ordinal).ok_or_else(|| {
            anyhow::anyhow!("snapshot row contains a column absent from schema discovery")
        })?;
        if column_schema.name.as_ref() != column.as_ref() {
            bail!("snapshot row column order differs from schema discovery");
        }
        let value = mysql_value_to_cdc(value, column_schema.value_kind);
        if key_columns
            .iter()
            .any(|key_column| key_column == column.as_ref())
        {
            key.push((Arc::clone(&column), value.clone()));
        }
        output.push((column, value));
    }
    if key.len() != key_columns.len() {
        bail!("snapshot row did not contain every stable-key column")
    }
    output.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    key.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    Ok((
        CdcRow::from_sorted_entries(output),
        CdcRow::from_sorted_entries(key),
    ))
}

fn snapshot_event_id(database: &str, table: &str, key: &CdcRow) -> Result<String> {
    let encoded = serde_json::to_vec(key).context("encode snapshot stable key")?;
    let mut hex = String::with_capacity(encoded.len() * 2);
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for byte in encoded {
        hex.push(char::from(DIGITS[usize::from(byte >> 4)]));
        hex.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    Ok(format!("snapshot:{database}:{table}:{hex}"))
}

fn quote_identifier(identifier: &str) -> Result<String> {
    relus_connector_rdbms::identifier::validate(identifier)?;
    Ok(format!("`{identifier}`"))
}
