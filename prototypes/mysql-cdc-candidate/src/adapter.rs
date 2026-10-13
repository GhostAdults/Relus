use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_async::{
    binlog::{
        events::{Event, EventData, RowsEventData},
        value::BinlogValue,
    },
    prelude::Queryable,
    BinlogStream, BinlogStreamRequest, ClientIdentity, Conn, OptsBuilder, Row, SslOpts, Value,
};
use tokio_util::sync::CancellationToken;

use crate::{
    envelope::{CdcOffset, CdcOperation, CdcRow, CdcValue, RowChange},
    error::{ReplicationError, ReplicationErrorKind},
    transaction::RawReplicationEvent,
};

#[derive(Clone, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    /// Encrypts the connection without authenticating the server certificate.
    /// This mode is only for compatibility testing and is not production-safe.
    Encrypted,
    Required {
        ca_path: Option<PathBuf>,
        server_name: String,
    },
    Mutual {
        ca_path: Option<PathBuf>,
        server_name: String,
        client_cert_path: PathBuf,
        client_key_path: PathBuf,
    },
}

impl std::fmt::Debug for TlsMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("Disabled"),
            Self::Encrypted => formatter.write_str("Encrypted(<identity-unverified>)"),
            Self::Required { server_name, .. } => formatter
                .debug_struct("Required")
                .field("ca_path", &"<optional-redacted-path>")
                .field("server_name", server_name)
                .finish(),
            Self::Mutual { server_name, .. } => formatter
                .debug_struct("Mutual")
                .field("ca_path", &"<optional-redacted-path>")
                .field("server_name", server_name)
                .field("client_cert_path", &"<redacted-path>")
                .field("client_key_path", &"<redacted-path>")
                .finish(),
        }
    }
}

impl TlsMode {
    fn ssl_opts(&self) -> Result<Option<SslOpts>, ReplicationError> {
        match self {
            Self::Disabled => Ok(None),
            Self::Encrypted => Ok(Some(encrypted_ssl_opts())),
            Self::Required {
                ca_path,
                server_name,
            } => {
                validate_tls_paths(ca_path.as_deref(), server_name, None, None)?;
                Ok(Some(verified_ssl_opts(
                    ca_path.as_deref(),
                    server_name,
                    None,
                )))
            }
            Self::Mutual {
                ca_path,
                server_name,
                client_cert_path,
                client_key_path,
            } => {
                validate_tls_paths(
                    ca_path.as_deref(),
                    server_name,
                    Some(client_cert_path),
                    Some(client_key_path),
                )?;
                Ok(Some(verified_ssl_opts(
                    ca_path.as_deref(),
                    server_name,
                    Some((client_cert_path, client_key_path)),
                )))
            }
        }
    }

    pub fn with_server_name(&self, replacement: String) -> Option<Self> {
        match self {
            Self::Disabled | Self::Encrypted => None,
            Self::Required { ca_path, .. } => Some(Self::Required {
                ca_path: ca_path.clone(),
                server_name: replacement,
            }),
            Self::Mutual {
                ca_path,
                client_cert_path,
                client_key_path,
                ..
            } => Some(Self::Mutual {
                ca_path: ca_path.clone(),
                server_name: replacement,
                client_cert_path: client_cert_path.clone(),
                client_key_path: client_key_path.clone(),
            }),
        }
    }
}

#[derive(Clone)]
pub struct MySqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub server_id: u32,
    pub start_offset: CdcOffset,
    pub tls: TlsMode,
    pub protocol_compression: bool,
}

impl MySqlConnectionConfig {
    pub fn validate(&self) -> Result<(), ReplicationError> {
        self.tls.ssl_opts().map(|_| ())
    }

    pub(crate) fn mysql_opts(&self) -> Result<mysql_async::Opts, ReplicationError> {
        let ssl_opts = self.tls.ssl_opts()?;
        let mut opts = OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.username.clone()))
            .pass(Some(self.password.clone()))
            .db_name(Some(self.database.clone()));
        opts = opts.ssl_opts(ssl_opts);
        if self.protocol_compression {
            opts = opts.compression(Some(mysql_async::Compression::fast()));
        }
        Ok(opts.into())
    }
}

impl std::fmt::Debug for MySqlConnectionConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MySqlConnectionConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("server_id", &self.server_id)
            .field("start_offset", &self.start_offset)
            .field("tls", &self.tls)
            .field("protocol_compression", &self.protocol_compression)
            .finish()
    }
}

fn validate_tls_paths(
    ca_path: Option<&std::path::Path>,
    server_name: &str,
    client_cert_path: Option<&std::path::Path>,
    client_key_path: Option<&std::path::Path>,
) -> Result<(), ReplicationError> {
    if ca_path.is_some_and(|path| path.as_os_str().is_empty()) {
        return Err(configuration_error(
            "TLS CA path must not be empty when supplied",
        ));
    }
    if server_name.trim().is_empty() {
        return Err(configuration_error("TLS server name must not be empty"));
    }
    if client_cert_path.is_some_and(|path| path.as_os_str().is_empty()) {
        return Err(configuration_error(
            "mutual TLS client certificate path must not be empty",
        ));
    }
    if client_key_path.is_some_and(|path| path.as_os_str().is_empty()) {
        return Err(configuration_error(
            "mutual TLS client key path must not be empty",
        ));
    }
    Ok(())
}

fn configuration_error(message: &'static str) -> ReplicationError {
    ReplicationError::new(ReplicationErrorKind::Configuration, false, message)
}

fn verified_ssl_opts(
    ca_path: Option<&std::path::Path>,
    server_name: &str,
    identity_paths: Option<(&std::path::Path, &std::path::Path)>,
) -> SslOpts {
    let mut ssl_opts =
        SslOpts::default().with_danger_tls_hostname_override(Some(server_name.to_owned()));
    if let Some(ca_path) = ca_path {
        ssl_opts = ssl_opts
            .with_root_certs(vec![ca_path.to_path_buf().into()])
            .with_disable_built_in_roots(true);
    }
    if let Some((client_cert_path, client_key_path)) = identity_paths {
        ssl_opts = ssl_opts.with_client_identity(Some(ClientIdentity::new(
            client_cert_path.to_path_buf().into(),
            client_key_path.to_path_buf().into(),
        )));
    }
    ssl_opts
}

fn encrypted_ssl_opts() -> SslOpts {
    SslOpts::default()
        .with_danger_accept_invalid_certs(true)
        .with_danger_skip_domain_validation(true)
}

#[async_trait]
pub trait MySqlReplicationClient: Send {
    async fn connect(&mut self) -> Result<(), ReplicationError>;
    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError>;
    async fn close(&mut self) -> Result<(), ReplicationError>;

    fn is_finite_stream(&self) -> bool {
        false
    }
}

pub async fn consume_until_cancelled<C: MySqlReplicationClient + ?Sized>(
    client: &mut C,
    cancellation: &CancellationToken,
    close_timeout: Duration,
) -> Result<(), ReplicationError> {
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            result = client.next_event() => {
                if result?.is_none() {
                    break;
                }
            }
        }
    }

    match tokio::time::timeout(close_timeout, client.close()).await {
        Ok(result) => result,
        Err(_) => Err(ReplicationError::new(
            ReplicationErrorKind::Shutdown,
            false,
            "replication connection did not close before the shutdown timeout",
        )),
    }
}

pub(crate) async fn verify_connection(
    config: &MySqlConnectionConfig,
) -> Result<(), ReplicationError> {
    let connection = Conn::new(config.mysql_opts()?)
        .await
        .map_err(map_mysql_error)?;
    connection.disconnect().await.map_err(map_mysql_error)
}

pub(crate) async fn verify_authentication_plugin(
    config: &MySqlConnectionConfig,
    expected_plugin: &'static str,
) -> Result<(), ReplicationError> {
    let mut connection = Conn::new(config.mysql_opts()?)
        .await
        .map_err(map_mysql_error)?;
    let actual_plugin = connection
        .query_first::<Row, _>(
            "SELECT plugin FROM mysql.user \
             WHERE User = SUBSTRING_INDEX(CURRENT_USER(), '@', 1) \
             AND Host = SUBSTRING_INDEX(CURRENT_USER(), '@', -1)",
        )
        .await
        .map_err(map_mysql_error)?
        .and_then(|row| row.get::<String, _>(0))
        .ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Authentication,
                false,
                "the server did not return the current account authentication plugin",
            )
        })?;
    let uses_expected_plugin = actual_plugin == expected_plugin;
    connection.disconnect().await.map_err(map_mysql_error)?;
    if !uses_expected_plugin {
        return Err(ReplicationError::new(
            ReplicationErrorKind::Authentication,
            false,
            "the supplied account does not use the expected authentication plugin",
        ));
    }
    Ok(())
}

async fn load_replication_schemas(
    connection: &mut Conn,
    database: &str,
) -> Result<BTreeMap<(String, String), ReplicationTableSchema>, ReplicationError> {
    let rows: Vec<Row> = connection
        .exec(
            "SELECT CAST(TABLE_NAME AS CHAR), \
                    CAST(COLUMN_NAME AS CHAR), \
                    ORDINAL_POSITION, \
                    CAST(COLUMN_KEY AS CHAR), \
                    CAST(COLUMN_TYPE AS CHAR), \
                    CAST(DATA_TYPE AS CHAR) \
             FROM information_schema.columns \
             WHERE TABLE_SCHEMA = ? \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
            (database,),
        )
        .await
        .map_err(map_mysql_error)?;
    let mut discovered = BTreeMap::<(String, String), Vec<ReplicationColumnSchema>>::new();
    for row in rows {
        let table = row.get::<String, _>(0).ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                "schema discovery did not return a table name",
            )
        })?;
        let name = row.get::<String, _>(1).ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                "schema discovery did not return a column name",
            )
        })?;
        let ordinal = row.get::<u64, _>(2).ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                "schema discovery did not return a column ordinal",
            )
        })?;
        let ordinal = usize::try_from(ordinal.saturating_sub(1)).map_err(|_| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                "schema discovery returned an unsupported column ordinal",
            )
        })?;
        let column_key = row.get::<String, _>(3).unwrap_or_default();
        let column_type = row.get::<String, _>(4).ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                "schema discovery did not return a column type",
            )
        })?;
        let data_type = row.get::<String, _>(5).ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                "schema discovery did not return a data type",
            )
        })?;
        let is_unsigned = column_type
            .split_ascii_whitespace()
            .any(|part| part.eq_ignore_ascii_case("unsigned"));
        discovered
            .entry((database.to_owned(), table))
            .or_default()
            .push(ReplicationColumnSchema {
                ordinal,
                name: Arc::from(name),
                is_primary_key: column_key.eq_ignore_ascii_case("PRI"),
                unsigned_integer_bits: is_unsigned
                    .then(|| mysql_integer_bits(&data_type))
                    .flatten(),
            });
    }

    discovered
        .into_iter()
        .map(|(table, mut columns)| {
            columns.sort_unstable_by_key(|column| column.ordinal);
            if columns
                .iter()
                .enumerate()
                .any(|(expected, column)| column.ordinal != expected)
            {
                return Err(ReplicationError::new(
                    ReplicationErrorKind::Decode,
                    false,
                    "schema discovery returned non-contiguous column ordinals",
                ));
            }
            let primary_key_count = columns
                .iter()
                .filter(|column| column.is_primary_key)
                .count();
            Ok((
                table,
                ReplicationTableSchema {
                    columns,
                    primary_key_count,
                },
            ))
        })
        .collect()
}

pub struct MySqlAsyncReplicationClient {
    config: MySqlConnectionConfig,
    stream: Option<BinlogStream>,
    schemas: BTreeMap<(String, String), ReplicationTableSchema>,
    current_file: String,
    current_gtid: Option<String>,
}

#[derive(Clone)]
struct ReplicationColumnSchema {
    ordinal: usize,
    name: Arc<str>,
    is_primary_key: bool,
    unsigned_integer_bits: Option<u32>,
}

#[derive(Clone)]
struct ReplicationTableSchema {
    columns: Vec<ReplicationColumnSchema>,
    primary_key_count: usize,
}

impl MySqlAsyncReplicationClient {
    pub fn new(config: MySqlConnectionConfig) -> Self {
        let current_file = config.start_offset.file().to_owned();
        let current_gtid = match &config.start_offset {
            CdcOffset::Gtid {
                transaction_gtid, ..
            } => transaction_gtid.clone(),
            CdcOffset::Position { .. } => None,
        };
        Self {
            config,
            stream: None,
            schemas: BTreeMap::new(),
            current_file,
            current_gtid,
        }
    }

    pub async fn connect(&mut self) -> Result<(), ReplicationError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let mut conn = Conn::new(self.config.mysql_opts()?)
            .await
            .map_err(map_mysql_error)?;
        self.schemas = load_replication_schemas(&mut conn, &self.config.database).await?;
        let request = match &self.config.start_offset {
            CdcOffset::Position { file, position } => {
                BinlogStreamRequest::new(self.config.server_id)
                    .with_filename(file.as_bytes())
                    .with_pos(*position)
            }
            CdcOffset::Gtid { resume_set, .. } => {
                let sids = resume_set
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(str::parse::<mysql_async::Sid>)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| {
                        ReplicationError::new(
                            ReplicationErrorKind::InvalidOffset,
                            false,
                            error.to_string(),
                        )
                    })?;
                BinlogStreamRequest::new(self.config.server_id)
                    .with_gtid()
                    .with_gtid_set(sids)
            }
        };
        self.stream = Some(
            conn.get_binlog_stream(request)
                .await
                .map_err(map_mysql_error)?,
        );
        Ok(())
    }

    pub async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        let next = self.read_event().await?;
        next.map(|event| self.map_event(event)).transpose()
    }

    pub async fn next_event_profiled(
        &mut self,
    ) -> Result<
        (
            Option<RawReplicationEvent>,
            std::time::Duration,
            std::time::Duration,
            std::time::Duration,
        ),
        ReplicationError,
    > {
        let read_started = std::time::Instant::now();
        let next = self.read_event().await?;
        let read_elapsed = read_started.elapsed();
        let Some(event) = next else {
            return Ok((
                None,
                read_elapsed,
                std::time::Duration::ZERO,
                std::time::Duration::ZERO,
            ));
        };
        let parse_started = std::time::Instant::now();
        let data = event.read_data().map_err(|error| {
            ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
        })?;
        let parse_elapsed = parse_started.elapsed();
        let map_started = std::time::Instant::now();
        let mapped = self.map_event_data(&event, data)?;
        Ok((
            Some(mapped),
            read_elapsed,
            parse_elapsed,
            map_started.elapsed(),
        ))
    }

    pub async fn close(&mut self) -> Result<(), ReplicationError> {
        if let Some(stream) = self.stream.take() {
            stream.close().await.map_err(map_mysql_error)?;
        }
        Ok(())
    }

    async fn read_event(&mut self) -> Result<Option<Event>, ReplicationError> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Configuration,
                false,
                "replication client is not connected",
            )
        })?;
        stream.next().await.transpose().map_err(map_mysql_error)
    }

    fn current_offset(&self, position: u64) -> CdcOffset {
        match &self.config.start_offset {
            CdcOffset::Gtid { resume_set, .. } => CdcOffset::Gtid {
                resume_set: resume_set.clone(),
                transaction_gtid: self.current_gtid.clone(),
                file: self.current_file.clone(),
                position,
            },
            CdcOffset::Position { .. } => CdcOffset::Position {
                file: self.current_file.clone(),
                position,
            },
        }
    }

    fn map_event(&mut self, event: Event) -> Result<RawReplicationEvent, ReplicationError> {
        let data = event.read_data().map_err(|error| {
            ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
        })?;
        self.map_event_data(&event, data)
    }

    fn map_event_data(
        &mut self,
        event: &Event,
        data: Option<EventData<'_>>,
    ) -> Result<RawReplicationEvent, ReplicationError> {
        let timestamp_seconds = event.header().timestamp();
        let log_position = u64::from(event.header().log_pos());
        let raw_type = event.header().event_type_raw();
        let Some(data) = data else {
            return Ok(RawReplicationEvent::Unsupported {
                event_type: raw_type,
            });
        };

        match data {
            EventData::GtidEvent(gtid) => {
                let transaction_id =
                    format_gtid(gtid.sid(), gtid.tag().map(ToString::to_string), gtid.gno());
                self.current_gtid = Some(transaction_id.clone());
                Ok(RawReplicationEvent::Begin {
                    transaction_id,
                    offset: self.current_offset(log_position),
                })
            }
            EventData::RowsEvent(rows) => {
                let table_map = self
                    .stream
                    .as_ref()
                    .and_then(|stream| stream.get_tme(rows.table_id()))
                    .ok_or_else(|| {
                        ReplicationError::new(
                            ReplicationErrorKind::Protocol,
                            false,
                            format!("missing TABLE_MAP for table id {}", rows.table_id()),
                        )
                    })?;
                let database = table_map.database_name();
                if database.as_ref() != self.config.database {
                    return Ok(RawReplicationEvent::Harmless);
                }
                let table = table_map.table_name();
                let schema = self
                    .schemas
                    .get(&(database.into_owned(), table.into_owned()))
                    .ok_or_else(|| {
                        ReplicationError::new(
                            ReplicationErrorKind::SchemaDrift,
                            false,
                            "replication row referenced a table absent from schema discovery",
                        )
                    })?;
                let changes = decode_rows(&rows, table_map, schema)?;
                Ok(RawReplicationEvent::Row {
                    timestamp_seconds,
                    offset: self.current_offset(log_position),
                    rows: changes,
                })
            }
            EventData::XidEvent(_) => {
                let result = RawReplicationEvent::Commit {
                    offset: self.current_offset(log_position),
                };
                self.current_gtid = None;
                Ok(result)
            }
            EventData::QueryEvent(query) => {
                let statement = query.query().trim().to_owned();
                if let Some(table) = schema_change_table(&statement) {
                    return Ok(RawReplicationEvent::SchemaChanged {
                        database: self.config.database.clone(),
                        table,
                        offset: self.current_offset(log_position),
                    });
                }
                match statement.to_ascii_uppercase().as_str() {
                    "BEGIN" => {
                        if self.current_gtid.is_some() {
                            Ok(RawReplicationEvent::Harmless)
                        } else {
                            let offset = self.current_offset(log_position);
                            Ok(RawReplicationEvent::Begin {
                                transaction_id: offset.transaction_id(),
                                offset,
                            })
                        }
                    }
                    "COMMIT" => {
                        let result = RawReplicationEvent::Commit {
                            offset: self.current_offset(log_position),
                        };
                        self.current_gtid = None;
                        Ok(result)
                    }
                    _ => Ok(RawReplicationEvent::Unsupported {
                        event_type: raw_type,
                    }),
                }
            }
            EventData::RotateEvent(rotate) => {
                self.current_file = rotate.name().into_owned();
                let offset = self.current_offset(rotate.position());
                Ok(RawReplicationEvent::Rotate {
                    file: self.current_file.clone(),
                    position: rotate.position(),
                    offset,
                })
            }
            EventData::HeartbeatEvent => Ok(RawReplicationEvent::Heartbeat {
                offset: self.current_offset(log_position),
            }),
            EventData::TableMapEvent(table_map) => Ok(RawReplicationEvent::TableMap {
                database: table_map.database_name().into_owned(),
                table: table_map.table_name().into_owned(),
                table_id: table_map.table_id(),
                offset: self.current_offset(log_position),
            }),
            EventData::FormatDescriptionEvent(_)
            | EventData::PreviousGtidsEvent(_)
            | EventData::RowsQueryEvent(_)
            | EventData::IgnorableEvent(_) => Ok(RawReplicationEvent::Harmless),
            EventData::TransactionPayloadEvent(_) => Ok(RawReplicationEvent::Unsupported {
                event_type: raw_type,
            }),
            EventData::UnknownEvent
            | EventData::IncidentEvent(_)
            | EventData::AnonymousGtidEvent(_) => Ok(RawReplicationEvent::Unsupported {
                event_type: raw_type,
            }),
            _ => Ok(RawReplicationEvent::Unsupported {
                event_type: raw_type,
            }),
        }
    }
}

#[async_trait]
impl MySqlReplicationClient for MySqlAsyncReplicationClient {
    async fn connect(&mut self) -> Result<(), ReplicationError> {
        MySqlAsyncReplicationClient::connect(self).await
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        MySqlAsyncReplicationClient::next_event(self).await
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        MySqlAsyncReplicationClient::close(self).await
    }
}

fn decode_rows(
    rows: &RowsEventData<'_>,
    table_map: &mysql_async::binlog::events::TableMapEvent<'_>,
    schema: &ReplicationTableSchema,
) -> Result<Vec<RowChange>, ReplicationError> {
    let operation = match rows {
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => {
            CdcOperation::Insert
        }
        RowsEventData::UpdateRowsEventV1(_) | RowsEventData::UpdateRowsEvent(_) => {
            CdcOperation::Update
        }
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
            CdcOperation::Delete
        }
        RowsEventData::PartialUpdateRowsEvent(_) => {
            return Err(ReplicationError::new(
                ReplicationErrorKind::UnsupportedEvent,
                false,
                "partial update row images are not supported",
            ));
        }
    };
    let database: Arc<str> = Arc::from(table_map.database_name().as_ref());
    let table: Arc<str> = Arc::from(table_map.table_name().as_ref());

    rows.rows(table_map)
        .map(|row| {
            let (before, after) = row.map_err(|error| {
                ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
            })?;
            let mut decode = |row| binlog_row_to_cdc_row(row, schema);
            let mut before = before.map(&mut decode).transpose()?;
            let mut after = after.map(&mut decode).transpose()?;
            let key = match (&mut after, &mut before) {
                (Some(row), _) if !row.key.is_empty() => std::mem::take(&mut row.key),
                (_, Some(row)) if !row.key.is_empty() => std::mem::take(&mut row.key),
                _ => CdcRow::new(),
            };
            Ok(RowChange {
                database: Arc::clone(&database),
                table: Arc::clone(&table),
                operation,
                key,
                before: before.map(|row| row.values),
                after: after.map(|row| row.values),
            })
        })
        .collect()
}

struct DecodedRow {
    values: CdcRow,
    key: CdcRow,
}

fn binlog_row_to_cdc_row(
    mut row: mysql_async::binlog::row::BinlogRow,
    schema: &ReplicationTableSchema,
) -> Result<DecodedRow, ReplicationError> {
    if row.columns_ref().len() != schema.columns.len() {
        return Err(ReplicationError::new(
            ReplicationErrorKind::SchemaDrift,
            false,
            "binlog row column count differs from discovered schema",
        ));
    }
    let mut output = Vec::with_capacity(schema.columns.len());
    let mut key = Vec::with_capacity(schema.primary_key_count);
    for column in &schema.columns {
        let value = row.take(column.ordinal).ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Decode,
                false,
                format!(
                    "binlog row value was unavailable at column offset {}",
                    column.ordinal
                ),
            )
        })?;
        let value = match value {
            BinlogValue::Value(value) => value,
            other => Value::try_from(other).map_err(|error| {
                ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
            })?,
        };
        let value = normalize_integer_signedness(value, column.unsigned_integer_bits);
        let value = mysql_value_to_cdc(value);
        if column.is_primary_key {
            key.push((Arc::clone(&column.name), value.clone()));
        }
        output.push((Arc::clone(&column.name), value));
    }
    output.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    key.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    Ok(DecodedRow {
        values: CdcRow::from_sorted_entries(output),
        key: CdcRow::from_sorted_entries(key),
    })
}

fn normalize_integer_signedness(value: Value, unsigned_integer_bits: Option<u32>) -> Value {
    match (value, unsigned_integer_bits) {
        (Value::Int(value), Some(bits)) => {
            let mask = if bits == u64::BITS {
                u64::MAX
            } else {
                (1_u64 << bits) - 1
            };
            Value::UInt((value as u64) & mask)
        }
        (value, _) => value,
    }
}

fn mysql_integer_bits(data_type: &str) -> Option<u32> {
    if data_type.eq_ignore_ascii_case("tinyint") {
        Some(8)
    } else if data_type.eq_ignore_ascii_case("smallint") {
        Some(16)
    } else if data_type.eq_ignore_ascii_case("mediumint") {
        Some(24)
    } else if data_type.eq_ignore_ascii_case("int") || data_type.eq_ignore_ascii_case("integer") {
        Some(32)
    } else if data_type.eq_ignore_ascii_case("bigint") {
        Some(64)
    } else {
        None
    }
}

fn mysql_value_to_cdc(value: Value) -> CdcValue {
    match value {
        Value::NULL => CdcValue::Null,
        Value::Bytes(bytes) => match String::from_utf8(bytes) {
            Ok(string) => CdcValue::String(string),
            Err(error) => CdcValue::Bytes(error.into_bytes()),
        },
        Value::Int(value) => CdcValue::Signed(value),
        Value::UInt(value) => CdcValue::Unsigned(value),
        Value::Float(value) => CdcValue::Float(f64::from(value)),
        Value::Double(value) => CdcValue::Float(value),
        Value::Date(year, month, day, 0, 0, 0, 0) => CdcValue::Date { year, month, day },
        Value::Date(year, month, day, hour, minute, second, micros) => CdcValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        },
        Value::Time(negative, days, hours, minutes, seconds, micros) => CdcValue::Time {
            negative,
            days,
            hours,
            minutes,
            seconds,
            micros,
        },
    }
}

fn schema_change_table(statement: &str) -> Option<String> {
    let tokens = statement
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let command = tokens.first()?.to_ascii_uppercase();
    if !matches!(
        command.as_str(),
        "ALTER" | "CREATE" | "DROP" | "RENAME" | "TRUNCATE"
    ) {
        return None;
    }
    let table_index = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("TABLE"))?;
    tokens
        .iter()
        .skip(table_index + 1)
        .find(|token| {
            !token.eq_ignore_ascii_case("IF")
                && !token.eq_ignore_ascii_case("NOT")
                && !token.eq_ignore_ascii_case("EXISTS")
                && !token.eq_ignore_ascii_case("OR")
                && !token.eq_ignore_ascii_case("REPLACE")
        })
        .map(|table| (*table).to_owned())
}

fn format_gtid(sid: [u8; 16], tag: Option<String>, gno: u64) -> String {
    let uuid = uuid::Uuid::from_bytes(sid);
    match tag {
        Some(tag) => format!("{uuid}:{tag}:{gno}"),
        None => format!("{uuid}:{gno}"),
    }
}

fn map_mysql_error(error: mysql_async::Error) -> ReplicationError {
    let message = error.to_string();
    let (kind, retryable) = match &error {
        mysql_async::Error::Io(mysql_async::IoError::Tls(_)) => (ReplicationErrorKind::Tls, false),
        mysql_async::Error::Io(_) => (ReplicationErrorKind::Network, true),
        mysql_async::Error::Server(server) => {
            return classify_mysql_server_error(server.code, message)
        }
        mysql_async::Error::Driver(mysql_async::DriverError::UnknownAuthPlugin { .. }) => {
            (ReplicationErrorKind::Authentication, false)
        }
        mysql_async::Error::Driver(mysql_async::DriverError::NoClientSslFlagFromServer)
        | mysql_async::Error::Driver(mysql_async::DriverError::NoKeyFound) => {
            (ReplicationErrorKind::Tls, false)
        }
        mysql_async::Error::Url(_) => (ReplicationErrorKind::Configuration, false),
        mysql_async::Error::Driver(_) => (ReplicationErrorKind::Protocol, false),
        mysql_async::Error::Other(_) => (ReplicationErrorKind::Network, true),
    };
    ReplicationError::new(kind, retryable, message)
}

pub fn classify_mysql_server_error(code: u16, message: impl Into<String>) -> ReplicationError {
    let (kind, retryable) = match code {
        1044 | 1045 | 1698 => (ReplicationErrorKind::Authentication, false),
        1040 | 1053 | 1203 => (ReplicationErrorKind::ServerUnavailable, true),
        1236 => (ReplicationErrorKind::BinlogPurged, false),
        3159 => (ReplicationErrorKind::Tls, false),
        _ => (ReplicationErrorKind::Protocol, false),
    };
    ReplicationError::new(kind, retryable, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(protocol_compression: bool) -> MySqlConnectionConfig {
        MySqlConnectionConfig {
            host: "localhost".to_owned(),
            port: 3306,
            username: "replication".to_owned(),
            password: "secret".to_owned(),
            database: "source".to_owned(),
            server_id: 42,
            start_offset: CdcOffset::Position {
                file: "mysql-bin.000001".to_owned(),
                position: 4,
            },
            tls: TlsMode::Disabled,
            protocol_compression,
        }
    }

    #[test]
    fn protocol_compression_is_explicitly_configurable() -> Result<(), ReplicationError> {
        assert_eq!(config(false).mysql_opts()?.compression(), None);
        assert_eq!(
            config(true).mysql_opts()?.compression(),
            Some(mysql_async::Compression::fast())
        );
        Ok(())
    }
}
