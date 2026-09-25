use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use async_trait::async_trait;
use futures_util::StreamExt;
use mysql_async::{
    binlog::events::{Event, EventData, RowsEventData},
    consts::ColumnFlags,
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
    Required {
        ca_path: PathBuf,
        server_name: String,
    },
    Mutual {
        ca_path: PathBuf,
        server_name: String,
        client_cert_path: PathBuf,
        client_key_path: PathBuf,
    },
}

impl std::fmt::Debug for TlsMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => formatter.write_str("Disabled"),
            Self::Required { server_name, .. } => formatter
                .debug_struct("Required")
                .field("ca_path", &"<redacted-path>")
                .field("server_name", server_name)
                .finish(),
            Self::Mutual { server_name, .. } => formatter
                .debug_struct("Mutual")
                .field("ca_path", &"<redacted-path>")
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
            Self::Required {
                ca_path,
                server_name,
            } => {
                validate_tls_paths(ca_path, server_name, None, None)?;
                Ok(Some(verified_ssl_opts(ca_path, server_name, None)))
            }
            Self::Mutual {
                ca_path,
                server_name,
                client_cert_path,
                client_key_path,
            } => {
                validate_tls_paths(
                    ca_path,
                    server_name,
                    Some(client_cert_path),
                    Some(client_key_path),
                )?;
                Ok(Some(verified_ssl_opts(
                    ca_path,
                    server_name,
                    Some((client_cert_path, client_key_path)),
                )))
            }
        }
    }

    pub fn with_server_name(&self, replacement: String) -> Option<Self> {
        match self {
            Self::Disabled => None,
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
            .finish()
    }
}

fn validate_tls_paths(
    ca_path: &std::path::Path,
    server_name: &str,
    client_cert_path: Option<&std::path::Path>,
    client_key_path: Option<&std::path::Path>,
) -> Result<(), ReplicationError> {
    if ca_path.as_os_str().is_empty() {
        return Err(configuration_error("TLS CA path must not be empty"));
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
    ca_path: &std::path::Path,
    server_name: &str,
    identity_paths: Option<(&std::path::Path, &std::path::Path)>,
) -> SslOpts {
    let mut ssl_opts = SslOpts::default()
        .with_root_certs(vec![ca_path.to_path_buf().into()])
        .with_disable_built_in_roots(true)
        .with_danger_tls_hostname_override(Some(server_name.to_owned()));
    if let Some((client_cert_path, client_key_path)) = identity_paths {
        ssl_opts = ssl_opts.with_client_identity(Some(ClientIdentity::new(
            client_cert_path.to_path_buf().into(),
            client_key_path.to_path_buf().into(),
        )));
    }
    ssl_opts
}

#[async_trait]
pub trait MySqlReplicationClient: Send {
    async fn connect(&mut self) -> Result<(), ReplicationError>;
    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError>;
    async fn close(&mut self) -> Result<(), ReplicationError>;
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
    let create_user = connection
        .query_first::<(String, String), _>("SHOW CREATE USER CURRENT_USER()")
        .await
        .map_err(map_mysql_error)?
        .ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Authentication,
                false,
                "the server did not return the current account authentication definition",
            )
        })?
        .1;
    let uses_expected_plugin = create_user.contains(expected_plugin);
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

pub struct MySqlAsyncReplicationClient {
    config: MySqlConnectionConfig,
    stream: Option<BinlogStream>,
    current_file: String,
    current_gtid: Option<String>,
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
            current_file,
            current_gtid,
        }
    }

    fn current_offset(&self, position: u64) -> CdcOffset {
        match &self.current_gtid {
            Some(transaction_gtid) => CdcOffset::Gtid {
                resume_set: match &self.config.start_offset {
                    CdcOffset::Gtid { resume_set, .. } => resume_set.clone(),
                    CdcOffset::Position { .. } => String::new(),
                },
                transaction_gtid: Some(transaction_gtid.clone()),
                file: self.current_file.clone(),
                position,
            },
            None => CdcOffset::Position {
                file: self.current_file.clone(),
                position,
            },
        }
    }

    fn map_event(&mut self, event: Event) -> Result<RawReplicationEvent, ReplicationError> {
        let timestamp_seconds = event.header().timestamp();
        let log_position = u64::from(event.header().log_pos());
        let raw_type = event.header().event_type_raw();
        let data = event.read_data().map_err(|error| {
            ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
        })?;
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
                let changes = decode_rows(&rows, table_map)?;
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
                match query.query().trim().to_ascii_uppercase().as_str() {
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
        if self.stream.is_some() {
            return Ok(());
        }
        let conn = Conn::new(self.config.mysql_opts()?)
            .await
            .map_err(map_mysql_error)?;
        let mut request = BinlogStreamRequest::new(self.config.server_id)
            .with_filename(self.config.start_offset.file().as_bytes())
            .with_pos(self.config.start_offset.position());
        if let CdcOffset::Gtid { resume_set, .. } = &self.config.start_offset {
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
            request = request.with_gtid().with_gtid_set(sids);
        }
        self.stream = Some(
            conn.get_binlog_stream(request)
                .await
                .map_err(map_mysql_error)?,
        );
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<RawReplicationEvent>, ReplicationError> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            ReplicationError::new(
                ReplicationErrorKind::Configuration,
                false,
                "replication client is not connected",
            )
        })?;
        let next = stream.next().await.transpose().map_err(map_mysql_error)?;
        next.map(|event| self.map_event(event)).transpose()
    }

    async fn close(&mut self) -> Result<(), ReplicationError> {
        if let Some(stream) = self.stream.take() {
            stream.close().await.map_err(map_mysql_error)?;
        }
        Ok(())
    }
}

fn decode_rows(
    rows: &RowsEventData<'_>,
    table_map: &mysql_async::binlog::events::TableMapEvent<'_>,
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

    rows.rows(table_map)
        .map(|row| {
            let (before, after) = row.map_err(|error| {
                ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
            })?;
            let before = before.map(binlog_row_to_cdc_row).transpose()?;
            let after = after.map(binlog_row_to_cdc_row).transpose()?;
            let key = after
                .as_ref()
                .filter(|row| !row.key.is_empty())
                .or_else(|| before.as_ref().filter(|row| !row.key.is_empty()))
                .map(|row| row.key.clone())
                .unwrap_or_default();
            Ok(RowChange {
                database: table_map.database_name().into_owned(),
                table: table_map.table_name().into_owned(),
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
    row: mysql_async::binlog::row::BinlogRow,
) -> Result<DecodedRow, ReplicationError> {
    let row = Row::try_from(row).map_err(|error| {
        ReplicationError::new(ReplicationErrorKind::Decode, false, error.to_string())
    })?;
    let columns = row
        .columns_ref()
        .iter()
        .map(|column| {
            (
                column.name_str().into_owned(),
                column.flags().contains(ColumnFlags::PRI_KEY_FLAG),
            )
        })
        .collect::<Vec<_>>();
    let values = row.unwrap();
    let mut output = BTreeMap::new();
    let mut key = BTreeMap::new();
    for ((column, is_primary_key), value) in columns.into_iter().zip(values) {
        let value = mysql_value_to_cdc(value);
        if is_primary_key {
            key.insert(column.clone(), value.clone());
        }
        output.insert(column, value);
    }
    Ok(DecodedRow {
        values: output,
        key,
    })
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
        mysql_async::Error::Server(server) if matches!(server.code, 1044 | 1045 | 1698) => {
            (ReplicationErrorKind::Authentication, false)
        }
        mysql_async::Error::Server(server) if server.code == 1236 => {
            (ReplicationErrorKind::BinlogPurged, false)
        }
        mysql_async::Error::Server(_) => (ReplicationErrorKind::Protocol, false),
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
