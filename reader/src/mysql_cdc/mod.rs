use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use anyhow::{bail, Context, Result};
use relus_common::{
    cdc::CdcOffset,
    connection::{SecretValue, TlsConfig},
    job_config::{DeliveryGuarantee, JobConfig},
};
use serde::Deserialize;

pub mod adapter;
pub mod checkpoint;
pub mod error;
pub mod production;
mod rdbms_mysql_util;
pub mod reader;
pub mod transaction;

pub use adapter::{
    classify_mysql_server_error, consume_until_cancelled, MySqlAsyncReplicationClient,
    MySqlConnectionConfig as ReplicationConnectionConfig, MySqlReplicationClient, TlsMode,
};
pub use checkpoint::{CdcCheckpoint, CheckpointIdentity, RedbCheckpointStore};
pub use error::{ReplicationError, ReplicationErrorKind};
pub use production::{forward_replication_client, ForwardReplicationOptions};
pub use reader::MySqlCdcReader;
pub use transaction::{RawReplicationEvent, TransactionAssembler};

const CURRENT_CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlCdcConfig {
    pub source_id: String,
    pub connection: MySqlConnectionConfig,
    pub replication: ReplicationConfig,
    pub tls: TlsConfig,
    pub startup: StartupConfig,
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub retry: RetryConfig,
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    #[serde(default)]
    pub buffer: BufferConfig,
    pub tables: Vec<TableRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlConnectionConfig {
    pub host: String,
    #[serde(default = "default_mysql_port")]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: SecretValue,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicationConfig {
    pub server_id: u32,
    #[serde(default)]
    pub protocol_compression: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartupMode {
    Initial,
    Earliest,
    Latest,
    Specific,
    Timestamp,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupConfig {
    pub mode: StartupMode,
    #[serde(default)]
    pub offset: Option<CdcOffset>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointConfig {
    pub id: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RetryConfig {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_attempts: Option<u32>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 500,
            max_delay_ms: 30_000,
            max_attempts: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HeartbeatConfig {
    pub interval_seconds: u64,
    pub timeout_seconds: u64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 10,
            timeout_seconds: 45,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BufferConfig {
    pub events: usize,
    pub bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            events: 1_024,
            bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRule {
    pub include: String,
    pub target: String,
    pub columns: ColumnSelection,
    #[serde(default)]
    pub initial_target_policy: InitialTargetPolicy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ColumnSelection {
    All(String),
    Mapping(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InitialTargetPolicy {
    #[default]
    RequireEmpty,
    Upsert,
    TruncateThenLoad,
}

impl MySqlCdcConfig {
    pub fn from_job(job: &JobConfig) -> Result<Self> {
        if job.source.source_type == "mysql_binlog" {
            bail!(
                "mysql_binlog configuration migration required: change source.type to mysql_cdc and use the versioned source.config structure"
            );
        }
        if job.source.source_type != "mysql_cdc" {
            bail!("source.type must be mysql_cdc");
        }
        let version = job
            .source
            .config_version
            .ok_or_else(|| anyhow::anyhow!("source.config_version is required for mysql_cdc"))?;
        if version != CURRENT_CONFIG_VERSION {
            bail!("unsupported source.config_version {version}; expected {CURRENT_CONFIG_VERSION}");
        }
        if !job.column_mapping.is_empty() {
            bail!(
                "top-level column_mapping is not valid for mysql_cdc; configure mappings in source.config.tables"
            );
        }
        let execution = job
            .execution
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("execution configuration is required for mysql_cdc"))?;
        if execution.delivery_guarantee != DeliveryGuarantee::AtLeastOnce {
            bail!("mysql_cdc currently supports only at_least_once delivery");
        }
        if !execution.checkpoint.enabled {
            bail!("execution.checkpoint.enabled must be true for mysql_cdc");
        }

        let encoded = serde_json::to_string(&job.source.config)
            .context("source.config could not be encoded")?;
        let mut deserializer = serde_json::Deserializer::from_str(&encoded);
        let config: Self =
            serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
                anyhow::anyhow!("source.config.{}: {}", error.path(), error.inner())
            })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.source_id.trim().is_empty() {
            bail!("source.config.source_id must not be empty");
        }
        if self.connection.host.trim().is_empty() {
            bail!("source.config.connection.host must not be empty");
        }
        if self.connection.database.trim().is_empty() {
            bail!("source.config.connection.database must not be empty");
        }
        if self.connection.username.trim().is_empty() {
            bail!("source.config.connection.username must not be empty");
        }
        relus_connector_rdbms::identifier::validate(&self.connection.database)
            .context("source.config.connection.database")?;
        if self.replication.server_id == 0 {
            bail!("source.config.replication.server_id must not be zero");
        }
        self.tls
            .validate(&self.connection.host)
            .context("source.config.tls")?;
        match self.startup.mode {
            StartupMode::Specific if self.startup.offset.is_none() => {
                bail!("source.config.startup.offset is required for specific mode")
            }
            StartupMode::Timestamp if self.startup.timestamp.is_none() => {
                bail!("source.config.startup.timestamp is required for timestamp mode")
            }
            StartupMode::Initial | StartupMode::Earliest | StartupMode::Latest
                if self.startup.offset.is_some() || self.startup.timestamp.is_some() =>
            {
                bail!("source.config.startup offset/timestamp does not match the selected mode")
            }
            _ => {}
        }
        if self.tables.is_empty() {
            bail!("source.config.tables must contain at least one table rule");
        }
        if self.checkpoint.id.trim().is_empty() {
            bail!("source.config.checkpoint.id must not be empty");
        }
        if self.checkpoint.path.as_os_str().is_empty() {
            bail!("source.config.checkpoint.path must not be empty");
        }
        if self.retry.initial_delay_ms == 0 {
            bail!("source.config.retry.initial_delay_ms must be greater than zero");
        }
        if self.retry.max_delay_ms < self.retry.initial_delay_ms {
            bail!(
                "source.config.retry.max_delay_ms must be greater than or equal to initial_delay_ms"
            );
        }
        if self.heartbeat.interval_seconds == 0 {
            bail!("source.config.heartbeat.interval_seconds must be greater than zero");
        }
        if self.heartbeat.timeout_seconds <= self.heartbeat.interval_seconds {
            bail!("source.config.heartbeat.timeout_seconds must be greater than interval_seconds");
        }
        for (index, table) in self.tables.iter().enumerate() {
            if table.include.trim().is_empty() || table.target.trim().is_empty() {
                bail!("source.config.tables include and target must not be empty");
            }
            validate_table_pattern(&table.include)
                .with_context(|| format!("source.config.tables[{index}].include"))?;
            relus_connector_rdbms::identifier::validate(&table.target)
                .with_context(|| format!("source.config.tables[{index}].target"))?;
            if matches!(&table.columns, ColumnSelection::All(value) if value != "*") {
                bail!("source.config.tables.columns string value must be `*`");
            }
            if table.initial_target_policy == InitialTargetPolicy::TruncateThenLoad {
                bail!(
                    "source.config.tables[{index}].initial_target_policy truncate_then_load is not supported by the current Sink capability model"
                );
            }
            if let ColumnSelection::Mapping(mapping) = &table.columns {
                if mapping.is_empty() {
                    bail!("source.config.tables[{index}].columns must not be empty");
                }
                let mut targets = BTreeSet::new();
                for (source, target) in mapping {
                    relus_connector_rdbms::identifier::validate(source).with_context(|| {
                        format!("source.config.tables[{index}].columns source `{source}`")
                    })?;
                    relus_connector_rdbms::identifier::validate(target).with_context(|| {
                        format!("source.config.tables[{index}].columns target `{target}`")
                    })?;
                    if !targets.insert(target) {
                        bail!(
                            "source.config.tables[{index}].columns maps multiple source columns to target `{target}`"
                        );
                    }
                }
            }
        }
        if self.buffer.events == 0 || self.buffer.bytes == 0 {
            bail!("source.config.buffer limits must be greater than zero");
        }
        Ok(())
    }
}

fn validate_table_pattern(pattern: &str) -> Result<()> {
    if pattern
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'*' | b'?'))
        && !pattern.starts_with('.')
        && !pattern.ends_with('.')
        && !pattern.contains("..")
    {
        Ok(())
    } else {
        bail!("table pattern may contain only identifiers, `.`, `*`, and `?`")
    }
}

fn default_mysql_port() -> u16 {
    3306
}
