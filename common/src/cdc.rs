use std::{collections::BTreeMap, fmt, sync::Arc};

use serde::{
    de::Deserializer,
    ser::{SerializeMap, Serializer},
    Deserialize, Serialize,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit};

pub const CDC_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SinkCapabilities {
    pub transactional_writes: bool,
    pub idempotent_snapshot: bool,
    pub idempotent_cdc: bool,
    pub supports_multi_table: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SinkRequirements {
    pub transactional_writes: bool,
    pub idempotent_snapshot: bool,
    pub idempotent_cdc: bool,
    pub supports_multi_table: bool,
}

impl SinkRequirements {
    pub fn validate(self, capabilities: SinkCapabilities) -> anyhow::Result<()> {
        if self.transactional_writes && !capabilities.transactional_writes {
            anyhow::bail!("source requires a Sink with transactional_writes capability");
        }
        if self.idempotent_snapshot && !capabilities.idempotent_snapshot {
            anyhow::bail!("source requires a Sink with idempotent_snapshot capability");
        }
        if self.idempotent_cdc && !capabilities.idempotent_cdc {
            anyhow::bail!("source requires a Sink with idempotent_cdc capability");
        }
        if self.supports_multi_table && !capabilities.supports_multi_table {
            anyhow::bail!("source requires a Sink with supports_multi_table capability");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq)]
pub enum CdcEventId {
    Text(String),
    Row { prefix: Arc<str>, row_index: u64 },
}

impl CdcEventId {
    pub fn row(prefix: Arc<str>, row_index: u64) -> Self {
        Self::Row { prefix, row_index }
    }
}

impl fmt::Display for CdcEventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(value) => formatter.write_str(value),
            Self::Row { prefix, row_index } => write!(formatter, "{prefix}{row_index}"),
        }
    }
}

impl PartialEq for CdcEventId {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl From<String> for CdcEventId {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<&str> for CdcEventId {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl Serialize for CdcEventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CdcEventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::Text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CdcOffset {
    Position {
        file: String,
        position: u64,
    },
    Gtid {
        resume_set: String,
        transaction_gtid: Option<String>,
        file: String,
        position: u64,
    },
}

impl CdcOffset {
    pub fn file(&self) -> &str {
        match self {
            Self::Position { file, .. } | Self::Gtid { file, .. } => file,
        }
    }

    pub fn position(&self) -> u64 {
        match self {
            Self::Position { position, .. } | Self::Gtid { position, .. } => *position,
        }
    }

    pub fn transaction_id(&self) -> String {
        match self {
            Self::Gtid {
                transaction_gtid: Some(gtid),
                ..
            } => gtid.clone(),
            Self::Gtid {
                resume_set,
                transaction_gtid: None,
                ..
            } => resume_set.clone(),
            Self::Position { file, position } => format!("{file}:{position}"),
        }
    }

    pub fn with_position(&self, file: impl Into<String>, position: u64) -> Self {
        match self {
            Self::Position { .. } => Self::Position {
                file: file.into(),
                position,
            },
            Self::Gtid {
                resume_set,
                transaction_gtid,
                ..
            } => Self::Gtid {
                resume_set: resume_set.clone(),
                transaction_gtid: transaction_gtid.clone(),
                file: file.into(),
                position,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcValue {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Date {
        year: u16,
        month: u8,
        day: u8,
    },
    DateTime {
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        micros: u32,
    },
    Time {
        negative: bool,
        days: u32,
        hours: u8,
        minutes: u8,
        seconds: u8,
        micros: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct CdcRow(Vec<(Arc<str>, CdcValue)>);

impl CdcRow {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_sorted_entries(entries: Vec<(Arc<str>, CdcValue)>) -> Self {
        debug_assert!(entries
            .windows(2)
            .all(|pair| pair[0].0.as_ref() <= pair[1].0.as_ref()));
        Self(entries)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn insert(&mut self, column: impl Into<Arc<str>>, value: CdcValue) -> Option<CdcValue> {
        let column = column.into();
        match self
            .0
            .binary_search_by(|(existing, _)| existing.as_ref().cmp(column.as_ref()))
        {
            Ok(index) => Some(std::mem::replace(&mut self.0[index].1, value)),
            Err(index) => {
                self.0.insert(index, (column, value));
                None
            }
        }
    }

    pub fn get(&self, column: &str) -> Option<&CdcValue> {
        self.0
            .binary_search_by(|(existing, _)| existing.as_ref().cmp(column))
            .ok()
            .map(|index| &self.0[index].1)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &CdcValue)> {
        self.0
            .iter()
            .map(|(column, value)| (column.as_ref(), value))
    }
}

impl<K, const N: usize> From<[(K, CdcValue); N]> for CdcRow
where
    K: Into<Arc<str>>,
{
    fn from(entries: [(K, CdcValue); N]) -> Self {
        let mut row = Self::new();
        for (column, value) in entries {
            row.insert(column, value);
        }
        row
    }
}

impl From<BTreeMap<String, CdcValue>> for CdcRow {
    fn from(entries: BTreeMap<String, CdcValue>) -> Self {
        Self::from_sorted_entries(
            entries
                .into_iter()
                .map(|(column, value)| (Arc::from(column), value))
                .collect(),
        )
    }
}

impl Serialize for CdcRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (column, value) in &self.0 {
            map.serialize_entry(column.as_ref(), value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for CdcRow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::<String, CdcValue>::deserialize(deserializer).map(Self::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcOperation {
    Snapshot,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange {
    pub database: Arc<str>,
    pub table: Arc<str>,
    pub operation: CdcOperation,
    pub key: CdcRow,
    pub before: Option<CdcRow>,
    pub after: Option<CdcRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcEnvelope {
    pub schema_version: u32,
    pub event_id: CdcEventId,
    pub transaction_id: Arc<str>,
    pub transaction_event_index: u64,
    pub row_index: u64,
    pub event_timestamp_seconds: u32,
    pub offset: Arc<CdcOffset>,
    pub change: RowChange,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcRowMessage {
    pub envelope: CdcEnvelope,
    pub target_table: String,
    #[serde(default)]
    pub column_mapping: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcSnapshotTargetPolicy {
    RequireEmpty,
    Upsert,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcSnapshotTable {
    pub source_database: String,
    pub source_table: String,
    pub target_table: String,
    pub policy: CdcSnapshotTargetPolicy,
}

pub struct BufferedCdcRow {
    pub message: CdcRowMessage,
    _byte_permit: Option<OwnedSemaphorePermit>,
}

impl fmt::Debug for BufferedCdcRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BufferedCdcRow")
            .field("message", &self.message)
            .field("bytes_bounded", &self._byte_permit.is_some())
            .finish()
    }
}

impl BufferedCdcRow {
    pub fn new(message: CdcRowMessage, byte_permit: OwnedSemaphorePermit) -> Self {
        Self {
            message,
            _byte_permit: Some(byte_permit),
        }
    }

    pub fn unbounded(message: CdcRowMessage) -> Self {
        Self {
            message,
            _byte_permit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCommit {
    pub schema_version: u32,
    pub event_id: String,
    pub transaction_id: Arc<str>,
    pub offset: Arc<CdcOffset>,
    pub event_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CdcMessage {
    Row(CdcEnvelope),
    Commit(TransactionCommit),
    TableMap {
        schema_version: u32,
        event_id: String,
        database: String,
        table: String,
        table_id: u64,
        offset: CdcOffset,
    },
    Rotate {
        schema_version: u32,
        event_id: String,
        file: String,
        position: u64,
        offset: CdcOffset,
    },
    Heartbeat {
        schema_version: u32,
        event_id: String,
        offset: CdcOffset,
    },
}

pub struct CdcCommitAcknowledger {
    commit: TransactionCommit,
    sender: oneshot::Sender<TransactionCommit>,
}

impl fmt::Debug for CdcCommitAcknowledger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CdcCommitAcknowledger")
            .field("event_id", &self.commit.event_id)
            .finish_non_exhaustive()
    }
}

impl CdcCommitAcknowledger {
    pub fn transaction_id(&self) -> &str {
        &self.commit.transaction_id
    }

    pub fn acknowledge(self) -> Result<(), TransactionCommit> {
        self.sender.send(self.commit)
    }
}

pub type CdcCommitAcknowledgement = oneshot::Receiver<TransactionCommit>;

pub fn cdc_commit_ack_channel(
    commit: TransactionCommit,
) -> (CdcCommitAcknowledger, CdcCommitAcknowledgement) {
    let (sender, receiver) = oneshot::channel();
    (CdcCommitAcknowledger { commit, sender }, receiver)
}
