use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const CDC_SCHEMA_VERSION: u32 = 1;

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
#[serde(untagged)]
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

pub type CdcRow = BTreeMap<String, CdcValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowChange {
    pub database: String,
    pub table: String,
    pub operation: CdcOperation,
    pub key: CdcRow,
    pub before: Option<CdcRow>,
    pub after: Option<CdcRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CdcEnvelope {
    pub schema_version: u32,
    pub event_id: String,
    pub transaction_id: String,
    pub transaction_event_index: u64,
    pub row_index: u64,
    pub event_timestamp_seconds: u32,
    pub offset: CdcOffset,
    pub change: RowChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionCommit {
    pub schema_version: u32,
    pub event_id: String,
    pub transaction_id: String,
    pub offset: CdcOffset,
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
