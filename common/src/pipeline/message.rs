//! Pipeline 消息与批次类型

use crate::{
    cdc::{BufferedCdcRow, CdcCommitAcknowledger, CdcSnapshotTable},
    types::{MappingRow, UnifiedValue},
};

/// Reader 到 Writer 的消息
#[derive(Debug)]
pub enum PipelineMessage {
    DataBatch(Vec<MappingRow>),
    CdcRow(BufferedCdcRow),
    CdcSnapshotTable(CdcSnapshotTable),
    CdcCommit(CdcCommitAcknowledger),
    ReaderFinished,
    Error(String),
}

/// 数据库批次（写入用）
#[derive(Debug)]
pub struct DbBatch {
    pub base_sql: String,
    pub table_name: String,
    pub rows: Vec<Vec<UnifiedValue>>,
    pub columns: Vec<String>,
}
