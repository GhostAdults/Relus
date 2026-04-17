//! Pipeline 消息与批次类型

use crate::types::{MappingRow, UnifiedValue};

/// Reader 到 Writer 的消息
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    DataBatch(Vec<MappingRow>),
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
