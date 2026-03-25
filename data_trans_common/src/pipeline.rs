use std::collections::HashMap;
use crate::types::DataSourceType;
use crate::types::TypedVal;
use serde_json::Value as JsonValue;

// ==========================================
// 配置
// ==========================================

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（分片读取任务数）
    pub reader_threads: usize,

    /// Writer 线程数（用于并发写入）
    pub writer_threads: usize,

    /// Channel 缓冲区大小
    pub channel_buffer_size: usize,

    /// 每个批次的大小
    pub batch_size: usize,

    /// 是否使用事务
    pub use_transaction: bool,

    /// 数据源类型
    pub data_source: DataSourceType,
}


// ==========================================
// 数据流中的消息类型
// ==========================================

/// eReadr 到 Writer 的消息
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    /// 数据批次
    DataBatch(Vec<MappedRow>),

    /// Reader 完成信号
    ReaderFinished,

    /// 错误信号
    Error(String),
}

// ==========================================
// 数据结构
// ==========================================

/// 映射后的数据行
#[derive(Debug, Clone)]
pub struct MappedRow {
    pub values: HashMap<String, TypedVal>,
    pub source: JsonValue,
}

/// 数据库批次
#[derive(Debug)]
pub struct DbBatch {
    pub sql: String,
    pub rows: Vec<Vec<TypedVal>>,
    pub columns: Vec<String>,
}
