//! Pipeline 模块 (core)
//!
//! 数据转换与编排核心：
//! - RecordBuilder: 统一数据转换入口
//! - PipelineMessage / DbBatch: 从 common re-export

mod record;
mod record_builder;

pub use record::*;
pub use record_builder::*;

// Re-export 消息类型（定义在 common）
pub use relus_common::pipeline::{DbBatch, PipelineMessage};
// Re-export MappingRow 相关类型
pub use relus_common::types::{BatchMetadata, MappingBatch, MappingRow};
