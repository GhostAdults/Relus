//! Pipeline 消息类型定义
//!
//! PipelineMessage 和 DbBatch 是 Reader → Writer 的数据通道类型，
//! 属于公共接口层，被 core、writer 共同使用。

mod message;

pub use message::*;
