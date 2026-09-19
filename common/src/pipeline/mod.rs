//! Pipeline 消息类型定义
//!
//! PipelineMessage 和 DbBatch 是 Reader → Writer 的数据通道类型，
//! 属于公共接口层，被 core、writer 共同使用。

use crate::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_CHANNEL_NUMBER, DEFAULT_PER_GROUP_CHANNEL,
    DEFAULT_READER_THREADS,
};

mod message;

pub use message::*;

/// Runtime pipeline execution settings shared by planners and executors.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（决定 Task 数量）
    pub reader_threads: usize,
    /// Channel 缓冲区大小
    pub buffer_size: usize,
    /// 全局并发 channel 数
    pub channel_number: usize,
    /// 每个 TaskGroup 内并发 channel 数
    pub per_group_channel: usize,
    /// 批处理大小
    pub batch_size: usize,
    /// 是否使用事务
    pub use_transaction: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            reader_threads: DEFAULT_READER_THREADS,
            buffer_size: DEFAULT_BUFFER_SIZE,
            channel_number: DEFAULT_CHANNEL_NUMBER,
            per_group_channel: DEFAULT_PER_GROUP_CHANNEL,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
        }
    }
}

impl PipelineConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.reader_threads > 0,
            "reader_threads must be greater than zero"
        );
        anyhow::ensure!(
            self.buffer_size > 0,
            "buffer_size must be greater than zero"
        );
        anyhow::ensure!(
            self.channel_number > 0,
            "channel_number must be greater than zero"
        );
        anyhow::ensure!(
            self.per_group_channel > 0,
            "per_group_channel must be greater than zero"
        );
        anyhow::ensure!(self.batch_size > 0, "batch_size must be greater than zero");
        Ok(())
    }
}
