//! `data_trans_reader` 提供从数据源读取数据的核心抽象与实现。
//!
//! # 核心特征
//!
//! ## [`Job`]
//! 数据读取任务的顶层 trait，所有数据源均需实现此接口：
//! - [`Job::split`] — 根据总记录数与并发度将数据源切分为多个 [`ReadTask`]，
//!   切分阶段**不持有**连接池，仅用于计算分片范围
//! - [`Job::execute_task`] — 执行单个分片任务，内部按需创建连接池并通过
//!   `mpsc::Sender` 将数据批量推送到下游管道
//! - [`Job::description`] — 返回数据源的描述信息，用于日志输出
//!
//! ## [`ReaderTask`]
//! 单个分片的底层读取接口，由具体数据源实现，供 [`Job::execute_task`] 调用。
//!
//! # 数据结构
//! - [`ReadTask`] — 描述一个分片任务（task_id、offset、limit、可选的自定义 SQL）
//! - [`JobSplitResult`] — [`Job::split`] 的返回值，包含总记录数与分片列表
//!
//! # 示例实现
//! `database_reader` 和 `api_reader` 是两个内置的参考实现，分别对应关系型数据库
//! 和 HTTP API 数据源，可作为自定义数据源实现的参考。

pub mod api_reader;
pub mod database_reader;
pub mod rdbms_reader_util;

use anyhow::Result;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

pub use rdbms_reader_util::rdbms_reader::{count_total_records, execute_query_stream, DbRowStream};
pub use rdbms_reader_util::rdbms_reader::{JsonRowMapper, RdbmsConfig, RdbmsJob, RowMapper};

#[derive(Debug, Clone)]
pub struct ReadTask {
    pub task_id: usize,
    pub conn: JsonValue,
    pub query_sql: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

pub struct JobSplitResult {
    pub total_records: usize,
    pub tasks: Vec<ReadTask>,
}

#[async_trait::async_trait]
pub trait Job<M>: Send + Sync
where
    M: Send + 'static,
{
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult>;
    async fn execute_task(&self, task: ReadTask, tx: mpsc::Sender<M>) -> Result<usize>;
    fn description(&self) -> String;
}

//
#[async_trait::async_trait]
pub trait ReaderTask<M>: Send + Sync
where
    M: Send + 'static,
{
    async fn read_data(&self, task: &ReadTask, tx: &mpsc::Sender<M>) -> Result<usize>;
}
