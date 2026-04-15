//! API Reader - HTTP API 数据源的 Reader 实现
//!
//! `ApiReader` 持有 `ApiJob`，`ReaderJob` trait 实现在 Reader 上。
//! `ApiJob` 负责业务逻辑（配置解析、RecordBuilder 构建），
//! `ApiReader` 负责生命周期管理和 pipeline 对接。

use anyhow::Result;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use relus_common::pipeline::{PipelineMessage, RecordBuilder};
use relus_common::types::SourceType;
use relus_common::JobConfig;

use crate::rdbms_reader_util::util;
use relus_common::interface::{ReadTask, ReaderJob, ReaderTask, SplitReaderResult};

pub struct ApiReader {
    job: ApiJob,
}

/// API 数据源的 Job 业务逻辑
pub struct ApiJob {
    config: Arc<JobConfig>,
    builder: RecordBuilder,
}

impl ApiJob {
    pub fn new(config: Arc<JobConfig>) -> Self {
        let source_type = SourceType::from_str(&config.input.source_type);
        let builder =
            RecordBuilder::new(config.column_mapping.clone(), config.column_types.clone())
                .with_source_type(source_type);
        Self { config, builder }
    }
}

impl ApiReader {
    pub fn init(config: Arc<JobConfig>) -> Result<Self> {
        let job = ApiJob::new(config);
        Ok(Self { job })
    }
}

#[async_trait::async_trait]
impl ReaderJob for ApiReader {
    async fn split(&self, _reader_threads: usize) -> Result<SplitReaderResult> {
        Ok(SplitReaderResult {
            total_records: 0,
            tasks: vec![ReadTask {
                task_id: 0,
                conn: JsonValue::Null,
                query_sql: None,
                offset: 0,
                limit: 0,
            }],
        })
    }
    fn description(&self) -> String {
        format!("ApiReader (source: {})", self.job.config.input.name)
    }
}

#[async_trait::async_trait]
impl ReaderTask for ApiReader {
    async fn read_data(
        &self,
        task: &ReadTask,
        tx: &mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        // to-do reader 应该在这里执行execute

        info!("Reader-{} 获取 (API 数据源)", task.task_id);
        let items = util::client_tool::fetch_from_api(&self.job.config).await?;
        info!("Reader-{} 获取了 {} 条数据", task.task_id, items.len());

        let mut sent = 0;
        for chunk in items.chunks(100) {
            let message = self.job.builder.build_message(chunk)?;
            tx.send(message).await?;
            sent += chunk.len();
        }

        info!("Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }
}
