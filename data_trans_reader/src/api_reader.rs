use crate::rdbms_reader_util::util;
use crate::{Job, JobSplitResult, ReadTask};
/// API Reader - HTTP API 数据源的 Job 实现
use anyhow::Result;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc;

use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::JobConfig;
use serde_json::Value as JsonValue;

/// API Job 实现
pub struct ApiJob {
    config: Arc<JobConfig>,
}

impl ApiJob {
    pub fn new(config: Arc<JobConfig>) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl Job<PipelineMessage> for ApiJob {
    async fn split(&self, _reader_threads: usize) -> Result<JobSplitResult> {
        Ok(JobSplitResult {
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

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        println!("Reader-{} 获取 (API 数据源)", task.task_id);
        let items = util::client_tool::fetch_from_api(&self.config).await?;
        println!("Reader-{} 获取了 {} 条数据", task.task_id, items.len());

        let mut sent = 0;
        for chunk in items.chunks(100) {
            let mapped = apply_mapping(
                chunk,
                &self.config.column_mapping,
                &self.config.column_types,
            )?;
            tx.send(PipelineMessage::DataBatch(mapped)).await?;
            sent += chunk.len();
        }

        println!("Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("ApiJob (source: {})", self.config.input.name)
    }
}

fn apply_mapping(
    items: &[JsonValue],
    column_mapping: &BTreeMap<String, String>,
    column_types: &Option<BTreeMap<String, String>>,
) -> Result<Vec<MappedRow>> {
    let mut mapped_rows = Vec::new();

    for item in items {
        let mut row = HashMap::new();

        for (target_col, source_path) in column_mapping {
            let source_val =
                util::client_tool::extract_by_path(item, source_path).unwrap_or(&JsonValue::Null);

            let type_hint = column_types
                .as_ref()
                .and_then(|m| m.get(target_col))
                .map(|s| s.as_str());

            let typed_val = util::pipeline_mapper::to_typed_value_origin(source_val, type_hint)?;
            row.insert(target_col.clone(), typed_val);
        }

        mapped_rows.push(MappedRow {
            values: row,
            source: item.clone(),
        });
    }

    Ok(mapped_rows)
}
