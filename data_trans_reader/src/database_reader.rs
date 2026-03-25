/// Database Reader - 数据库数据源的 Job 实现
use crate::{Job, JobSplitResult, ReadTask};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::rdbms_reader_util::rdbms_reader::{RdbmsConfig, RdbmsJob};
use crate::rdbms_reader_util::util::pipeline_mapper::PipelineRowMapper;
use data_trans_common::JobConfig;
use data_trans_common::PipelineMessage;

pub struct DatabaseJob {
    original_config: Arc<JobConfig>,
}

impl DatabaseJob {
    pub fn new(original_config: Arc<JobConfig>) -> Result<Self> {
        Ok(Self { original_config })
    }

    fn build_rdbms_job(&self) -> Result<RdbmsJob<PipelineMessage>> {
        let db_config = JobConfig::parse_database_config(&self.original_config.input)?;

        let input_config = &self.original_config.input.config;
        let split_pk = input_config.get("split_pk").and_then(|v| v.as_str()).map(|s| s.to_string());
        let where_clause = input_config.get("where").and_then(|v| v.as_str()).map(|s| s.to_string());
        let columns = input_config.get("columns").and_then(|v| v.as_str()).unwrap_or("*").to_string();

        let rdbms_config = RdbmsConfig {
            table: db_config.table,
            table_count: 1,
            is_table_mode: self.original_config.input.is_table_mode,
            query_sql: self.original_config.input.query_sql.clone(),
            column_mapping: self.original_config.column_mapping.clone(),
            column_types: self.original_config.column_types.clone(),
            split_pk,
            split_factor: 5,
            where_clause,
            columns,
        };

        let mapper = Arc::new(PipelineRowMapper::new(
            self.original_config.column_mapping.clone(),
            self.original_config.column_types.clone(),
        ));

        Ok(RdbmsJob::init(
            Arc::clone(&self.original_config),
            rdbms_config,
            mapper,
        ))
    }
}

#[async_trait::async_trait]
impl Job<PipelineMessage> for DatabaseJob {
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult> {
        let rdbms_job = self.build_rdbms_job()?;
        rdbms_job.split(reader_threads).await
    }

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        let rdbms_job = self.build_rdbms_job()?;
        rdbms_job.execute_task(task, tx).await
    }

    fn description(&self) -> String {
        format!("DatabaseJob (source: {})", self.original_config.input.name)
    }
}
