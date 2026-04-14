use anyhow::Result;
/// Database Reader - 数据库数据源的 Job 实现
use relus_common::interface::{ReadTask, ReaderJob, SplitReaderResult};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::rdbms_reader_util::rdbms_reader::{RdbmsConfig, RdbmsJob};
use relus_common::JobConfig;
use relus_common::PipelineMessage;
use relus_connector_rdbms::schema::SchemaDiscoveryConfig;

pub struct DatabaseJob {
    original_config: Arc<JobConfig>,
    discovery_config: SchemaDiscoveryConfig,
}

impl DatabaseJob {
    pub fn new(original_config: Arc<JobConfig>) -> Result<Self> {
        Ok(Self {
            original_config,
            discovery_config: SchemaDiscoveryConfig::default(),
        })
    }

    pub fn with_discovery_config(mut self, config: SchemaDiscoveryConfig) -> Self {
        self.discovery_config = config;
        self
    }

    fn build_rdbms_job(&self) -> RdbmsJob {
        let db_config = self.original_config.input.parse_database_config().unwrap();

        let input = &self.original_config.input;
        let split_pk = input.config_str("split_pk");
        let where_clause = input.config_str("where");
        let columns = input.config_str("columns").unwrap_or_else(|| {
            self.original_config
                .column_mapping
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        });
        let split_factor = input.config_u64("split_factor").map(|n| n as usize);

        let rdbms_config = RdbmsConfig {
            table: db_config.table,
            table_count: 1,
            is_table_mode: self.original_config.input.is_table_mode,
            query_sql: self.original_config.input.query_sql.clone(),
            column_mapping: self.original_config.column_mapping.clone(),
            column_types: self.original_config.column_types.clone(),
            split_pk,
            split_factor,
            where_clause,
            columns,
        };

        RdbmsJob::init(Arc::clone(&self.original_config), rdbms_config)
    }

    pub async fn discover(&self) -> Result<RdbmsJob> {
        let mut job = self.build_rdbms_job();

        if self.discovery_config.enabled {
            info!("Schema discovery enabled, discovering table schema...");
            job.discover().await?;
        }

        Ok(job)
    }
}

#[async_trait::async_trait]
impl ReaderJob for DatabaseJob {
    async fn split(&self, reader_threads: usize) -> Result<SplitReaderResult> {
        let rdbms_job = self.discover().await?;
        rdbms_job.split(reader_threads).await
    }

    async fn execute_task(
        &self,
        task: ReadTask,
        tx: mpsc::Sender<PipelineMessage>,
    ) -> Result<usize> {
        let rdbms_job = self.discover().await?;
        rdbms_job.execute_task(task, tx).await
    }

    fn description(&self) -> String {
        format!("{}", self.original_config.input.name)
    }
}
