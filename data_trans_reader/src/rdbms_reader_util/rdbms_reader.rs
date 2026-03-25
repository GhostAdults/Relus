use crate::rdbms_reader_util::util::*;
use anyhow::Result;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::{Job, JobSplitResult, ReadTask, ReaderTask};
use data_trans_common::JobConfig;

#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub table_count: usize,
    pub is_table_mode: bool,
    pub query_sql: Option<Vec<String>>,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub split_pk: Option<String>,
    pub split_factor: usize,
    pub where_clause: Option<String>,
    pub columns: String,
}

pub struct RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub mapper: Arc<dyn RowMapper<M> + Send + Sync>,
}

impl<M> RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub fn init(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        mapper: Arc<dyn RowMapper<M> + Send + Sync>,
    ) -> Self {
        Self {
            original_config,
            config,
            mapper,
        }
    }

    // 发送数据批次到管道
    async fn send_batch(&self, rows: &[JsonValue], tx: &mpsc::Sender<M>) -> Result<usize> {
        let messages = self.mapper.map_rows(rows)?;
        let count = messages.len();

        for msg in messages {
            tx.send(msg)
                .await
                .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
        }

        Ok(count)
    }
}

/// Job
#[async_trait::async_trait]
impl<M> Job<M> for RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult> {
        let result = reader_split_util::do_split(&self, reader_threads).await;
        Ok(result)
    }

    async fn execute_task(&self, task: ReadTask, tx: mpsc::Sender<M>) -> Result<usize> {
        info!(
            "Reader-{} 启动 (OFFSET={}, LIMIT={})",
            task.task_id, task.offset, task.limit
        );

        let sent: usize = self.read_data(&task, &tx).await?;

        info!("Reader-{} 完成，共发送 {} 条数据", task.task_id, sent);
        Ok(sent)
    }

    fn description(&self) -> String {
        format!("RdbmsJob (table: {})", self.config.table)
    }
}

/// Task
#[async_trait::async_trait]
impl<M> ReaderTask<M> for RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    async fn read_data(&self, slice_task: &ReadTask, tx: &mpsc::Sender<M>) -> Result<usize> {
        let pool = get_pool_from_config(&self.original_config).await?;
        let query_str = self
            .config
            .query_sql
            .as_ref()
            .and_then(|v| v.first())
            .map(|s| s.as_str());
        // 创建sql
        let sql = build_query_sql(
            self.config.table.as_str(),
            query_str,
            slice_task.limit,
            slice_task.offset,
        );
        let row_stream: DbRowStream<'_> = execute_query_stream(pool.as_ref(), &sql)?;

        let mut sent = 0;
        let mut buffer = Vec::with_capacity(100);

        let mut stream = row_stream.into_inner();

        while let Some(row_result) = stream.next().await {
            let json_row = row_result?;
            buffer.push(json_row);

            if buffer.len() >= 100 {
                sent += self.send_batch(&buffer, tx).await?;
                buffer.clear();

                if sent % 1000 == 0 {
                    info!("Reader-{} 已发送 {} 条", slice_task.task_id, sent);
                }
            }
        }

        if !buffer.is_empty() {
            sent += self.send_batch(&buffer, tx).await?;
        }

        Ok(sent)
    }
}

pub trait RowMapper<M>: Send + Sync {
    fn map_rows(&self, rows: &[JsonValue]) -> Result<Vec<M>>;
}

pub struct JsonRowMapper;

impl RowMapper<JsonValue> for JsonRowMapper {
    fn map_rows(&self, rows: &[JsonValue]) -> Result<Vec<JsonValue>> {
        Ok(rows.to_vec())
    }
}

// 获取当前表数据count
pub async fn count_total_records(pool: &DbPool, table: &str, query: Option<&str>) -> Result<usize> {
    let sql = if let Some(custom_query) = query {
        format!("SELECT COUNT(*) FROM ({}) AS subquery", custom_query)
    } else {
        format!("SELECT COUNT(*) FROM {}", table)
    };

    let count: i64 = match pool {
        DbPool::Postgres(pg_pool) => sqlx::query_scalar(&sql).fetch_one(pg_pool).await?,
        DbPool::Mysql(my_pool) => sqlx::query_scalar(&sql).fetch_one(my_pool).await?,
    };

    Ok(count as usize)
}

type JsonStream<'a> =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<JsonValue, sqlx::Error>> + Send + 'a>>;

pub struct DbRowStream<'a> {
    stream: JsonStream<'a>,
}

impl<'a> DbRowStream<'a> {
    pub fn into_inner(self) -> JsonStream<'a> {
        self.stream
    }
}

pub fn execute_query_stream<'a>(pool: &'a DbPool, sql: &'a str) -> Result<DbRowStream<'a>> {
    use futures::StreamExt;
    use sqlx::{Column, Row};

    let stream: JsonStream<'a> = match pool {
        DbPool::Postgres(pg_pool) => Box::pin(sqlx::query(sql).fetch(pg_pool).map(|result| {
            result.map(|row: sqlx::postgres::PgRow| {
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let col_name = col.name();
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col_name.to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                JsonValue::Object(obj)
            })
        })),
        DbPool::Mysql(my_pool) => Box::pin(sqlx::query(sql).fetch(my_pool).map(|result| {
            result.map(|row: sqlx::mysql::MySqlRow| {
                let mut obj = serde_json::Map::new();
                for (idx, col) in row.columns().iter().enumerate() {
                    let col_name = col.name();
                    let val: Option<String> = row.try_get(idx).ok();
                    obj.insert(
                        col_name.to_string(),
                        val.map(JsonValue::String).unwrap_or(JsonValue::Null),
                    );
                }
                JsonValue::Object(obj)
            })
        })),
    };

    Ok(DbRowStream { stream })
}
