//! RDBMS Writer 核心实现
//!
//! 提供关系型数据库写入功能，支持 PostgreSQL 和 MySQL

use std::sync::Arc;

use anyhow::{bail, Result};
use data_trans_common::pipeline::{DbBatch, MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;
use data_trans_common::JobConfig;
use data_trans_reader::rdbms_reader_util::util::{DbKind, DbPool, get_pool_from_config};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::rdbms_writer_util::util::writer_split_util;
use crate::types::{JobSplitResult, WriteMode, WriteTask};
use crate::{Job, WriterTaskExecutor};

/// RDBMS 写入配置
#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub key_columns: Vec<String>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

/// RDBMS Writer Job
pub struct RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub writer: Arc<dyn RowWriter<M> + Send + Sync>,
}

impl<M> RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    pub fn new(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        writer: Arc<dyn RowWriter<M> + Send + Sync>,
    ) -> Self {
        Self {
            original_config,
            config,
            writer,
        }
    }
}

/// Job 实现
#[async_trait::async_trait]
impl<M> Job<M> for RdbmsJob<M>
where
    M: Send + Sync + 'static,
{
    async fn split(&self, writer_threads: usize) -> Result<JobSplitResult> {
        let result = writer_split_util::do_split(&self.original_config, writer_threads);
        Ok(result)
    }

    async fn execute_task(&self, task: WriteTask, mut rx: mpsc::Receiver<M>) -> Result<usize> {
        info!("Writer-{} 启动", task.task_id);

        let pool = get_pool_from_config(&self.original_config).await?;
        let db_kind = match pool.as_ref() {
            DbPool::Postgres(_) => DbKind::Postgres,
            DbPool::Mysql(_) => DbKind::Mysql,
        };

        let mut written = 0;

        while let Some(msg) = rx.recv().await {
            match self.writer.process_message(&msg, &pool, &self.config, db_kind, &task).await {
                Ok(count) => {
                    written += count;
                    if count > 0 {
                        info!("Writer-{} 写入 {} 条数据（累计：{}）", task.task_id, count, written);
                    }
                }
                Err(e) => {
                    warn!("Writer-{} 写入失败: {}", task.task_id, e);
                }
            }
        }

        info!("Writer-{} 完成，共写入 {} 条数据", task.task_id, written);
        Ok(written)
    }

    fn description(&self) -> String {
        format!("RdbmsJob (table: {})", self.config.table)
    }
}

/// Row Writer trait - 处理消息并写入
#[async_trait::async_trait]
pub trait RowWriter<M>: Send + Sync {
    async fn process_message(
        &self,
        msg: &M,
        pool: &Arc<DbPool>,
        config: &RdbmsConfig,
        db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize>;
}

/// PipelineMessage 的 RowWriter 实现
pub struct PipelineRowWriter;

#[async_trait::async_trait]
impl RowWriter<PipelineMessage> for PipelineRowWriter {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<DbPool>,
        config: &RdbmsConfig,
        db_kind: DbKind,
        task: &WriteTask,
    ) -> Result<usize> {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                if rows.is_empty() {
                    return Ok(0);
                }

                let batch = prepare_db_batch(rows, &config.table, &config.key_columns, config.mode, db_kind)?;
                execute_db_write(&batch, pool, task.use_transaction, task.batch_size).await
            }
            PipelineMessage::ReaderFinished => {
                info!("Writer-{} 收到 Reader 完成信号", task.task_id);
                Ok(0)
            }
            PipelineMessage::Error(err) => {
                bail!("收到错误信号: {}", err)
            }
        }
    }
}

/// 准备数据库批次数据
pub fn prepare_db_batch(
    mapped_rows: &[MappedRow],
    table_name: &str,
    key_columns: &[String],
    mode: WriteMode,
    db_kind: DbKind,
) -> Result<DbBatch> {
    if mapped_rows.is_empty() {
        bail!("没有数据需要同步");
    }

    let columns: Vec<String> = mapped_rows[0].values.keys().cloned().collect();
    let (keys, nonkeys) = split_keys_nonkeys(&columns, key_columns);
    let sql = build_sql(table_name, &columns, &keys, &nonkeys, mode, db_kind)?;

    let mut rows = Vec::new();
    for mapped_row in mapped_rows {
        let mut values = Vec::new();
        for col in &columns {
            let val = mapped_row
                .values
                .get(col)
                .cloned()
                .unwrap_or(TypedVal::Text("".to_string()));
            values.push(val);
        }
        rows.push(values);
    }

    Ok(DbBatch { sql, rows, columns })
}

/// 分离主键列和非主键列
fn split_keys_nonkeys(all_cols: &[String], key_cols: &[String]) -> (Vec<String>, Vec<String>) {
    let mut keys = Vec::new();
    let mut nonkeys = Vec::new();

    for col in all_cols {
        if key_cols.contains(col) {
            keys.push(col.clone());
        } else {
            nonkeys.push(col.clone());
        }
    }

    (keys, nonkeys)
}

/// 构建 SQL 语句
pub fn build_sql(
    table: &str,
    columns: &[String],
    keys: &[String],
    nonkeys: &[String],
    mode: WriteMode,
    kind: DbKind,
) -> Result<String> {
    let mut sql = String::new();

    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str(" (");
    sql.push_str(&columns.join(", "));
    sql.push_str(") VALUES (");
    sql.push_str(&build_placeholders(kind, columns.len(), 1));
    sql.push(')');

    if mode == WriteMode::Upsert {
        match kind {
            DbKind::Postgres => {
                if !keys.is_empty() {
                    sql.push_str(" ON CONFLICT (");
                    sql.push_str(&keys.join(", "));
                    sql.push_str(") DO UPDATE SET ");
                    let sets: Vec<String> = nonkeys
                        .iter()
                        .map(|c| format!("{} = EXCLUDED.{}", c, c))
                        .collect();
                    sql.push_str(&sets.join(", "));
                } else {
                    bail!("upsert 模式需要指定 key_columns");
                }
            }
            DbKind::Mysql => {
                sql.push_str(" ON DUPLICATE KEY UPDATE ");
                let sets: Vec<String> = nonkeys
                    .iter()
                    .map(|c| format!("{} = VALUES({})", c, c))
                    .collect();
                sql.push_str(&sets.join(", "));
            }
        }
    }

    Ok(sql)
}

/// 构建占位符
fn build_placeholders(kind: DbKind, count: usize, _offset: usize) -> String {
    match kind {
        DbKind::Postgres => (1..=count)
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(", "),
        DbKind::Mysql => vec!["?"; count].join(", "),
    }
}

/// 执行数据库写入
pub async fn execute_db_write(
    batch: &DbBatch,
    pool: &Arc<DbPool>,
    use_transaction: bool,
    batch_size: usize,
) -> Result<usize> {
    let total_rows = batch.rows.len();
    let mut processed = 0;

    match pool.as_ref() {
        DbPool::Postgres(pg_pool) => {
            let mut start = 0;
            while start < total_rows {
                let end = (start + batch_size).min(total_rows);

                if use_transaction {
                    let mut tx = pg_pool.begin().await?;
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_pg(&batch.sql, row_values, &mut *tx).await?;
                    }
                    tx.commit().await?;
                } else {
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_pg(&batch.sql, row_values, pg_pool).await?;
                    }
                }

                processed += end - start;
                start = end;
            }
        }
        DbPool::Mysql(my_pool) => {
            let mut start = 0;
            while start < total_rows {
                let end = (start + batch_size).min(total_rows);

                if use_transaction {
                    let mut tx = my_pool.begin().await?;
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_my(&batch.sql, row_values, &mut *tx).await?;
                    }
                    tx.commit().await?;
                } else {
                    for row_values in &batch.rows[start..end] {
                        execute_single_row_my(&batch.sql, row_values, my_pool).await?;
                    }
                }

                processed += end - start;
                start = end;
            }
        }
    }

    Ok(processed)
}

/// 执行单行 SQL（PostgreSQL）
async fn execute_single_row_pg(
    sql: &str,
    values: &[TypedVal],
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
) -> Result<()> {
    let mut q = sqlx::query(sql);

    for v in values {
        match v {
            TypedVal::I64(n) => q = q.bind(n),
            TypedVal::F64(n) => q = q.bind(n),
            TypedVal::Bool(b) => q = q.bind(b),
            TypedVal::OptI64(n) => q = q.bind(n),
            TypedVal::OptF64(n) => q = q.bind(n),
            TypedVal::OptBool(n) => q = q.bind(n),
            TypedVal::OptNaiveTs(n) => q = q.bind(n),
            TypedVal::Text(s) => q = q.bind(s.clone()),
        }
    }

    q.execute(executor).await?;
    Ok(())
}

/// 执行单行 SQL（MySQL）
async fn execute_single_row_my(
    sql: &str,
    values: &[TypedVal],
    executor: impl sqlx::Executor<'_, Database = sqlx::MySql>,
) -> Result<()> {
    let mut q = sqlx::query(sql);

    for v in values {
        match v {
            TypedVal::I64(n) => q = q.bind(n),
            TypedVal::F64(n) => q = q.bind(n),
            TypedVal::Bool(b) => q = q.bind(b),
            TypedVal::OptI64(n) => q = q.bind(n),
            TypedVal::OptF64(n) => q = q.bind(n),
            TypedVal::OptBool(n) => q = q.bind(n),
            TypedVal::OptNaiveTs(n) => q = q.bind(n),
            TypedVal::Text(s) => q = q.bind(s.clone()),
        }
    }

    q.execute(executor).await?;
    Ok(())
}

/// WriterTaskExecutor 实现
pub struct RdbmsWriterExecutor {
    pool: Arc<DbPool>,
    config: RdbmsConfig,
    db_kind: DbKind,
    use_transaction: bool,
    batch_size: usize,
}

impl RdbmsWriterExecutor {
    pub fn new(pool: Arc<DbPool>, config: RdbmsConfig, use_transaction: bool, batch_size: usize) -> Self {
        let db_kind = match pool.as_ref() {
            DbPool::Postgres(_) => DbKind::Postgres,
            DbPool::Mysql(_) => DbKind::Mysql,
        };
        Self {
            pool,
            config,
            db_kind,
            use_transaction,
            batch_size,
        }
    }
}

#[async_trait::async_trait]
impl WriterTaskExecutor for RdbmsWriterExecutor {
    async fn write_batch(&self, rows: &[MappedRow]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let batch = prepare_db_batch(rows, &self.config.table, &self.config.key_columns, self.config.mode, self.db_kind)?;
        execute_db_write(&batch, &self.pool, self.use_transaction, self.batch_size).await
    }
}
