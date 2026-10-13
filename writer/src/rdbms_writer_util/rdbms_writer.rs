//! RDBMS Writer 核心实现
//!
//! Writer 只负责：接收 PipelineMessage → 提取数据 → 调用 connector-rdbms 的 SQL builder → 执行写入

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::{bail, Result};
use relus_connector_rdbms::pool::RdbmsPool;
use relus_connector_rdbms::sql_builder::{execute_rdbms_write, validate_upsert_keys, WriteRows};
use relus_connector_rdbms::util::get_pool_from_output;

use relus_common::pipeline::PipelineMessage;
use relus_common::types::UnifiedValue;
use relus_common::JobConfig;
use relus_common::MappingRow;
use tokio::sync::mpsc;
use tracing::info;

use crate::rdbms_writer_util::util::writer_split_util;
use relus_common::cdc::{
    CdcOperation, CdcRowMessage, CdcSnapshotTable, CdcSnapshotTargetPolicy, CdcValue,
};
use relus_common::job_config::WriteMode;

use crate::{DataWriterJob, DataWriterTask, SinkCapabilities, SplitWriterResult, WriteTask};

/// RDBMS 写入配置
#[derive(Debug, Clone)]
pub struct RdbmsConfig {
    pub table: String,
    pub key_columns: Vec<String>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

pub struct CdcWritePlan {
    pub table: String,
    pub mode: WriteMode,
    pub key_columns: Vec<String>,
    pub rows: WriteRows,
}

pub fn plan_cdc_write(message: &CdcRowMessage) -> Result<Vec<CdcWritePlan>> {
    let change = &message.envelope.change;
    if change.operation == CdcOperation::Update {
        let before = change
            .before
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CDC update event is missing its before image"))?;
        let after = change
            .after
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CDC update event is missing its after image"))?;
        let stable_key_changed = change
            .key
            .iter()
            .any(|(column, old_value)| after.get(column) != Some(old_value));
        if stable_key_changed {
            return Ok(vec![
                plan_cdc_row(message, before, WriteMode::Delete)?,
                plan_cdc_row(message, after, WriteMode::Upsert)?,
            ]);
        }
    }
    let row = match change.operation {
        CdcOperation::Snapshot | CdcOperation::Insert | CdcOperation::Update => {
            change.after.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "CDC {:?} event is missing its after image",
                    change.operation
                )
            })?
        }
        CdcOperation::Delete => change
            .before
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("CDC delete event is missing its before image"))?,
    };
    let mode = match change.operation {
        CdcOperation::Snapshot | CdcOperation::Insert => WriteMode::Upsert,
        CdcOperation::Update => WriteMode::Update,
        CdcOperation::Delete => WriteMode::Delete,
    };
    Ok(vec![plan_cdc_row(message, row, mode)?])
}

fn plan_cdc_row(
    message: &CdcRowMessage,
    row: &relus_common::cdc::CdcRow,
    mode: WriteMode,
) -> Result<CdcWritePlan> {
    let change = &message.envelope.change;
    let mut entries = row
        .iter()
        .filter_map(|(source, value)| {
            if message.column_mapping.is_empty() {
                Some((source.to_owned(), cdc_value_to_unified(value)))
            } else {
                message
                    .column_mapping
                    .get(source)
                    .cloned()
                    .map(|target| (target, cdc_value_to_unified(value)))
            }
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let key_columns = change
        .key
        .iter()
        .map(|(source, _)| {
            if message.column_mapping.is_empty() {
                Ok(source.to_owned())
            } else {
                message.column_mapping.get(source).cloned().ok_or_else(|| {
                    anyhow::anyhow!("CDC column mapping must include stable-key column `{source}`")
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if key_columns.is_empty() {
        bail!("CDC writes require a primary key for replay-safe delivery");
    }
    let (columns, values): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
    Ok(CdcWritePlan {
        table: message.target_table.clone(),
        mode,
        key_columns,
        rows: WriteRows::new(columns, vec![values]),
    })
}

fn cdc_value_to_unified(value: &CdcValue) -> UnifiedValue {
    match value {
        CdcValue::Null => UnifiedValue::Null,
        CdcValue::Bool(value) => UnifiedValue::Bool(*value),
        CdcValue::Signed(value) => UnifiedValue::Int(*value),
        CdcValue::Unsigned(value) => match i64::try_from(*value) {
            Ok(value) => UnifiedValue::Int(value),
            Err(_) => UnifiedValue::Decimal(rust_decimal::Decimal::from(*value)),
        },
        CdcValue::Float(value) => UnifiedValue::Float(*value),
        CdcValue::String(value) => UnifiedValue::String(value.clone()),
        CdcValue::Bytes(value) => UnifiedValue::Bytes(value.clone()),
        CdcValue::Date { year, month, day } => {
            chrono::NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))
                .map(UnifiedValue::Date)
                .unwrap_or_else(|| UnifiedValue::String(format!("{year:04}-{month:02}-{day:02}")))
        }
        CdcValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        } => chrono::NaiveDate::from_ymd_opt(i32::from(*year), u32::from(*month), u32::from(*day))
            .and_then(|date| {
                date.and_hms_micro_opt(
                    u32::from(*hour),
                    u32::from(*minute),
                    u32::from(*second),
                    *micros,
                )
            })
            .map(UnifiedValue::DateTime)
            .unwrap_or_else(|| {
                UnifiedValue::String(format!(
                    "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}"
                ))
            }),
        CdcValue::Time {
            negative,
            days,
            hours,
            minutes,
            seconds,
            micros,
        } => {
            let total_hours = u64::from(*days) * 24 + u64::from(*hours);
            if !negative && total_hours < 24 {
                chrono::NaiveTime::from_hms_micro_opt(
                    total_hours as u32,
                    u32::from(*minutes),
                    u32::from(*seconds),
                    *micros,
                )
                .map(UnifiedValue::Time)
                .unwrap_or_else(|| UnifiedValue::String(format_cdc_time(value)))
            } else {
                UnifiedValue::String(format_cdc_time(value))
            }
        }
    }
}

fn format_cdc_time(value: &CdcValue) -> String {
    let CdcValue::Time {
        negative,
        days,
        hours,
        minutes,
        seconds,
        micros,
    } = value
    else {
        return String::new();
    };
    let sign = if *negative { "-" } else { "" };
    let total_hours = u64::from(*days) * 24 + u64::from(*hours);
    format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
}

/// RDBMS Writer
pub struct RdbmsWriter {
    job: RdbmsJob,
}

impl RdbmsWriter {
    pub fn init(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        writer: Arc<dyn RowWriter + Send + Sync>,
    ) -> Result<Self> {
        let job = RdbmsJob::new(original_config, config, writer);
        Ok(Self { job })
    }

    pub fn from_job(job: RdbmsJob) -> Self {
        Self { job }
    }
}

#[async_trait::async_trait]
impl DataWriterJob for RdbmsWriter {
    async fn split(&self, writer_threads: usize) -> Result<SplitWriterResult> {
        let result = writer_split_util::do_split(&self.job.original_config, writer_threads);
        Ok(result)
    }

    fn description(&self) -> String {
        format!("RdbmsWriter (table: {})", self.job.config.table)
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            transactional_writes: false,
            idempotent_snapshot: true,
            idempotent_cdc: true,
            supports_multi_table: true,
        }
    }
}

/// RDBMS Writer Job 业务逻辑
pub struct RdbmsJob {
    pub original_config: Arc<JobConfig>,
    pub config: RdbmsConfig,
    pub writer: Arc<dyn RowWriter>,
}

impl RdbmsJob {
    pub fn new(
        original_config: Arc<JobConfig>,
        config: RdbmsConfig,
        writer: Arc<dyn RowWriter>,
    ) -> Self {
        Self {
            original_config,
            config,
            writer,
        }
    }
}

/// Row Writer trait - 处理消息并写入
#[async_trait::async_trait]
pub trait RowWriter: Send + Sync {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<RdbmsPool>,
        config: &RdbmsConfig,
        task: &WriteTask,
    ) -> Result<usize>;
}

/// 将 pipeline 映射后的行批次转换为 RDBMS 写入行模型。
fn mapping_rows_to_write_rows(mapped_rows: &[MappingRow]) -> WriteRows {
    let columns: Vec<String> = mapped_rows[0].field_names().cloned().collect();
    let mut rows = Vec::with_capacity(mapped_rows.len());
    for mapped_row in mapped_rows {
        let mut values = Vec::with_capacity(columns.len());
        for col in &columns {
            let val = mapped_row
                .get_value(col)
                .cloned()
                .unwrap_or(UnifiedValue::String(String::new()));
            values.push(val);
        }
        rows.push(values);
    }
    WriteRows::new(columns, rows)
}

/// PipelineMessage 的 RowWriter 实现
pub struct PipelineRowWriter;

#[async_trait::async_trait]
impl RowWriter for PipelineRowWriter {
    async fn process_message(
        &self,
        msg: &PipelineMessage,
        pool: &Arc<RdbmsPool>,
        config: &RdbmsConfig,
        task: &WriteTask,
    ) -> Result<usize> {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                if rows.is_empty() {
                    return Ok(0);
                }
                let write_rows = mapping_rows_to_write_rows(rows);
                execute_rdbms_write(
                    pool,
                    &config.table,
                    config.mode,
                    &config.key_columns,
                    write_rows,
                    task.batch_size,
                )
                .await
            }
            PipelineMessage::CdcRow(_)
            | PipelineMessage::CdcSnapshotTable(_)
            | PipelineMessage::CdcCommit(_) => {
                bail!("CDC messages require a CDC-capable sink configuration")
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

#[async_trait::async_trait]
impl DataWriterTask for RdbmsWriter {
    async fn write_data(
        &self,
        task: WriteTask,
        mut rx: mpsc::Receiver<PipelineMessage>,
    ) -> Result<usize> {
        let pool = get_pool_from_output(&self.job.original_config).await?;

        if self.job.config.mode == WriteMode::Upsert {
            validate_upsert_keys(&pool, &self.job.config.table, &self.job.config.key_columns)
                .await?;
        }

        let mut written = 0;
        let mut pending_cdc_transaction = None;
        let mut snapshot_targets = BTreeMap::new();
        let mut validated_cdc_targets = BTreeSet::new();

        while let Some(msg) = rx.recv().await {
            match msg {
                PipelineMessage::CdcSnapshotTable(table) => {
                    prepare_snapshot_target(&pool, &table, &mut snapshot_targets).await?;
                    continue;
                }
                PipelineMessage::CdcRow(message) => {
                    if message.message.envelope.change.operation == CdcOperation::Snapshot
                        && !snapshot_targets.contains_key(&message.message.target_table)
                    {
                        bail!(
                            "Writer-{} received a snapshot row before its target-table preflight",
                            task.task_id
                        );
                    }
                    if pending_cdc_transaction.as_ref().is_some_and(|transaction| {
                        transaction != &message.message.envelope.transaction_id
                    }) {
                        bail!(
                            "Writer-{} received interleaved CDC transactions",
                            task.task_id
                        );
                    }
                    pending_cdc_transaction.get_or_insert_with(|| {
                        Arc::clone(&message.message.envelope.transaction_id)
                    });
                    written += execute_cdc_row(
                        &pool,
                        &message.message,
                        task.batch_size,
                        &mut validated_cdc_targets,
                    )
                    .await?;
                    continue;
                }
                PipelineMessage::CdcCommit(acknowledger) => {
                    if pending_cdc_transaction.as_ref().is_some_and(|transaction| {
                        transaction.as_ref() != acknowledger.transaction_id()
                    }) {
                        bail!("Writer-{} received a mismatched CDC commit", task.task_id);
                    }
                    pending_cdc_transaction = None;
                    acknowledger.acknowledge().map_err(|_| {
                        anyhow::anyhow!(
                            "Writer-{} committed CDC transaction but the Reader acknowledgement channel closed",
                            task.task_id
                        )
                    })?;
                    continue;
                }
                PipelineMessage::ReaderFinished if pending_cdc_transaction.is_some() => {
                    bail!(
                        "Writer-{} reached end of stream with an uncommitted CDC transaction",
                        task.task_id
                    );
                }
                other => {
                    match self
                        .job
                        .writer
                        .process_message(&other, &pool, &self.job.config, &task)
                        .await
                    {
                        Ok(count) => {
                            written += count;
                            if count > 0 {
                                info!(
                                    "Writer-{} 写入中 {} 条数据（累计：{}）",
                                    task.task_id, count, written
                                );
                            }
                        }
                        Err(e) => bail!("Writer-{} 写入失败: {}", task.task_id, e),
                    }
                }
            }
        }
        info!("Writer-{} 完成，共写入 {} 条数据", task.task_id, written);
        Ok(written)
    }
}

async fn execute_cdc_row(
    pool: &Arc<RdbmsPool>,
    message: &CdcRowMessage,
    batch_size: usize,
    validated_cdc_targets: &mut BTreeSet<(String, Vec<String>)>,
) -> Result<usize> {
    let plans = plan_cdc_write(message)?;
    let mut written = 0;
    for plan in plans {
        let validation_key = (plan.table.clone(), plan.key_columns.clone());
        if !validated_cdc_targets.contains(&validation_key) {
            validate_upsert_keys(pool, &plan.table, &plan.key_columns).await?;
            validated_cdc_targets.insert(validation_key);
        }
        written += execute_rdbms_write(
            pool,
            &plan.table,
            plan.mode,
            &plan.key_columns,
            plan.rows,
            batch_size,
        )
        .await?;
    }
    Ok(written)
}

async fn prepare_snapshot_target(
    pool: &RdbmsPool,
    table: &CdcSnapshotTable,
    prepared: &mut BTreeMap<String, (String, String)>,
) -> Result<()> {
    relus_connector_rdbms::identifier::validate(&table.target_table)?;
    if let Some((database, source_table)) = prepared.get(&table.target_table) {
        bail!(
            "multiple snapshot source tables route to target `{}`: {}.{} and {}.{}",
            table.target_table,
            database,
            source_table,
            table.source_database,
            table.source_table
        );
    }
    if table.policy == CdcSnapshotTargetPolicy::RequireEmpty
        && snapshot_target_has_rows(pool, &table.target_table).await?
    {
        bail!(
            "snapshot target `{}` must be empty for initial_target_policy=require_empty",
            table.target_table
        );
    }
    prepared.insert(
        table.target_table.clone(),
        (table.source_database.clone(), table.source_table.clone()),
    );
    Ok(())
}

async fn snapshot_target_has_rows(pool: &RdbmsPool, table: &str) -> Result<bool> {
    match pool {
        RdbmsPool::Postgres(pool) => {
            let query = format!("SELECT 1 FROM \"{table}\" LIMIT 1");
            Ok(sqlx::query(&query).fetch_optional(pool).await?.is_some())
        }
        RdbmsPool::Mysql(pool) => {
            let query = format!("SELECT 1 FROM `{table}` LIMIT 1");
            Ok(sqlx::query(&query).fetch_optional(pool).await?.is_some())
        }
    }
}
