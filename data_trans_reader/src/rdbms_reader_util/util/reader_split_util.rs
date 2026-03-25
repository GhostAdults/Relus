use crate::rdbms_reader_util::util::{DbPool, get_pool_from_config};
use crate::RdbmsJob;
use crate::{JobSplitResult, ReadTask};
use anyhow::Result;
use serde_json::Value as JsonValue;
use tracing::warn;

pub async fn do_split<M>(rdbms_job: &RdbmsJob<M>, advice_number: usize) -> JobSplitResult
where
    M: Send + Sync + 'static,
{
    let config = &rdbms_job.config;

    let pool = match get_pool_from_config(&rdbms_job.original_config).await {
        Ok(p) => p,
        Err(e) => {
            warn!("获取数据库连接池失败: {:?}", e);
            return JobSplitResult {
                total_records: 0,
                tasks: vec![],
            };
        }
    };

    let data_source_config = &rdbms_job.original_config.input.config;

    let conns: Vec<JsonValue> = data_source_config
        .get("connections")
        .and_then(|v| v.as_array())
        .cloned()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| JsonValue::String(s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let mut tasks = Vec::new();

    if config.is_table_mode {
        let shard_count = calculate_shard_count(conns.len(), advice_number, config.split_factor);

        if let Some(split_pk) = &config.split_pk {
            if !split_pk.trim().is_empty() && shard_count > 1 {
                match split_by_pk(&pool, config, split_pk, shard_count, &conns).await {
                    Ok(split_tasks) => tasks = split_tasks,
                    Err(e) => {
                        warn!("主键切分失败，回退到 LIMIT/OFFSET 模式: {:?}", e);
                        tasks = split_by_limit_offset(config, shard_count, &conns);
                    }
                }
            } else {
                tasks = split_by_limit_offset(config, shard_count, &conns);
            }
        } else {
            tasks = split_by_limit_offset(config, shard_count, &conns);
        }
    } else if let Some(sqls) = &config.query_sql {
        for (i, sql) in sqls.iter().enumerate() {
            if sql.trim().is_empty() {
                continue;
            }
            let conn = conns.get(i).cloned().unwrap_or_else(|| JsonValue::Null);
            tasks.push(ReadTask {
                task_id: i,
                conn,
                query_sql: Some(sql.clone()),
                offset: 0,
                limit: 0,
            });
        }
    }

    let total_records = tasks.len();
    JobSplitResult {
        total_records,
        tasks,
    }
}

fn calculate_shard_count(conn_count: usize, advice_number: usize, split_factor: usize) -> usize {
    let split_factor = if split_factor > 0 { split_factor } else { 5 };
    let base_count = if conn_count == 0 { 1 } else { conn_count };
    let each_table_split = (advice_number + base_count - 1) / base_count;
    each_table_split * split_factor
}

async fn split_by_pk(
    pool: &DbPool,
    config: &crate::RdbmsConfig,
    split_pk: &str,
    shard_count: usize,
    conns: &[JsonValue],
) -> Result<Vec<ReadTask>> {
    let (min_pk, max_pk) = get_pk_range(
        pool,
        &config.table,
        split_pk,
        config.where_clause.as_deref(),
    )
    .await?;

    let mut tasks = Vec::new();

    let ranges = if is_numeric_pk(pool, &config.table, split_pk)
        .await
        .unwrap_or(false)
    {
        split_by_numeric_pk(min_pk.as_deref(), max_pk.as_deref(), shard_count, split_pk)
    } else {
        split_by_string_pk(min_pk.as_deref(), max_pk.as_deref(), shard_count, split_pk)
    };

    for (i, range) in ranges.iter().enumerate() {
        let query_sql = build_range_query_sql(
            &config.table,
            &config.columns,
            config.where_clause.as_deref(),
            range,
        );
        let conn = conns
            .get(i % conns.len().max(1))
            .cloned()
            .unwrap_or_else(|| JsonValue::Null);
        tasks.push(ReadTask {
            task_id: i,
            conn,
            query_sql: Some(query_sql),
            offset: 0,
            limit: 0,
        });
    }

    let null_sql = build_null_pk_query_sql(
        &config.table,
        &config.columns,
        config.where_clause.as_deref(),
        split_pk,
    );
    let conn = conns.first().cloned().unwrap_or_else(|| JsonValue::Null);
    tasks.push(ReadTask {
        task_id: tasks.len(),
        conn,
        query_sql: Some(null_sql),
        offset: 0,
        limit: 0,
    });

    Ok(tasks)
}

fn split_by_limit_offset(
    config: &crate::RdbmsConfig,
    shard_count: usize,
    conns: &[JsonValue],
) -> Vec<ReadTask> {
    let mut tasks = Vec::new();
    let shard_size = 1000;

    for i in 0..shard_count {
        let offset = i * shard_size;
        let query_sql = build_limit_offset_sql(
            &config.table,
            &config.columns,
            config.where_clause.as_deref(),
            shard_size,
            offset,
        );
        let conn = conns
            .get(i % conns.len().max(1))
            .cloned()
            .unwrap_or_else(|| JsonValue::Null);
        tasks.push(ReadTask {
            task_id: i,
            conn,
            query_sql: Some(query_sql),
            offset,
            limit: shard_size,
        });
    }
    tasks
}

#[derive(Debug, Clone)]
pub enum PkRange {
    Range {
        pk: String,
        min: String,
        max: String,
    },
    Inclusive {
        pk: String,
        min: String,
        max: String,
    },
}

async fn get_pk_range(
    pool: &DbPool,
    table: &str,
    split_pk: &str,
    where_clause: Option<&str>,
) -> Result<(Option<String>, Option<String>)> {
    let where_cond = match where_clause {
        Some(w) => format!(" WHERE ({}) AND {} IS NOT NULL", w, split_pk),
        None => format!(" WHERE {} IS NOT NULL", split_pk),
    };
    let sql = format!(
        "SELECT MIN({}), MAX({}) FROM {}{}",
        split_pk, split_pk, table, where_cond
    );

    pool.executor().fetch_string_pair(&sql).await
}

async fn is_numeric_pk(pool: &DbPool, table: &str, split_pk: &str) -> Result<bool> {
    let sql = format!(
        "SELECT data_type FROM information_schema.columns WHERE table_name = '{}' AND column_name = '{}'",
        table, split_pk
    );

    let data_type = pool.executor().fetch_optional_string(&sql).await?;

    let is_numeric = data_type
        .map(|dt| {
            let dt_lower = dt.to_lowercase();
            dt_lower.contains("int")
                || dt_lower.contains("bigint")
                || dt_lower.contains("smallint")
                || dt_lower.contains("tinyint")
        })
        .unwrap_or(false);

    Ok(is_numeric)
}

fn split_by_numeric_pk(
    min: Option<&str>,
    max: Option<&str>,
    count: usize,
    pk: &str,
) -> Vec<PkRange> {
    let min_val: i64 = min.and_then(|s| s.parse().ok()).unwrap_or(0);
    let max_val: i64 = max.and_then(|s| s.parse().ok()).unwrap_or(0);

    if min_val >= max_val || count <= 1 {
        return vec![PkRange::Inclusive {
            pk: pk.to_string(),
            min: min_val.to_string(),
            max: max_val.to_string(),
        }];
    }

    let step = (max_val - min_val + count as i64) / count as i64;
    (0..count)
        .map(|i| {
            let start = min_val + step * i as i64;
            let end = if i == count - 1 {
                max_val + 1
            } else {
                start + step
            };
            PkRange::Range {
                pk: pk.to_string(),
                min: start.to_string(),
                max: end.to_string(),
            }
        })
        .collect()
}

fn split_by_string_pk(
    min: Option<&str>,
    max: Option<&str>,
    _count: usize,
    pk: &str,
) -> Vec<PkRange> {
    vec![PkRange::Range {
        pk: pk.to_string(),
        min: min.unwrap_or("").to_string(),
        max: max.unwrap_or("").to_string(),
    }]
}

fn build_range_query_sql(
    table: &str,
    columns: &str,
    where_clause: Option<&str>,
    range: &PkRange,
) -> String {
    let base = format!("SELECT {} FROM {}", columns, table);
    let range_cond = match range {
        PkRange::Range { pk, min, max } => format!("{} >= {} AND {} < {}", pk, min, pk, max),
        PkRange::Inclusive { pk, min, max } => format!("{} >= {} AND {} <= {}", pk, min, pk, max),
    };

    match where_clause {
        Some(w) => format!("{} WHERE ({}) AND ({})", base, w, range_cond),
        None => format!("{} WHERE {}", base, range_cond),
    }
}

fn build_null_pk_query_sql(
    table: &str,
    columns: &str,
    where_clause: Option<&str>,
    split_pk: &str,
) -> String {
    let base = format!("SELECT {} FROM {}", columns, table);
    let null_cond = format!("{} IS NULL", split_pk);

    match where_clause {
        Some(w) => format!("{} WHERE ({}) AND ({})", base, w, null_cond),
        None => format!("{} WHERE {}", base, null_cond),
    }
}

fn build_limit_offset_sql(
    table: &str,
    columns: &str,
    where_clause: Option<&str>,
    limit: usize,
    offset: usize,
) -> String {
    let base = format!("SELECT {} FROM {}", columns, table);

    match where_clause {
        Some(w) => format!("{} WHERE {} LIMIT {} OFFSET {}", base, w, limit, offset),
        None => format!("{} LIMIT {} OFFSET {}", base, limit, offset),
    }
}
