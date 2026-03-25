//! Writer 批量写入示例
//!
//! 演示不同批次大小对写入性能的影响

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;
use data_trans_writer::DatabaseJob;
use serde_json::json;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Writer 批量写入示例 ===\n");

    // 1. 测试不同批次大小
    println!("1. 批次大小性能对比:");
    let batch_sizes = vec![10, 50, 100, 500, 1000];
    let total_rows = 1000;

    for batch_size in batch_sizes {
        let config = create_batch_config(batch_size);
        let arc_config = Arc::new(config);
        let job = DatabaseJob::new(arc_config)?;

        let start = Instant::now();
        let result = simulate_batch_write(&job, total_rows, batch_size).await?;
        let elapsed = start.elapsed();

        println!("  batch_size={:4}: {} 行, 耗时 {:?}",
            batch_size, result, elapsed);
    }

    // 2. 演示批次切分逻辑
    println!("\n2. 批次切分演示:");
    demonstrate_batch_splitting(250, 100)?;

    // 3. 展示 PipelineMessage 批次处理
    println!("\n3. PipelineMessage 批次处理:");
    demonstrate_message_batching().await?;

    println!("\n=== 示例完成 ===");
    Ok(())
}

/// 创建指定批次大小的配置
fn create_batch_config(batch_size: usize) -> JobConfig {
    JobConfig {
        id: format!("batch_test_{}", batch_size),
        input: DataSourceConfig {
            name: "batch_source".to_string(),
            source_type: "api".to_string(),
            is_table_mode: true,
            query_sql: None,
            config: json!({ "url": "https://api.example.com/data" }),
        },
        output: DataSourceConfig {
            name: "batch_target".to_string(),
            source_type: "database".to_string(),
            is_table_mode: true,
            query_sql: None,
            config: json!({
                "db_type": "postgres",
                "url": "postgres://user:pass@localhost/db",
                "table": "batch_test",
                "key_columns": ["id"],
                "use_transaction": true,
            }),
        },
        column_mapping: {
            let mut map = BTreeMap::new();
            map.insert("id".to_string(), "id".to_string());
            map.insert("data".to_string(), "data".to_string());
            map
        },
        column_types: None,
        mode: Some("insert".to_string()),
        batch_size: Some(batch_size),
    }
}

/// 模拟批量写入
async fn simulate_batch_write(
    _job: &DatabaseJob,
    total_rows: usize,
    batch_size: usize,
) -> anyhow::Result<usize> {
    let (tx, mut rx) = mpsc::channel(1000);

    let mut sent = 0;
    while sent < total_rows {
        let remaining = total_rows - sent;
        let current_batch = batch_size.min(remaining);

        let batch_data: Vec<MappedRow> = (sent..sent + current_batch)
            .map(|i| create_test_row(i as i64))
            .collect();

        tx.send(PipelineMessage::DataBatch(batch_data)).await?;
        sent += current_batch;
    }

    tx.send(PipelineMessage::ReaderFinished).await?;

    let mut received = 0;
    while let Some(msg) = rx.recv().await {
        if let PipelineMessage::DataBatch(rows) = msg {
            received += rows.len();
        } else if let PipelineMessage::ReaderFinished = msg {
            break;
        }
    }

    Ok(received)
}

/// 创建测试行
fn create_test_row(id: i64) -> MappedRow {
    let mut values = HashMap::new();
    values.insert("id".to_string(), TypedVal::I64(id));
    values.insert("data".to_string(), TypedVal::Text(format!("data_{}", id)));

    MappedRow {
        values,
        source: json!({ "id": id, "data": format!("data_{}", id) }),
    }
}

/// 演示批次切分
fn demonstrate_batch_splitting(total: usize, batch_size: usize) -> anyhow::Result<()> {
    let mut batches = Vec::new();

    for start in (0..total).step_by(batch_size) {
        let end = (start + batch_size).min(total);
        batches.push((start, end));
    }

    println!("  总行数: {}, 批次大小: {}", total, batch_size);
    println!("  切分为 {} 个批次:", batches.len());
    for (i, (start, end)) in batches.iter().enumerate() {
        println!("    批次 {}: 行 {}-{} ({} 行)",
            i + 1, start, end - 1, end - start);
    }

    Ok(())
}

/// 演示 PipelineMessage 批次处理
async fn demonstrate_message_batching() -> anyhow::Result<()> {
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel(10);

    // 发送不同大小的批次
    let batch_sizes = vec![5, 10, 3, 20, 7];

    println!("  发送批次:");
    for (i, size) in batch_sizes.iter().enumerate() {
        let batch: Vec<MappedRow> = (0..*size)
            .map(|j| create_test_row((i * 100 + j) as i64))
            .collect();

        tx.send(PipelineMessage::DataBatch(batch)).await?;
        println!("    批次 {}: {} 行", i + 1, size);
    }

    tx.send(PipelineMessage::ReaderFinished).await?;

    // 接收并统计
    println!("  接收批次:");
    let mut total_rows = 0;
    let mut batch_count = 0;

    while let Some(msg) = rx.recv().await {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                batch_count += 1;
                total_rows += rows.len();
                println!("    批次 {}: {} 行 (累计: {} 行)",
                    batch_count, rows.len(), total_rows);
            }
            PipelineMessage::ReaderFinished => {
                println!("    收到完成信号");
                break;
            }
            PipelineMessage::Error(err) => {
                println!("    错误: {}", err);
            }
        }
    }

    println!("  总计: {} 个批次, {} 行", batch_count, total_rows);
    Ok(())
}
