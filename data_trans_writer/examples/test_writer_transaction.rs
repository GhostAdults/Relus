//! Writer 事务模式示例
//!
//! 演示 data_trans_writer 的事务处理功能

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;
use data_trans_writer::DatabaseJob;
use serde_json::json;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Writer 事务模式示例 ===\n");

    // 1. 对比事务与非事务模式
    println!("1. 配置对比:");
    compare_transaction_modes();

    // 2. 创建事务配置
    println!("\n2. 创建事务模式配置:");
    let config = create_transaction_config(true);
    println!("  使用事务: {}", is_transaction_enabled(&config));
    println!("  批次大小: {:?}", config.batch_size);

    // 3. 创建 Job 并测试
    println!("\n3. 测试事务模式写入:");
    let arc_config = Arc::new(config);
    let job = DatabaseJob::new(arc_config)?;

    // 4. 模拟批量写入
    println!("\n4. 模拟批量写入:");
    simulate_batch_write(&job, 100, 10).await?;

    println!("\n=== 示例完成 ===");
    Ok(())
}

/// 对比事务与非事务模式
fn compare_transaction_modes() {
    let configs = vec![
        ("非事务模式", false, 100),
        ("事务模式", true, 100),
    ];

    for (name, use_tx, batch_size) in configs {
        println!("  {}:", name);
        println!("    use_transaction: {}", use_tx);
        println!("    batch_size: {}", batch_size);
        println!("    特点: {}", if use_tx {
            "原子性保证，失败时回滚"
        } else {
            "逐条提交，部分失败不影响已提交数据"
        });
    }
}

/// 创建事务配置
fn create_transaction_config(use_transaction: bool) -> JobConfig {
    let output_config = json!({
        "db_type": "mysql",
        "url": "mysql://user:pass@localhost/db",
        "table": "orders",
        "key_columns": ["order_id"],
        "use_transaction": use_transaction,
    });

    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("order_id".to_string(), "order_id".to_string());
    column_mapping.insert("customer_id".to_string(), "customer_id".to_string());
    column_mapping.insert("amount".to_string(), "amount".to_string());
    column_mapping.insert("status".to_string(), "status".to_string());

    JobConfig {
        id: "transaction_example".to_string(),
        input: DataSourceConfig {
            name: "order_source".to_string(),
            source_type: "api".to_string(),
            is_table_mode: true,
            query_sql: None,
            config: json!({ "url": "https://api.example.com/orders" }),
        },
        output: DataSourceConfig {
            name: "order_target".to_string(),
            source_type: "database".to_string(),
            is_table_mode: true,
            query_sql: None,
            config: output_config,
        },
        column_mapping,
        column_types: None,
        mode: Some("insert".to_string()),
        batch_size: Some(50),
    }
}

/// 检查是否启用事务
fn is_transaction_enabled(config: &JobConfig) -> bool {
    config.output.config.get("use_transaction")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// 模拟批量写入
async fn simulate_batch_write(_job: &DatabaseJob, total_rows: usize, batch_size: usize) -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(100);

    let mut written = 0;
    let mut batch_count = 0;

    // 分批发送数据
    while written < total_rows {
        let remaining = total_rows - written;
        let current_batch = batch_size.min(remaining);

        let batch_data: Vec<MappedRow> = (written..written + current_batch)
            .map(|i| create_order_row(i as i64))
            .collect();

        tx.send(PipelineMessage::DataBatch(batch_data)).await?;
        batch_count += 1;
        written += current_batch;

        println!("  批次 {}: 已发送 {} 行 (累计: {})",
            batch_count, current_batch, written);
    }

    // 发送完成信号
    tx.send(PipelineMessage::ReaderFinished).await?;

    // 接收并统计
    let mut total_received = 0;
    while let Some(msg) = rx.recv().await {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                total_received += rows.len();
            }
            PipelineMessage::ReaderFinished => break,
            PipelineMessage::Error(err) => {
                println!("  错误: {}", err);
            }
        }
    }

    println!("  总计接收: {} 行", total_received);
    Ok(())
}

/// 创建订单行
fn create_order_row(order_id: i64) -> MappedRow {
    let mut values = HashMap::new();
    values.insert("order_id".to_string(), TypedVal::I64(order_id));
    values.insert("customer_id".to_string(), TypedVal::I64(order_id % 10 + 1));
    values.insert("amount".to_string(), TypedVal::F64((order_id as f64) * 99.99));
    values.insert("status".to_string(), TypedVal::Text("pending".to_string()));

    MappedRow {
        values,
        source: json!({
            "order_id": order_id,
            "customer_id": order_id % 10 + 1,
            "amount": order_id as f64 * 99.99,
        }),
    }
}
