//! Writer Upsert 模式示例
//!
//! 演示如何使用 data_trans_writer 的 Upsert 模式进行数据更新

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;
use data_trans_writer::{DatabaseJob, Job, WriteMode};
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Writer Upsert 模式示例 ===\n");

    // 1. 测试 WriteMode 解析
    println!("1. WriteMode 解析测试:");
    test_write_mode_parsing();

    // 2. 创建 Upsert 配置
    println!("\n2. 创建 Upsert 配置:");
    let config = create_upsert_config();
    println!("  模式: {}", config.mode.as_ref().unwrap());
    println!("  主键列: {:?}", get_key_columns(&config));
    println!("  批次大小: {:?}", config.batch_size);

    // 3. 创建 DatabaseJob
    println!("\n3. 创建 DatabaseJob:");
    let arc_config = Arc::new(config);
    let job = DatabaseJob::new(arc_config.clone())?;
    println!("  {}", job.description());

    // 4. 任务切分
    println!("\n4. 任务切分:");
    let split_result = job.split(3).await?;
    println!("  切分为 {} 个任务", split_result.tasks.len());
    for task in &split_result.tasks {
        println!("    Task {} - mode: {:?}, batch_size: {}",
            task.task_id, task.mode, task.batch_size);
    }

    // 5. 模拟 Upsert 数据流
    println!("\n5. 模拟 Upsert 数据流:");
    simulate_upsert_flow().await?;

    println!("\n=== 示例完成 ===");
    Ok(())
}

/// 测试 WriteMode 解析
fn test_write_mode_parsing() {
    let test_cases = vec![
        ("insert", WriteMode::Insert),
        ("INSERT", WriteMode::Insert),
        ("upsert", WriteMode::Upsert),
        ("UPSERT", WriteMode::Upsert),
        ("invalid", WriteMode::Insert),  // 默认为 Insert
    ];

    for (input, expected) in test_cases {
        let result = WriteMode::from_str(input);
        let status = if result == expected { "✓" } else { "✗" };
        println!("  {} WriteMode::from_str(\"{}\") = {:?}", status, input, result);
    }
}

/// 创建 Upsert 配置
fn create_upsert_config() -> JobConfig {
    let input = DataSourceConfig {
        name: "upsert_source".to_string(),
        source_type: "api".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "url": "https://api.example.com/data",
        }),
    };

    let output = DataSourceConfig {
        name: "upsert_target".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "db_type": "postgres",
            "url": "postgres://user:pass@localhost/db",
            "table": "products",
            "key_columns": ["id"],
            "use_transaction": true,
        }),
    };

    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());
    column_mapping.insert("name".to_string(), "name".to_string());
    column_mapping.insert("price".to_string(), "price".to_string());
    column_mapping.insert("updated_at".to_string(), "updated_at".to_string());

    JobConfig {
        id: "upsert_example".to_string(),
        input,
        output,
        column_mapping,
        column_types: None,
        mode: Some("upsert".to_string()),
        batch_size: Some(50),
    }
}

/// 获取主键列
fn get_key_columns(config: &JobConfig) -> Option<Vec<String>> {
    config.output.config.get("key_columns").and_then(|v| {
        if let serde_json::Value::Array(arr) = v {
            Some(arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
        } else {
            None
        }
    })
}

/// 模拟 Upsert 数据流
async fn simulate_upsert_flow() -> anyhow::Result<()> {
    use tokio::sync::mpsc;

    let (tx, mut rx) = mpsc::channel(10);

    // 模拟初始数据（插入）
    println!("  批次 1: 插入新数据");
    let initial_data = vec![
        create_mapped_row(1, "Product A", 100.0),
        create_mapped_row(2, "Product B", 200.0),
    ];
    tx.send(PipelineMessage::DataBatch(initial_data)).await?;
    println!("    已发送 2 行新数据");

    // 模拟更新数据（upsert）
    println!("  批次 2: 更新现有数据");
    let update_data = vec![
        create_mapped_row(1, "Product A Updated", 150.0),  // 更新 id=1
        create_mapped_row(3, "Product C", 300.0),           // 插入 id=3
    ];
    tx.send(PipelineMessage::DataBatch(update_data)).await?;
    println!("    已发送 2 行数据（1 更新 + 1 插入）");

    // 发送完成信号
    tx.send(PipelineMessage::ReaderFinished).await?;

    // 接收并统计
    let mut total_rows = 0;
    while let Some(msg) = rx.recv().await {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                total_rows += rows.len();
            }
            PipelineMessage::ReaderFinished => break,
            PipelineMessage::Error(err) => {
                println!("    错误: {}", err);
            }
        }
    }

    println!("  总计处理 {} 行数据", total_rows);
    Ok(())
}

/// 创建映射行
fn create_mapped_row(id: i64, name: &str, price: f64) -> MappedRow {
    let mut values = HashMap::new();
    values.insert("id".to_string(), TypedVal::I64(id));
    values.insert("name".to_string(), TypedVal::Text(name.to_string()));
    values.insert("price".to_string(), TypedVal::F64(price));
    values.insert("updated_at".to_string(), TypedVal::Text("2024-01-01".to_string()));

    MappedRow {
        values,
        source: json!({
            "id": id,
            "name": name,
            "price": price,
        }),
    }
}
