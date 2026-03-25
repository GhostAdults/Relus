//! Writer 基本功能示例
//!
//! 演示如何使用 data_trans_writer 进行基本的数据写入操作

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;
use data_trans_writer::{DatabaseJob, Job};
use serde_json::json;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 创建测试配置
    let config = create_test_config("insert");

    println!("=== Writer 基本功能示例 ===");
    println!("配置 ID: {}", config.id);
    println!("写入模式: {:?}", config.mode);
    println!("批次大���: {:?}", config.batch_size);

    // 2. 创建 DatabaseJob
    let arc_config = Arc::new(config);
    let job = DatabaseJob::new(arc_config.clone())?;
    println!("\nJob 描述: {}", job.description());

    // 3. 测试任务切分
    let split_result = job.split(2).await?;
    println!("\n任务切分结果:");
    println!("  任务数量: {}", split_result.tasks.len());
    for task in &split_result.tasks {
        println!("  - Task {}: mode={:?}, batch_size={}, use_transaction={}",
            task.task_id, task.mode, task.batch_size, task.use_transaction);
    }

    // 4. 模拟数据发送
    println!("\n模拟数据发送:");
    let (tx, mut rx) = mpsc::channel(10);

    // 创建测试数据
    let test_data = create_test_rows(5);
    println!("  创建了 {} 行测试数据", test_data.len());

    // 发送数据批次
    tx.send(PipelineMessage::DataBatch(test_data)).await?;
    println!("  已发送数据批次");

    // 发送完成信号
    tx.send(PipelineMessage::ReaderFinished).await?;
    println!("  已发送完成信号");

    // 5. 接收并处理消息
    println!("\n接收消息:");
    let mut count = 0;
    while let Some(msg) = rx.recv().await {
        match msg {
            PipelineMessage::DataBatch(rows) => {
                println!("  收到数据批次: {} 行", rows.len());
                count += rows.len();
            }
            PipelineMessage::ReaderFinished => {
                println!("  收到完成信号");
                break;
            }
            PipelineMessage::Error(err) => {
                println!("  收到错误: {}", err);
            }
        }
    }

    println!("\n=== 示例完成 ===");
    println!("总计处理 {} 行数据", count);

    Ok(())
}

/// 创建测试配置
fn create_test_config(mode: &str) -> JobConfig {
    let input = DataSourceConfig {
        name: "test_input".to_string(),
        source_type: "api".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "url": "https://api.example.com/data",
            "method": "GET",
        }),
    };

    let output = DataSourceConfig {
        name: "test_output".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "db_type": "sqlite",
            "url": "sqlite::memory:",
            "table": "test_table",
            "key_columns": ["id"],
            "use_transaction": false,
        }),
    };

    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());
    column_mapping.insert("name".to_string(), "name".to_string());
    column_mapping.insert("value".to_string(), "value".to_string());

    let mut column_types = BTreeMap::new();
    column_types.insert("id".to_string(), "int".to_string());
    column_types.insert("name".to_string(), "text".to_string());
    column_types.insert("value".to_string(), "text".to_string());

    JobConfig {
        id: "test_writer_basic".to_string(),
        input,
        output,
        column_mapping,
        column_types: Some(column_types),
        mode: Some(mode.to_string()),
        batch_size: Some(100),
    }
}

/// 创建测试数据行
fn create_test_rows(count: usize) -> Vec<MappedRow> {
    (0..count).map(|i| {
        let mut values = HashMap::new();
        values.insert("id".to_string(), TypedVal::I64(i as i64));
        values.insert("name".to_string(), TypedVal::Text(format!("item_{}", i)));
        values.insert("value".to_string(), TypedVal::Text(format!("value_{}", i)));

        MappedRow {
            values,
            source: json!({
                "id": i,
                "name": format!("item_{}", i),
                "value": format!("value_{}", i),
            }),
        }
    }).collect()
}
