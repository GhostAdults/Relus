//! data_trans_writer 集成测试
//!
//! 测试 DatabaseJob 和 RdbmsJob 的完整写入流程

use std::collections::BTreeMap;
use std::sync::Arc;

use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_common::pipeline::{MappedRow, PipelineMessage};
use data_trans_common::types::TypedVal;
use data_trans_writer::{DatabaseJob, Job, WriteMode};
use serde_json::json;
use tokio::sync::mpsc;

/// 创建测试用的 JobConfig
fn create_test_job_config(mode: &str) -> JobConfig {
    let input = DataSourceConfig {
        name: "test_input".to_string(),
        source_type: "api".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({}),
    };

    let output = DataSourceConfig {
        name: "test_output".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:test@localhost:3307/test_db",
            "table": "test_table",
            "key_columns": ["id"],
            "use_transaction": false,
        }),
    };

    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());

    JobConfig {
        id: "test_job".to_string(),
        input,
        output,
        column_mapping,
        column_types: None,
        mode: Some(mode.to_string()),
        batch_size: Some(10),
    }
}

#[test]
fn test_database_job_new() {
    let config = create_test_job_config("insert");
    let arc_config = Arc::new(config);
    let job = DatabaseJob::new(arc_config).unwrap();
    assert_eq!(job.description(), "DatabaseJob (target: test_output)");
}

#[test]
fn test_database_job_description() {
    let config = create_test_job_config("insert");
    let arc_config = Arc::new(config);
    let job = DatabaseJob::new(arc_config).unwrap();
    let desc = job.description();
    assert!(desc.contains("DatabaseJob"));
    assert!(desc.contains("test_output"));
}

#[test]
fn test_write_mode_parsing() {
    assert_eq!(WriteMode::from_str("insert"), WriteMode::Insert);
    assert_eq!(WriteMode::from_str("INSERT"), WriteMode::Insert);
    assert_eq!(WriteMode::from_str("upsert"), WriteMode::Upsert);
    assert_eq!(WriteMode::from_str("UPSERT"), WriteMode::Upsert);
    assert_eq!(WriteMode::from_str("invalid"), WriteMode::Insert);
}

#[test]
fn test_mapped_row_creation() {
    let mut values = std::collections::HashMap::new();
    values.insert("id".to_string(), TypedVal::I64(1));
    values.insert("name".to_string(), TypedVal::Text("test".to_string()));

    let row = MappedRow {
        values,
        source: json!({"id": 1, "name": "test"}),
    };

    assert_eq!(row.values.len(), 2);
    assert!(matches!(row.values.get("id"), Some(TypedVal::I64(1))));
}

#[tokio::test]
async fn test_pipeline_message_data_batch() {
    let mut values = std::collections::HashMap::new();
    values.insert("id".to_string(), TypedVal::I64(1));

    let rows = vec![MappedRow {
        values,
        source: json!({"id": 1}),
    }];

    let msg = PipelineMessage::DataBatch(rows);
    match msg {
        PipelineMessage::DataBatch(r) => {
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].values.len(), 1);
        }
        _ => panic!("Expected DataBatch"),
    }
}

#[tokio::test]
async fn test_pipeline_message_reader_finished() {
    let msg = PipelineMessage::ReaderFinished;
    match msg {
        PipelineMessage::ReaderFinished => {}
        _ => panic!("Expected ReaderFinished"),
    }
}

#[tokio::test]
async fn test_pipeline_message_error() {
    let msg = PipelineMessage::Error("test error".to_string());
    match msg {
        PipelineMessage::Error(e) => {
            assert_eq!(e, "test error");
        }
        _ => panic!("Expected Error"),
    }
}

#[tokio::test]
async fn test_channel_send_receive() {
    let (tx, mut rx) = mpsc::channel(10);

    let mut values = std::collections::HashMap::new();
    values.insert("id".to_string(), TypedVal::I64(1));

    let rows = vec![MappedRow {
        values,
        source: json!({"id": 1}),
    }];

    tx.send(PipelineMessage::DataBatch(rows)).await.unwrap();
    tx.send(PipelineMessage::ReaderFinished).await.unwrap();

    let msg1 = rx.recv().await.unwrap();
    match msg1 {
        PipelineMessage::DataBatch(r) => assert_eq!(r.len(), 1),
        _ => panic!("Expected DataBatch"),
    }

    let msg2 = rx.recv().await.unwrap();
    match msg2 {
        PipelineMessage::ReaderFinished => {}
        _ => panic!("Expected ReaderFinished"),
    }
}

#[test]
fn test_job_config_default_with_id() {
    let config = JobConfig::default_with_id("test_id".to_string());
    assert_eq!(config.id, "test_id");
    assert_eq!(config.mode, Some("insert".to_string()));
    assert_eq!(config.batch_size, Some(100));
}

#[test]
fn test_db_config_default() {
    let config = data_trans_common::DbConfig::default();
    assert_eq!(config.url, "");
    assert_eq!(config.table, "");
    assert_eq!(config.max_connections, Some(10));
    assert_eq!(config.acquire_timeout_secs, Some(30));
    assert_eq!(config.use_transaction, Some(false));
}
