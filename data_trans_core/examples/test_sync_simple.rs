use data_trans_common::job_config::{DataSourceConfig, JobConfig};
use data_trans_core::core::serve::sync;
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() {
    let input = DataSourceConfig {
        name: "mysql_source".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: Some(vec![
            "SELECT id, type, site_number FROM zone_data".to_string()
        ]),
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:123456@127.0.0.1:3306/my_db",
            "table": "zone_data",
            "key_columns": ["id"]
        }),
    };

    let output = DataSourceConfig {
        name: "pg_target".to_string(),
        source_type: "database".to_string(),
        is_table_mode: true,
        query_sql: None,
        config: json!({
            "db_type": "mysql",
            "url": "mysql://root:123456@127.0.0.1:3306/my_db",
            "table": "zone_data",
            "key_columns": ["id"]
        }),
    };

    let mut column_mapping = BTreeMap::new();
    column_mapping.insert("id".to_string(), "id".to_string());
    column_mapping.insert("type".to_string(), "type".to_string());
    column_mapping.insert("site_number".to_string(), "site_number".to_string());

    let mut column_types = BTreeMap::new();
    column_types.insert("id".to_string(), "int".to_string());
    column_types.insert("type".to_string(), "text".to_string());
    column_types.insert("site_number".to_string(), "text".to_string());

    let config = JobConfig {
        id: "test_sync".to_string(),
        input,
        output,
        column_mapping,
        column_types: Some(column_types),
        mode: Some("insert".to_string()),
        batch_size: Some(100),
    };

    println!("开始同步...");
    match sync(&config).await {
        Ok(_) => println!("同步成功"),
        Err(e) => println!("同步失败: {}", e),
    }
}
