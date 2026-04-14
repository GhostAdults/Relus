use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataSourceConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    #[serde(default = "default_is_table_mode")]
    pub is_table_mode: bool,
    pub query_sql: Option<Vec<String>>,
    pub config: Value,
}

fn default_is_table_mode() -> bool {
    true
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DbConfig {
    pub url: String,
    pub table: String,
    pub key_columns: Option<Vec<String>>,
    pub max_connections: Option<u32>,
    pub acquire_timeout_secs: Option<u64>,
    pub use_transaction: Option<bool>,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: "".to_string(),
            table: "".to_string(),
            key_columns: Some(Vec::<String>::new()),
            max_connections: Some(10),
            acquire_timeout_secs: Some(30),
            use_transaction: Some(false),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ApiConfig {
    pub url: String,
    pub method: Option<String>,
    pub headers: Option<BTreeMap<String, String>>,
    pub body: Option<Value>,
    pub items_json_path: Option<String>,
    pub timeout_secs: Option<u64>,
}

impl DataSourceConfig {
    pub fn config_str(&self, key: &str) -> Option<String> {
        self.config.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
    }

    pub fn config_u64(&self, key: &str) -> Option<u64> {
        self.config.get(key).and_then(|v| v.as_u64())
    }

    pub fn config_bool(&self, key: &str) -> Option<bool> {
        self.config.get(key).and_then(|v| v.as_bool())
    }

    pub fn config_str_or_default(&self, key: &str) -> String {
        self.config.get(key).and_then(|v| v.as_str()).unwrap_or_default().to_string()
    }

    /// 从 config 中提取 connection 对象
    /// - 数组格式: `{ "connections": [{ ... }] }` → 取第一个
    /// - 对象格式: `{ "connection": { ... } }` → 直接使用
    fn extract_connection(&self) -> Result<&Value> {
        if let Some(arr) = self.config.get("connections").and_then(|v| v.as_array()) {
            return arr
                .first()
                .ok_or_else(|| anyhow::anyhow!("config.connections 数组为空"));
        }
        if let Some(obj) = self.config.get("connection") {
            return Ok(obj);
        }
        Err(anyhow::anyhow!(
            "config 中缺少 connections 或 connection 字段"
        ))
    }

    pub fn parse_database_config(&self) -> Result<DbConfig> {
        let conn = self.extract_connection()?;

        let url = conn
            .get("url")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let table = conn
            .get("table")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let key_columns = conn.get("key_columns").and_then(|v| {
            v.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
        });

        let max_connections = conn
            .get("max_connections")
            .and_then(|v| v.as_u64())
            .map(|i| i as u32);
        let acquire_timeout_secs = conn.get("acquire_timeout_secs").and_then(|v| v.as_u64());
        let use_transaction = conn.get("use_transaction").and_then(|v| v.as_bool());

        Ok(DbConfig {
            url,
            table,
            key_columns,
            max_connections,
            acquire_timeout_secs,
            use_transaction,
        })
    }

    pub fn get_source_db_type(&self) -> Option<String> {
        if self.source_type != "database" {
            return None;
        }
        self.extract_connection()
            .ok()
            .and_then(|conn| conn.get("db_type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    pub fn parse_api_config(&self) -> Result<ApiConfig> {
        let url = self.config_str_or_default("url");
        let method = self.config_str("method");
        let items_json_path = self.config_str("items_json_path");
        let timeout_secs = self.config_u64("timeout_secs");
        let headers = self.config.get("headers").and_then(|v| {
            if let Value::Object(h) = v {
                let mut hm = BTreeMap::new();
                for (k, v) in h {
                    if let Some(s) = v.as_str() {
                        hm.insert(k.clone(), s.to_string());
                    }
                }
                Some(hm)
            } else {
                None
            }
        });

        Ok(ApiConfig {
            url,
            method,
            headers,
            body: None,
            items_json_path,
            timeout_secs,
        })
    }
}
