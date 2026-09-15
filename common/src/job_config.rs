use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::{fmt, str::FromStr};

use crate::data_source_config::DataSourceConfig;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobConfig {
    #[serde(alias = "input")]
    pub source: DataSourceConfig,
    #[serde(alias = "target", alias = "output")]
    pub sink: DataSourceConfig,
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: Option<BTreeMap<String, String>>,
    pub sync_mode: Option<SyncMode>,
    pub batch_size: Option<usize>,
    pub channel_buffer_size: Option<usize>,
    #[serde(default)]
    pub job_id: Option<String>,
    #[serde(default)]
    pub schedule: Option<ScheduleConfig>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JobConfigParseMode {
    #[default]
    Compatible,
    Strict,
}

const JOB_FIELDS: &[&str] = &[
    "source",
    "input",
    "sink",
    "target",
    "output",
    "column_mapping",
    "column_types",
    "sync_mode",
    "batch_size",
    "channel_buffer_size",
    "job_id",
    "schedule",
];
const DATA_SOURCE_FIELDS: &[&str] = &[
    "name",
    "type",
    "is_table_mode",
    "query_sql",
    "writer_mode",
    "config",
];
const SCHEDULE_FIELDS: &[&str] = &["type", "value"];

fn reject_unknown_fields(value: &serde_json::Value, path: &str, allowed: &[&str]) -> Result<()> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    if let Some(field) = object
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        anyhow::bail!("config.parse.field: {path}.{field}: unknown field `{field}`");
    }
    Ok(())
}

fn validate_strict_fields(value: &serde_json::Value) -> Result<()> {
    reject_unknown_fields(value, "$", JOB_FIELDS)?;
    if let Some(source) = value.get("source").or_else(|| value.get("input")) {
        reject_unknown_fields(source, "source", DATA_SOURCE_FIELDS)?;
    }
    if let Some(sink) = value
        .get("sink")
        .or_else(|| value.get("target"))
        .or_else(|| value.get("output"))
    {
        reject_unknown_fields(sink, "sink", DATA_SOURCE_FIELDS)?;
    }
    if let Some(schedule) = value.get("schedule") {
        reject_unknown_fields(schedule, "schedule", SCHEDULE_FIELDS)?;
    }
    Ok(())
}

impl JobConfig {
    pub fn parse_json(input: &str) -> Result<Self> {
        Self::parse_json_with_mode(input, JobConfigParseMode::Compatible)
    }

    pub fn parse_value(value: serde_json::Value) -> Result<Self> {
        Self::parse_value_with_mode(value, JobConfigParseMode::Compatible)
    }

    pub fn parse_json_with_mode(input: &str, mode: JobConfigParseMode) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_str(input)
            .map_err(|error| anyhow::anyhow!("config.parse.json: {}", error))?;
        Self::parse_value_with_mode(value, mode)
    }

    pub fn parse_value_with_mode(
        value: serde_json::Value,
        mode: JobConfigParseMode,
    ) -> Result<Self> {
        if mode == JobConfigParseMode::Strict {
            validate_strict_fields(&value)?;
        }
        let encoded = serde_json::to_string(&value).context("config.parse.value")?;
        let mut deserializer = serde_json::Deserializer::from_str(&encoded);
        serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
            anyhow::anyhow!("config.parse.field: {}: {}", error.path(), error.inner())
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    #[default]
    Fullsnapshot,
    Incremental,
    Mix,
}

/// 调度策略配置（可从 JSON 反序列化）
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ScheduleConfig {
    /// cron 表达式
    Cron(String),
    /// 带类型标签
    Typed {
        r#type: String,
        value: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Copy)]
#[serde(rename_all = "lowercase")]
pub enum WriteMode {
    #[default]
    Insert,
    Upsert,
    Update,
    Delete,
}

impl WriteMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::Insert => "insert",
            WriteMode::Upsert => "upsert",
            WriteMode::Update => "update",
            WriteMode::Delete => "delete",
        }
    }

    pub fn from_config(config: &JobConfig) -> Self {
        config
            .sink
            .writer_mode
            .as_deref()
            .and_then(|mode| mode.parse().ok())
            .unwrap_or_default()
    }
}

impl FromStr for WriteMode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "insert" => Ok(WriteMode::Insert),
            "upsert" => Ok(WriteMode::Upsert),
            "update" => Ok(WriteMode::Update),
            "delete" => Ok(WriteMode::Delete),
            _ => Err(()),
        }
    }
}

impl fmt::Display for JobConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let json = serde_json::to_string_pretty(self).map_err(|_| fmt::Error)?;
        write!(f, "{}", json)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct CreateConfigReq {
    pub task_id: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UpdateConfigReq {
    pub task_id: String,
    pub updates: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MappingConfig {
    pub column_mapping: BTreeMap<String, String>,
    pub column_types: BTreeMap<String, String>,
    pub key_columns: Option<Vec<String>>,
    pub mode: Option<String>,
}

// impl JobConfig {
//     pub fn default_test() -> Self {
//         Self {
//             input: DataSourceConfig {
//                 name: "api_source".to_string(),
//                 source_type: "api".to_string(),
//                 is_table_mode: true,
//                 query_sql: None,
//                 config: serde_json::json!({
//                     "url": "",
//                     "method": "GET",
//                     "headers": {},
//                     "items_json_path": null,
//                     "timeout_secs": 30
//                 }),
//             },
//             output: DataSourceConfig {
//                 name: "db_target".to_string(),
//                 source_type: "database".to_string(),
//                 is_table_mode: true,
//                 query_sql: None,
//                 config: serde_json::json!({
//                     "connections": [{
//                         "db_type": "postgres",
//                         "url": "",
//                         "table": "",
//                         "key_columns": [],
//                         "max_connections": 10,
//                         "acquire_timeout_secs": 30,
//                         "use_transaction": true
//                     }]
//                 }),
//             },
//             column_mapping: BTreeMap::new(),
//             column_types: None,
//             mode: Some("insert".to_string()),
//             batch_size: Some(100),
//             channel_buffer_size: None,
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(source: &str, sink: &str) -> serde_json::Value {
        let mut value = json!({
            "column_mapping": {},
            "column_types": null
        });
        value[source] = json!({"name":"source","type":"api","config":{}});
        value[sink] = json!({"name":"sink","type":"database","config":{}});
        value
    }

    #[test]
    fn parses_canonical_and_compatibility_names() {
        for (source, sink) in [
            ("source", "sink"),
            ("input", "output"),
            ("source", "target"),
        ] {
            let config = JobConfig::parse_value(job(source, sink)).unwrap();
            assert_eq!(config.source.name, "source");
            assert_eq!(config.sink.name, "sink");
        }
    }

    #[test]
    fn compatible_mode_ignores_unknown_top_level_fields() {
        let mut value = job("source", "sink");
        value["future_option"] = json!(true);
        JobConfig::parse_value(value).unwrap();
    }

    #[test]
    fn strict_mode_rejects_unknown_top_level_fields() {
        let mut value = job("source", "sink");
        value["future_option"] = json!(true);
        let error = JobConfig::parse_value_with_mode(value, JobConfigParseMode::Strict)
            .unwrap_err()
            .to_string();
        assert!(error.contains("config.parse.field"));
        assert!(error.contains("future_option"));
    }

    #[test]
    fn strict_mode_rejects_unknown_data_source_fields_with_path() {
        let mut value = job("source", "sink");
        value["source"]["future_option"] = json!(true);
        let error = JobConfig::parse_value_with_mode(value, JobConfigParseMode::Strict)
            .unwrap_err()
            .to_string();
        assert!(error.contains("source"));
        assert!(error.contains("future_option"));
    }

    #[test]
    fn strict_mode_rejects_unknown_schedule_fields_with_path() {
        let mut value = job("source", "sink");
        value["schedule"] = json!({"type":"cron","value":"*/5 * * * *","typo":true});
        let error = JobConfig::parse_value_with_mode(value, JobConfigParseMode::Strict)
            .unwrap_err()
            .to_string();
        assert!(error.contains("schedule.typo"));
    }

    #[test]
    fn reports_json_and_field_errors_separately() {
        let error = JobConfig::parse_json("{").unwrap_err().to_string();
        assert!(error.contains("config.parse.json"));
        assert!(error.contains("line 1 column"));

        let error = JobConfig::parse_value(json!({
            "source": {"name":"source","type":"api","config":{}},
            "sink": {"name":"sink","type":"database","config":{}},
            "column_mapping": []
        }))
        .unwrap_err()
        .to_string();
        assert!(error.contains("config.parse.field"));
        assert!(error.contains("column_mapping"));
    }

    #[test]
    fn missing_required_field_reports_field_error() {
        let mut value = job("source", "sink");
        value.as_object_mut().unwrap().remove("sink");
        let error = JobConfig::parse_value(value).unwrap_err().to_string();
        assert!(error.contains("config.parse.field"));
        assert!(error.contains("sink"));
    }
}
