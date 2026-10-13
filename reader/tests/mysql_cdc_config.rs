#![allow(clippy::expect_used)]

use relus_common::{job_config::JobConfigParseMode, JobConfig};
use relus_reader::mysql_cdc::{MySqlCdcConfig, StartupMode};

fn production_job(source_type: &str) -> serde_json::Value {
    serde_json::json!({
        "execution": {
            "delivery_guarantee": "at_least_once",
            "checkpoint": {
                "enabled": true,
                "interval": "30s",
                "timeout": "2m",
                "max_consecutive_failures": 3
            }
        },
        "source": {
            "name": "mysql-sales-cdc",
            "type": source_type,
            "config_version": 1,
            "config": {
                "source_id": "mysql-sales",
                "connection": {
                    "host": "127.0.0.1",
                    "port": 3306,
                    "database": "sales",
                    "username": "replication",
                    "password": {"env": "RELUS_TEST_REPLICATION_PASSWORD"}
                },
                "replication": {
                    "server_id": 4101,
                    "protocol_compression": true
                },
                "tls": {"mode": "disabled"},
                "startup": {
                    "mode": "specific",
                    "offset": {
                        "kind": "position",
                        "file": "mysql-bin.000007",
                        "position": 4
                    }
                },
                "checkpoint": {
                    "id": "mysql-sales",
                    "path": "./checkpoints/mysql-sales.json"
                },
                "retry": {},
                "heartbeat": {},
                "buffer": {},
                "tables": [{
                    "include": "orders",
                    "target": "ods_orders",
                    "columns": "*"
                }]
            }
        },
        "sink": {
            "name": "warehouse",
            "type": "database",
            "config": {"connection": {"table": "unused-for-cdc"}}
        },
        "column_mapping": {}
    })
}

#[test]
fn parses_the_versioned_mysql_cdc_configuration_without_exposing_secrets() {
    let job =
        JobConfig::parse_value_with_mode(production_job("mysql_cdc"), JobConfigParseMode::Strict)
            .expect("the canonical job shape should parse");
    let config = MySqlCdcConfig::from_job(&job).expect("the CDC config should validate");

    assert_eq!(config.source_id, "mysql-sales");
    assert_eq!(config.connection.database, "sales");
    assert_eq!(config.replication.server_id, 4101);
    assert!(config.replication.protocol_compression);
    assert!(matches!(config.startup.mode, StartupMode::Specific));
    assert_eq!(config.tables[0].include, "orders");
    assert_eq!(config.tables[0].target, "ods_orders");

    let debug = format!("{config:?}");
    assert!(!debug.contains("RELUS_TEST_REPLICATION_PASSWORD"));
    assert!(debug.contains("<redacted>"));
}

#[test]
fn rejects_the_legacy_source_type_with_a_migration_error() {
    let job = JobConfig::parse_value(production_job("mysql_binlog")).expect(
        "the generic job parser should retain compatibility long enough to report migration",
    );
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("the legacy source type must not start")
        .to_string();

    assert!(error.contains("mysql_binlog"));
    assert!(error.contains("mysql_cdc"));
    assert!(error.contains("migration"));
}

#[test]
fn rejects_unknown_nested_fields_with_the_full_configuration_path() {
    let mut value = production_job("mysql_cdc");
    value["source"]["config"]["replication"]["serve_id"] = serde_json::json!(4102);
    let job =
        JobConfig::parse_value(value).expect("generic parsing should preserve the source config");
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("unknown CDC fields must fail closed")
        .to_string();

    assert!(error.contains("source.config.replication.serve_id"));
}

#[test]
fn rejects_global_column_mapping_for_mysql_cdc() {
    let mut value = production_job("mysql_cdc");
    value["column_mapping"] = serde_json::json!({"id": "id"});
    let job = JobConfig::parse_value(value).expect("the generic job should parse");
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("CDC mappings belong to table rules")
        .to_string();

    assert!(error.contains("column_mapping"));
    assert!(error.contains("source.config.tables"));
}

#[test]
fn rejects_invalid_heartbeat_and_retry_windows_before_connecting() {
    let mut value = production_job("mysql_cdc");
    value["source"]["config"]["heartbeat"] = serde_json::json!({
        "interval_seconds": 30,
        "timeout_seconds": 30
    });
    let job = JobConfig::parse_value(value).expect("the generic job should parse");
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("heartbeat timeout must exceed its interval")
        .to_string();
    assert!(error.contains("heartbeat.timeout_seconds"));

    let mut value = production_job("mysql_cdc");
    value["source"]["config"]["retry"] = serde_json::json!({
        "initial_delay_ms": 2_000,
        "max_delay_ms": 1_000,
        "max_attempts": 3
    });
    let job = JobConfig::parse_value(value).expect("the generic job should parse");
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("retry backoff must not shrink below its initial delay")
        .to_string();
    assert!(error.contains("retry.max_delay_ms"));
}

#[test]
fn rejects_unsafe_or_invalid_sink_identifiers_at_configuration_time() {
    let mut value = production_job("mysql_cdc");
    value["source"]["config"]["tables"][0]["target"] =
        serde_json::json!("ods_orders; DROP TABLE audit");
    let job = JobConfig::parse_value(value).expect("the generic job should parse");
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("target identifiers must be validated before execution")
        .to_string();
    assert!(error.contains("source.config.tables[0].target"));
}

#[test]
fn repository_mysql_cdc_example_uses_the_production_configuration_shape() {
    let contents = include_str!("../../cli/user_config/mybinlog2pg.json");
    let job = JobConfig::parse_json_with_mode(contents, JobConfigParseMode::Strict)
        .expect("the checked-in MySQL CDC example should parse strictly");
    let config = MySqlCdcConfig::from_job(&job)
        .expect("the checked-in MySQL CDC example should pass Reader validation");

    assert!(matches!(config.startup.mode, StartupMode::Initial));
    assert!(contents.contains("RELUS_MYSQL_CDC_PASSWORD"));
    assert!(!contents.contains("mysql_binlog"));
    assert!(!contents.contains("123456"));
}

#[test]
fn rejects_truncate_snapshot_policy_until_the_sink_declares_it_safe() {
    let mut value = production_job("mysql_cdc");
    value["source"]["config"]["startup"] = serde_json::json!({"mode": "initial"});
    value["source"]["config"]["tables"][0]["initial_target_policy"] =
        serde_json::json!("truncate_then_load");
    let job = JobConfig::parse_value(value).expect("the generic job should parse");
    let error = MySqlCdcConfig::from_job(&job)
        .expect_err("unsafe truncate must not silently run without a Sink capability")
        .to_string();

    assert!(error.contains("truncate_then_load"));
    assert!(error.contains("Sink capability"));
}
