//! SQL 构建器单元测试

use data_trans_reader::rdbms_reader_util::util::dbpool::DbKind;
use data_trans_writer::rdbms_writer_util::rdbms_writer::build_sql;
use data_trans_writer::WriteMode;

#[test]
fn test_build_sql_insert_postgres() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let keys = vec!["id".to_string()];
    let nonkeys = vec!["name".to_string()];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Insert, DbKind::Postgres).unwrap();
    assert_eq!(sql, "INSERT INTO test_table (id, name) VALUES ($1, $2)");
}

#[test]
fn test_build_sql_insert_mysql() {
    let columns = vec!["id".to_string(), "name".to_string()];
    let keys = vec!["id".to_string()];
    let nonkeys = vec!["name".to_string()];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Insert, DbKind::Mysql).unwrap();
    assert_eq!(sql, "INSERT INTO test_table (id, name) VALUES (?, ?)");
}

#[test]
fn test_build_sql_upsert_postgres() {
    let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
    let keys = vec!["id".to_string()];
    let nonkeys = vec!["name".to_string(), "value".to_string()];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Upsert, DbKind::Postgres).unwrap();
    assert!(sql.contains("INSERT INTO test_table (id, name, value) VALUES ($1, $2, $3)"));
    assert!(sql.contains("ON CONFLICT (id)"));
    assert!(sql.contains("DO UPDATE SET"));
    assert!(sql.contains("name = EXCLUDED.name"));
    assert!(sql.contains("value = EXCLUDED.value"));
}

#[test]
fn test_build_sql_upsert_postgres_multiple_keys() {
    let columns = vec!["id".to_string(), "tenant_id".to_string(), "name".to_string()];
    let keys = vec!["id".to_string(), "tenant_id".to_string()];
    let nonkeys = vec!["name".to_string()];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Upsert, DbKind::Postgres).unwrap();
    assert!(sql.contains("ON CONFLICT (id, tenant_id)"));
    assert!(sql.contains("name = EXCLUDED.name"));
}

#[test]
fn test_build_sql_upsert_mysql() {
    let columns = vec!["id".to_string(), "name".to_string(), "value".to_string()];
    let keys = vec!["id".to_string()];
    let nonkeys = vec!["name".to_string(), "value".to_string()];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Upsert, DbKind::Mysql).unwrap();
    assert!(sql.contains("INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)"));
    assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
    assert!(sql.contains("name = VALUES(name)"));
    assert!(sql.contains("value = VALUES(value)"));
}

#[test]
fn test_build_sql_upsert_postgres_no_keys_error() {
    let columns = vec!["name".to_string(), "value".to_string()];
    let keys: Vec<String> = vec![];
    let nonkeys = vec!["name".to_string(), "value".to_string()];
    let result = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Upsert, DbKind::Postgres);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("key_columns"));
}

#[test]
fn test_build_sql_single_column() {
    let columns = vec!["id".to_string()];
    let keys = vec!["id".to_string()];
    let nonkeys: Vec<String> = vec![];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Insert, DbKind::Postgres).unwrap();
    assert_eq!(sql, "INSERT INTO test_table (id) VALUES ($1)");
}

#[test]
fn test_build_sql_many_columns() {
    let columns: Vec<String> = (0..10).map(|i| format!("col{}", i)).collect();
    let keys = vec!["col0".to_string()];
    let nonkeys: Vec<String> = (1..10).map(|i| format!("col{}", i)).collect();
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Insert, DbKind::Mysql).unwrap();
    let placeholders: Vec<&str> = (0..10).map(|_| "?").collect();
    let expected = format!("INSERT INTO test_table ({}) VALUES ({})", columns.join(", "), placeholders.join(", "));
    assert_eq!(sql, expected);
}

#[test]
fn test_build_sql_upsert_mysql_no_nonkeys() {
    let columns = vec!["id".to_string()];
    let keys = vec!["id".to_string()];
    let nonkeys: Vec<String> = vec![];
    let sql = build_sql("test_table", &columns, &keys, &nonkeys, WriteMode::Upsert, DbKind::Mysql).unwrap();
    // MySQL 允许没有非主键列的 upsert
    assert!(sql.contains("INSERT INTO test_table"));
    assert!(sql.contains("ON DUPLICATE KEY UPDATE"));
}
