use std::{collections::BTreeMap, sync::Arc};

use anyhow::{bail, Context, Result};
use mysql_async::{prelude::Queryable, Conn, Row, Value};
use relus_common::cdc::{CdcOffset, CdcValue};
use sha2::{Digest, Sha256};

use super::TableRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MySqlValueKind {
    Binary,
    Text,
    Other,
}

#[derive(Clone)]
pub(crate) struct ReplicationColumnSchema {
    pub ordinal: usize,
    pub name: Arc<str>,
    pub is_stable_key: bool,
    pub unsigned_integer_bits: Option<u32>,
    pub value_kind: MySqlValueKind,
    type_signature: Arc<str>,
}

#[derive(Clone)]
pub(crate) struct ReplicationTableSchema {
    pub columns: Vec<ReplicationColumnSchema>,
    pub stable_key_count: usize,
}

#[derive(Debug)]
pub(crate) struct SnapshotTable {
    pub name: String,
    pub engine: String,
    pub key_columns: Vec<String>,
}

pub(crate) struct SourceMetadata {
    pub server_uuid: String,
    pub schemas: BTreeMap<(String, String), ReplicationTableSchema>,
    pub schema_fingerprint: String,
}

pub(crate) async fn load_source_metadata(
    connection: &mut Conn,
    database: &str,
) -> Result<SourceMetadata> {
    let server_uuid = connection
        .query_first::<String, _>("SELECT @@server_uuid")
        .await
        .context("query MySQL server identity")?
        .ok_or_else(|| anyhow::anyhow!("MySQL server identity is unavailable"))?;
    let schemas = load_replication_schemas(connection, database).await?;
    let schema_fingerprint = fingerprint_schemas(database, &schemas);
    Ok(SourceMetadata {
        server_uuid,
        schemas,
        schema_fingerprint,
    })
}

pub(crate) async fn query_earliest_position(connection: &mut Conn) -> Result<CdcOffset> {
    let row = connection
        .query_first::<Row, _>("SHOW BINARY LOGS")
        .await
        .context("query retained MySQL binary logs")?
        .ok_or_else(|| anyhow::anyhow!("MySQL returned no retained binary logs"))?;
    let file = row
        .get::<String, _>(0)
        .ok_or_else(|| anyhow::anyhow!("MySQL binary log name is unavailable"))?;
    Ok(CdcOffset::Position { file, position: 4 })
}

pub(crate) async fn query_master_position(connection: &mut Conn) -> Result<CdcOffset> {
    let row = connection
        .query_first::<Row, _>("SHOW MASTER STATUS")
        .await
        .context("query MySQL master status")?
        .ok_or_else(|| anyhow::anyhow!("MySQL master status is unavailable"))?;
    let file = row
        .get::<String, _>(0)
        .ok_or_else(|| anyhow::anyhow!("MySQL current binary log name is unavailable"))?;
    let position = row
        .get::<u64, _>(1)
        .ok_or_else(|| anyhow::anyhow!("MySQL current binary log position is unavailable"))?;
    Ok(CdcOffset::Position { file, position })
}

pub(crate) async fn ensure_binlog_exists(connection: &mut Conn, required_file: &str) -> Result<()> {
    let logs = connection
        .query::<Row, _>("SHOW BINARY LOGS")
        .await
        .context("verify the snapshot Binlog remains available")?;
    if logs
        .iter()
        .any(|row| row.get::<String, _>(0).as_deref() == Some(required_file))
    {
        Ok(())
    } else {
        bail!("snapshot Binlog was purged before the initial snapshot completed")
    }
}

pub(crate) async fn discover_snapshot_tables(
    connection: &mut Conn,
    database: &str,
) -> Result<Vec<SnapshotTable>> {
    let engines = connection
        .exec::<Row, _, _>(
            "SELECT TABLE_NAME, ENGINE FROM information_schema.tables \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
            (database,),
        )
        .await
        .context("discover MySQL snapshot tables")?;
    let stable_keys = load_stable_keys(connection, database).await?;
    engines
        .into_iter()
        .map(|row| {
            let name = required_string(&row, 0, "snapshot table name")?;
            let engine = required_string(&row, 1, "snapshot storage engine")?;
            let key_columns = stable_keys.get(&name).cloned().unwrap_or_default();
            Ok(SnapshotTable {
                name,
                engine,
                key_columns,
            })
        })
        .collect()
}

pub(crate) fn fingerprint_table_rules(rules: &[TableRule]) -> String {
    let mut canonical = String::new();
    for rule in rules {
        canonical.push_str(&rule.include);
        canonical.push('\0');
        canonical.push_str(&rule.target);
        canonical.push('\0');
        canonical.push_str(match rule.initial_target_policy {
            super::InitialTargetPolicy::RequireEmpty => "require_empty",
            super::InitialTargetPolicy::Upsert => "upsert",
            super::InitialTargetPolicy::TruncateThenLoad => "truncate_then_load",
        });
        canonical.push('\0');
        match &rule.columns {
            super::ColumnSelection::All(value) => canonical.push_str(value),
            super::ColumnSelection::Mapping(mapping) => {
                for (source, target) in mapping {
                    canonical.push_str(source);
                    canonical.push('=');
                    canonical.push_str(target);
                    canonical.push('\0');
                }
            }
        }
        canonical.push('\n');
    }
    sha256_hex(canonical.as_bytes())
}

pub(crate) fn mysql_value_to_cdc(value: Value, kind: MySqlValueKind) -> CdcValue {
    match value {
        Value::NULL => CdcValue::Null,
        Value::Bytes(bytes) if kind == MySqlValueKind::Binary => CdcValue::Bytes(bytes),
        Value::Bytes(bytes) => match String::from_utf8(bytes) {
            Ok(string) => CdcValue::String(string),
            Err(error) => CdcValue::Bytes(error.into_bytes()),
        },
        Value::Int(value) => CdcValue::Signed(value),
        Value::UInt(value) => CdcValue::Unsigned(value),
        Value::Float(value) => CdcValue::Float(f64::from(value)),
        Value::Double(value) => CdcValue::Float(value),
        Value::Date(year, month, day, 0, 0, 0, 0) => CdcValue::Date { year, month, day },
        Value::Date(year, month, day, hour, minute, second, micros) => CdcValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        },
        Value::Time(negative, days, hours, minutes, seconds, micros) => CdcValue::Time {
            negative,
            days,
            hours,
            minutes,
            seconds,
            micros,
        },
    }
}

async fn load_replication_schemas(
    connection: &mut Conn,
    database: &str,
) -> Result<BTreeMap<(String, String), ReplicationTableSchema>> {
    let stable_keys = load_stable_keys(connection, database).await?;
    let rows: Vec<Row> = connection
        .exec(
            "SELECT CAST(TABLE_NAME AS CHAR), \
                    CAST(COLUMN_NAME AS CHAR), \
                    ORDINAL_POSITION, \
                    CAST(COLUMN_TYPE AS CHAR), \
                    CAST(DATA_TYPE AS CHAR), \
                    CAST(IS_NULLABLE AS CHAR), \
                    COALESCE(CAST(CHARACTER_SET_NAME AS CHAR), ''), \
                    COALESCE(CAST(COLLATION_NAME AS CHAR), '') \
             FROM information_schema.columns \
             WHERE TABLE_SCHEMA = ? \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
            (database,),
        )
        .await
        .context("discover MySQL replication columns")?;
    let mut discovered = BTreeMap::<(String, String), Vec<ReplicationColumnSchema>>::new();
    for row in rows {
        let table = required_string(&row, 0, "table name")?;
        let name = required_string(&row, 1, "column name")?;
        let ordinal = row
            .get::<u64, _>(2)
            .ok_or_else(|| anyhow::anyhow!("schema discovery did not return a column ordinal"))?;
        let ordinal = usize::try_from(ordinal.saturating_sub(1))
            .context("schema discovery returned an unsupported column ordinal")?;
        let column_type = required_string(&row, 3, "column type")?;
        let data_type = required_string(&row, 4, "data type")?;
        let is_nullable = required_string(&row, 5, "column nullability")?;
        let character_set = required_string(&row, 6, "column character set")?;
        let collation = required_string(&row, 7, "column collation")?;
        let is_unsigned = column_type
            .split_ascii_whitespace()
            .any(|part| part.eq_ignore_ascii_case("unsigned"));
        let table_key = (database.to_owned(), table);
        let is_stable_key = stable_keys
            .get(&table_key.1)
            .is_some_and(|columns| columns.contains(&name));
        discovered
            .entry(table_key)
            .or_default()
            .push(ReplicationColumnSchema {
                ordinal,
                name: Arc::from(name),
                is_stable_key,
                unsigned_integer_bits: is_unsigned
                    .then(|| mysql_integer_bits(&data_type))
                    .flatten(),
                value_kind: mysql_value_kind(&data_type),
                type_signature: Arc::from(format!(
                    "{column_type}|{data_type}|{is_nullable}|{character_set}|{collation}"
                )),
            });
    }

    discovered
        .into_iter()
        .map(|(table, mut columns)| {
            columns.sort_unstable_by_key(|column| column.ordinal);
            if columns
                .iter()
                .enumerate()
                .any(|(expected, column)| column.ordinal != expected)
            {
                bail!("schema discovery returned non-contiguous column ordinals");
            }
            let stable_key_count = columns.iter().filter(|column| column.is_stable_key).count();
            Ok((
                table,
                ReplicationTableSchema {
                    columns,
                    stable_key_count,
                },
            ))
        })
        .collect()
}

async fn load_stable_keys(
    connection: &mut Conn,
    database: &str,
) -> Result<BTreeMap<String, Vec<String>>> {
    let rows: Vec<Row> = connection
        .exec(
            "SELECT CAST(TABLE_NAME AS CHAR), \
                    CAST(INDEX_NAME AS CHAR), \
                    CAST(COLUMN_NAME AS CHAR), \
                    SEQ_IN_INDEX, \
                    CAST(NULLABLE AS CHAR) \
             FROM information_schema.statistics \
             WHERE TABLE_SCHEMA = ? AND NON_UNIQUE = 0 \
             ORDER BY TABLE_NAME, \
                      CASE WHEN INDEX_NAME = 'PRIMARY' THEN 0 ELSE 1 END, \
                      INDEX_NAME, SEQ_IN_INDEX",
            (database,),
        )
        .await
        .context("discover MySQL stable keys")?;
    let mut indexes = BTreeMap::<(String, String), Vec<(u64, String, bool)>>::new();
    for row in rows {
        let table = required_string(&row, 0, "unique index table")?;
        let index = required_string(&row, 1, "unique index name")?;
        let column = required_string(&row, 2, "unique index column")?;
        let sequence = row.get::<u64, _>(3).ok_or_else(|| {
            anyhow::anyhow!("schema discovery did not return a unique index column order")
        })?;
        let nullable = row
            .get::<String, _>(4)
            .unwrap_or_default()
            .eq_ignore_ascii_case("YES");
        indexes
            .entry((table, index))
            .or_default()
            .push((sequence, column, nullable));
    }

    let mut by_table = BTreeMap::<String, Vec<(String, Vec<(u64, String, bool)>)>>::new();
    for ((table, index), columns) in indexes {
        by_table.entry(table).or_default().push((index, columns));
    }
    Ok(by_table
        .into_iter()
        .filter_map(|(table, mut indexes)| {
            indexes.sort_unstable_by(|(left, _), (right, _)| {
                let left_primary = left == "PRIMARY";
                let right_primary = right == "PRIMARY";
                right_primary
                    .cmp(&left_primary)
                    .then_with(|| left.cmp(right))
            });
            indexes.into_iter().find_map(|(_, mut columns)| {
                columns.sort_unstable_by_key(|(sequence, _, _)| *sequence);
                columns.iter().all(|(_, _, nullable)| !nullable).then(|| {
                    (
                        table.clone(),
                        columns.into_iter().map(|(_, column, _)| column).collect(),
                    )
                })
            })
        })
        .collect())
}

fn required_string(row: &Row, index: usize, description: &str) -> Result<String> {
    row.get::<String, _>(index)
        .ok_or_else(|| anyhow::anyhow!("schema discovery did not return a {description}"))
}

fn mysql_integer_bits(data_type: &str) -> Option<u32> {
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" => Some(8),
        "smallint" => Some(16),
        "mediumint" => Some(24),
        "int" | "integer" => Some(32),
        "bigint" => Some(64),
        _ => None,
    }
}

fn mysql_value_kind(data_type: &str) -> MySqlValueKind {
    match data_type.to_ascii_lowercase().as_str() {
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit"
        | "geometry" => MySqlValueKind::Binary,
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => MySqlValueKind::Text,
        _ => MySqlValueKind::Other,
    }
}

fn fingerprint_schemas(
    database: &str,
    schemas: &BTreeMap<(String, String), ReplicationTableSchema>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(database.as_bytes());
    hasher.update([0]);
    for ((schema_database, table), schema) in schemas {
        hasher.update(schema_database.as_bytes());
        hasher.update([0]);
        hasher.update(table.as_bytes());
        hasher.update([0]);
        for column in &schema.columns {
            hasher.update(column.ordinal.to_le_bytes());
            hasher.update(column.name.as_bytes());
            hasher.update([column.is_stable_key as u8]);
            hasher.update(
                column
                    .unsigned_integer_bits
                    .unwrap_or_default()
                    .to_le_bytes(),
            );
            hasher.update([match column.value_kind {
                MySqlValueKind::Binary => 1,
                MySqlValueKind::Text => 2,
                MySqlValueKind::Other => 3,
            }]);
            hasher.update(column.type_signature.as_bytes());
            hasher.update([0]);
        }
    }
    hex_digest(hasher.finalize())
}

fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex_digest(hasher.finalize())
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    use std::fmt::Write;

    let mut output = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_mysql_types_do_not_use_utf8_heuristics() {
        assert_eq!(mysql_value_kind("varbinary"), MySqlValueKind::Binary);
        assert_eq!(mysql_value_kind("blob"), MySqlValueKind::Binary);
        assert_eq!(mysql_value_kind("bit"), MySqlValueKind::Binary);
        assert_eq!(mysql_value_kind("varchar"), MySqlValueKind::Text);
        assert_eq!(
            mysql_value_to_cdc(Value::Bytes(b"valid utf8".to_vec()), MySqlValueKind::Binary),
            CdcValue::Bytes(b"valid utf8".to_vec())
        );
    }
}
