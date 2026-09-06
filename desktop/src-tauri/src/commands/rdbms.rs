use relus_connector_rdbms::connector::{
    fetch_columns_mysql, fetch_columns_postgres, list_tables_mysql, list_tables_postgres,
};
use relus_connector_rdbms::metadata::ColMeta;
use relus_connector_rdbms::pool::{detect_database_kind, get_db_pool, DatabaseKind};
use tokio::sync::RwLock;

#[derive(Clone)]
struct ActiveDatabase {
    pool_key: String,
    kind: DatabaseKind,
}

#[derive(Default)]
pub struct DatabaseSession {
    active: RwLock<Option<ActiveDatabase>>,
}

impl DatabaseSession {
    async fn current(&self) -> Result<ActiveDatabase, String> {
        self.active
            .read()
            .await
            .clone()
            .ok_or_else(|| "数据库尚未连接，请先提交连接配置".to_string())
    }
}

#[tauri::command]
pub async fn connect_database(
    state: tauri::State<'_, DatabaseSession>,
    db_url: String,
    db_type: Option<String>,
) -> Result<Vec<String>, String> {
    let explicit_kind = db_type
        .map(|value| parse_database_kind(&value))
        .transpose()?;
    let kind = detect_database_kind(&db_url, explicit_kind).map_err(|error| error.to_string())?;
    let (pool_key, _) = get_db_pool(&db_url, kind, 5, None, None)
        .await
        .map_err(|error| format!("连接数据库失败: {error}"))?;

    let tables = match kind {
        DatabaseKind::Postgres => list_tables_postgres(&pool_key)
            .await
            .map_err(|error| error.to_string()),
        DatabaseKind::Mysql => list_tables_mysql(&pool_key)
            .await
            .map_err(|error| error.to_string()),
    }?;

    *state.active.write().await = Some(ActiveDatabase { pool_key, kind });
    Ok(tables)
}

#[tauri::command]
pub async fn list_database_tables(
    state: tauri::State<'_, DatabaseSession>,
) -> Result<Vec<String>, String> {
    let active = state.current().await?;
    match active.kind {
        DatabaseKind::Postgres => list_tables_postgres(&active.pool_key)
            .await
            .map_err(|error| error.to_string()),
        DatabaseKind::Mysql => list_tables_mysql(&active.pool_key)
            .await
            .map_err(|error| error.to_string()),
    }
}

#[tauri::command]
pub async fn get_database_table_schema(
    state: tauri::State<'_, DatabaseSession>,
    table: String,
) -> Result<Vec<ColMeta>, String> {
    if table.trim().is_empty() {
        return Err("表名不能为空".to_string());
    }

    let active = state.current().await?;
    match active.kind {
        DatabaseKind::Postgres => fetch_columns_postgres(&active.pool_key, &table)
            .await
            .map_err(|error| error.to_string()),
        DatabaseKind::Mysql => fetch_columns_mysql(&active.pool_key, &table)
            .await
            .map_err(|error| error.to_string()),
    }
}

fn parse_database_kind(value: &str) -> Result<DatabaseKind, String> {
    match value.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => Ok(DatabaseKind::Postgres),
        "mysql" => Ok(DatabaseKind::Mysql),
        _ => Err(format!("不支持的数据库类型: {value}")),
    }
}
