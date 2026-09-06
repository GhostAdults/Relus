use crate::metadata::{ColMeta, TableMeta};
use crate::pool::{
    detect_database_kind, get_db_pool, get_db_pool_by_key, DatabaseKind, RdbmsPool,
};
use anyhow::{bail, Context, Result};
use sqlx::Row as _;
use tracing::info;

/// RDBMS 通用操作：表发现、列探测、批量查询
pub struct RdbmsConnector {
    pool_key: String,
    kind: DatabaseKind,
}

impl RdbmsConnector {
    pub async fn connect(url: &str) -> Result<Self> {
        let kind = detect_database_kind(url, None)?;
        let (pool_key, _) = get_db_pool(url, kind, 5, None, None).await?;
        info!("[RdbmsConnector] 已连接 {}", database_kind_name(kind));
        Ok(Self {
            pool_key,
            kind,
        })
    }

    pub fn db_type(&self) -> &str {
        database_kind_name(self.kind)
    }

    /// 列出用户表
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        match self.kind {
            DatabaseKind::Postgres => list_tables_postgres(&self.pool_key).await,
            DatabaseKind::Mysql => list_tables_mysql(&self.pool_key).await,
        }
    }

    /// 探测表结构
    pub async fn describe(&self, table: &str) -> Result<Vec<ColMeta>> {
        match self.kind {
            DatabaseKind::Postgres => fetch_columns_postgres(&self.pool_key, table).await,
            DatabaseKind::Mysql => fetch_columns_mysql(&self.pool_key, table).await,
        }
    }

    /// 检查表是否存在
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        let tables = self.list_tables().await?;
        Ok(tables.iter().any(|t| t.eq_ignore_ascii_case(table)))
    }

    /// 获取表完整元数据
    pub async fn table_meta(&self, table: &str) -> Result<TableMeta> {
        self.describe(table).await?;
        Ok(TableMeta {
            schema: None,
            name: table.to_string(),
        })
    }
}

fn postgres_pool(pool_key: &str) -> Result<sqlx::PgPool> {
    match get_db_pool_by_key(pool_key).context("获取 PostgreSQL 连接失败")? {
        RdbmsPool::Postgres(pool) => Ok(pool),
        RdbmsPool::Mysql(_) => bail!("连接池类型不匹配，预期 PostgreSQL"),
    }
}

fn mysql_pool(pool_key: &str) -> Result<sqlx::MySqlPool> {
    match get_db_pool_by_key(pool_key).context("获取 MySQL 连接失败")? {
        RdbmsPool::Mysql(pool) => Ok(pool),
        RdbmsPool::Postgres(_) => bail!("连接池类型不匹配，预期 MySQL"),
    }
}

fn database_kind_name(kind: DatabaseKind) -> &'static str {
    match kind {
        DatabaseKind::Postgres => "postgres",
        DatabaseKind::Mysql => "mysql",
    }
}

pub async fn list_tables_postgres(pool_key: &str) -> Result<Vec<String>> {
    let pool = postgres_pool(pool_key)?;
    let rows = sqlx::query(
        "SELECT tablename FROM pg_catalog.pg_tables \
         WHERE schemaname NOT IN ('pg_catalog','information_schema')",
    )
    .fetch_all(&pool)
    .await?;
    Ok(rows.iter().map(|r| r.get("tablename")).collect())
}

pub async fn list_tables_mysql(pool_key: &str) -> Result<Vec<String>> {
    let pool = mysql_pool(pool_key)?;
    let rows = sqlx::query("SHOW TABLES").fetch_all(&pool).await?;
    Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
}

pub async fn fetch_columns_postgres(pool_key: &str, table: &str) -> Result<Vec<ColMeta>> {
    let pool = postgres_pool(pool_key)?;
    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_schema='public' AND table_name=$1 \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(&pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ColMeta {
                name: row.try_get("column_name")?,
                data_type: row.try_get("data_type")?,
                nullable: row.try_get::<String, _>("is_nullable")? == "YES",
            })
        })
        .collect()
}

pub async fn fetch_columns_mysql(pool_key: &str, table: &str) -> Result<Vec<ColMeta>> {
    let pool = mysql_pool(pool_key)?;
    let rows = sqlx::query(
        "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, \
                CAST(DATA_TYPE AS CHAR) AS DATA_TYPE, \
                CAST(IS_NULLABLE AS CHAR) AS IS_NULLABLE \
         FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = ? \
         ORDER BY ORDINAL_POSITION",
    )
    .bind(table)
    .fetch_all(&pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ColMeta {
                name: row.try_get("COLUMN_NAME")?,
                data_type: row.try_get("DATA_TYPE")?,
                nullable: row.try_get::<String, _>("IS_NULLABLE")? == "YES",
            })
        })
        .collect()
}
