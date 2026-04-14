use crate::metadata::{ColMeta, TableMeta};
use crate::pool::RdbmsPool;
use anyhow::Result;
use sqlx::{MySqlPool, PgPool, Row as _};
use tracing::info;

/// RDBMS 通用操作：表发现、列探测、批量查询
pub struct RdbmsConnector {
    pool: RdbmsPool,
}

impl RdbmsConnector {
    pub fn new(pool: RdbmsPool) -> Self {
        info!("[RdbmsConnector] 已连接 {}", pool.db_type());
        Self { pool }
    }

    pub async fn connect(url: &str) -> Result<Self> {
        let pool = RdbmsPool::from_url(url).await?;
        Ok(Self::new(pool))
    }

    pub fn db_type(&self) -> &str {
        self.pool.db_type()
    }

    /// 列出用户表
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        match &self.pool {
            RdbmsPool::Postgres(p) => list_tables_postgres(p).await,
            RdbmsPool::Mysql(p) => list_tables_mysql(p).await,
        }
    }

    /// 探测表结构
    pub async fn describe(&self, table: &str) -> Result<Vec<ColMeta>> {
        match &self.pool {
            RdbmsPool::Postgres(p) => fetch_columns_postgres(p, table).await,
            RdbmsPool::Mysql(p) => fetch_columns_mysql(p, table).await,
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

async fn list_tables_postgres(pool: &PgPool) -> Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT tablename FROM pg_catalog.pg_tables \
         WHERE schemaname NOT IN ('pg_catalog','information_schema')",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|r| r.get("tablename")).collect())
}

async fn list_tables_mysql(pool: &MySqlPool) -> Result<Vec<String>> {
    let rows = sqlx::query("SHOW TABLES").fetch_all(pool).await?;
    Ok(rows.iter().map(|r| r.get::<String, _>(0)).collect())
}

async fn fetch_columns_postgres(pool: &PgPool, table: &str) -> Result<Vec<ColMeta>> {
    let rows = sqlx::query(
        "SELECT column_name, data_type, is_nullable \
         FROM information_schema.columns \
         WHERE table_schema='public' AND table_name=$1 \
         ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| ColMeta {
            name: r.get("column_name"),
            data_type: r.get("data_type"),
            nullable: r.get::<String, _>("is_nullable") == "YES",
        })
        .collect())
}

async fn fetch_columns_mysql(pool: &MySqlPool, table: &str) -> Result<Vec<ColMeta>> {
    let rows = sqlx::query(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE \
         FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = ? \
         ORDER BY ORDINAL_POSITION",
    )
    .bind(table)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .map(|r| ColMeta {
            name: r.get("COLUMN_NAME"),
            data_type: r.get("DATA_TYPE"),
            nullable: r.get::<String, _>("IS_NULLABLE") == "YES",
        })
        .collect())
}
