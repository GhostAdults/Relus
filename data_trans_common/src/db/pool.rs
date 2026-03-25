//! Database connection pool management
//!
//! Provides connection pooling for PostgreSQL and MySQL with caching support.

use anyhow::Result;
use async_trait::async_trait;
use clap::ValueEnum;
use dashmap::DashMap;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use std::sync::OnceLock;
use std::time::Duration;

use super::util::{dbkind_from_opt_str, DbParams};

/// Database type enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Hash)]
pub enum DbKind {
    Postgres,
    Mysql,
}

/// Database connection pool wrapper
#[derive(Debug, Clone)]
pub enum DbPool {
    Postgres(sqlx::PgPool),
    Mysql(sqlx::MySqlPool),
}

/// Database pool configuration
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct PoolConfig {
    pub kind: DbKind,
    pub url: String,
    pub max_conns: u32,
    pub timeout_secs: Option<u64>,
}

static DB_POOLS: OnceLock<DashMap<PoolConfig, DbPool>> = OnceLock::new();

async fn create_pool(cfg: &PoolConfig) -> Result<DbPool> {
    let timeout = Duration::from_secs(cfg.timeout_secs.unwrap_or(30));
    match cfg.kind {
        DbKind::Postgres => {
            let pool = PgPoolOptions::new()
                .max_connections(cfg.max_conns)
                .acquire_timeout(timeout)
                .connect(&cfg.url)
                .await?;
            Ok(DbPool::Postgres(pool))
        }
        DbKind::Mysql => {
            let pool = MySqlPoolOptions::new()
                .max_connections(cfg.max_conns)
                .acquire_timeout(timeout)
                .connect(&cfg.url)
                .await?;
            Ok(DbPool::Mysql(pool))
        }
    }
}

/// Get or create a database connection pool
pub async fn get_db_pool(
    url: &str,
    kind: DbKind,
    max_conns: u32,
    timeout_secs: Option<u64>,
) -> Result<DbPool> {
    let pools = DB_POOLS.get_or_init(|| DashMap::new());
    let key = PoolConfig {
        kind,
        url: url.to_string(),
        max_conns,
        timeout_secs,
    };
    if let Some(pool) = pools.get(&key) {
        println!("Current DB Pools count: {}", pools.len());
        return Ok(pool.clone());
    }

    let pool = create_pool(&key).await?;
    println!("create db pool: {:?}", key);
    pools.insert(key, pool.clone());
    Ok(pool)
}

/// Detect database type from URL or explicit option
pub fn detect_db_kind(url: &str, explicit: Option<DbKind>) -> Result<DbKind> {
    if let Some(k) = explicit {
        return Ok(k);
    }
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(DbKind::Postgres)
    } else if url.starts_with("mysql://") {
        Ok(DbKind::Mysql)
    } else {
        anyhow::bail!(
            "无法识别数据库类型，需提供 db_type 或使用以 postgres:// 或 mysql:// 开头的连接串"
        )
    }
}

/// Get pool from DbParams query
pub async fn get_pool_from_query<T: DbParams>(q: &T) -> Result<DbPool> {
    let db_url = q.resolve_url()?;
    let kind = detect_db_kind(&db_url, dbkind_from_opt_str(&q.resolve_type()))?;
    get_db_pool(&db_url, kind, 5, None).await
}

/// Database executor trait for common operations
#[async_trait]
pub trait DbExecutor: Send + Sync {
    async fn fetch_string_pair(&self, sql: &str) -> Result<(Option<String>, Option<String>)>;
    async fn fetch_optional_string(&self, sql: &str) -> Result<Option<String>>;
    async fn execute(&self, sql: &str) -> Result<u64>;
}

/// PostgreSQL executor reference
pub struct PgExecutorRef {
    pool: sqlx::PgPool,
}

#[async_trait]
impl DbExecutor for PgExecutorRef {
    async fn fetch_string_pair(&self, sql: &str) -> Result<(Option<String>, Option<String>)> {
        let row: (Option<String>, Option<String>) =
            sqlx::query_as(sql).fetch_one(&self.pool).await?;
        Ok(row)
    }

    async fn fetch_optional_string(&self, sql: &str) -> Result<Option<String>> {
        let result: Option<(String,)> = sqlx::query_as(sql).fetch_optional(&self.pool).await?;
        Ok(result.map(|r| r.0))
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }
}

/// MySQL executor reference
pub struct MySqlExecutorRef {
    pool: sqlx::MySqlPool,
}

#[async_trait]
impl DbExecutor for MySqlExecutorRef {
    async fn fetch_string_pair(&self, sql: &str) -> Result<(Option<String>, Option<String>)> {
        let row: (Option<String>, Option<String>) =
            sqlx::query_as(sql).fetch_one(&self.pool).await?;
        Ok(row)
    }

    async fn fetch_optional_string(&self, sql: &str) -> Result<Option<String>> {
        let result: Option<(String,)> = sqlx::query_as(sql).fetch_optional(&self.pool).await?;
        Ok(result.map(|r| r.0))
    }

    async fn execute(&self, sql: &str) -> Result<u64> {
        let result = sqlx::query(sql).execute(&self.pool).await?;
        Ok(result.rows_affected())
    }
}

impl DbPool {
    /// Get executor for database operations
    pub fn executor(&self) -> Box<dyn DbExecutor> {
        match self {
            DbPool::Postgres(p) => Box::new(PgExecutorRef { pool: p.clone() }),
            DbPool::Mysql(p) => Box::new(MySqlExecutorRef { pool: p.clone() }),
        }
    }
}
