use std::{env, time::Duration};

use serde::Serialize;

#[derive(Debug)]
pub struct BenchmarkConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub table: String,
    pub binlog_file: String,
    pub binlog_position: u64,
    pub server_id: u32,
    pub expected_rows: u64,
    pub timeout: Duration,
}

impl BenchmarkConfig {
    pub fn from_environment() -> Result<Self, String> {
        Ok(Self {
            host: required("RELUS_BENCH_HOST")?,
            port: parsed("RELUS_BENCH_PORT")?,
            username: required("RELUS_BENCH_USERNAME")?,
            password: required("RELUS_BENCH_PASSWORD")?,
            database: required("RELUS_BENCH_DATABASE")?,
            table: required("RELUS_BENCH_TABLE")?,
            binlog_file: required("RELUS_BENCH_BINLOG_FILE")?,
            binlog_position: parsed("RELUS_BENCH_BINLOG_POSITION")?,
            server_id: parsed("RELUS_BENCH_SERVER_ID")?,
            expected_rows: parsed("RELUS_BENCH_EXPECTED_ROWS")?,
            timeout: Duration::from_secs(parsed("RELUS_BENCH_TIMEOUT_SECONDS")?),
        })
    }

    pub fn qualified_table(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub implementation: &'static str,
    pub rows: u64,
    pub output_bytes: u64,
    pub elapsed_seconds: f64,
    pub rows_per_second: f64,
}

impl BenchmarkResult {
    pub fn new(
        implementation: &'static str,
        rows: u64,
        output_bytes: u64,
        elapsed: Duration,
    ) -> Self {
        let elapsed_seconds = elapsed.as_secs_f64();
        Self {
            implementation,
            rows,
            output_bytes,
            elapsed_seconds,
            rows_per_second: rows as f64 / elapsed_seconds,
        }
    }
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required environment variable {name}"))
}

fn parsed<T>(name: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = required(name)?;
    value
        .parse()
        .map_err(|error| format!("invalid {name}: {error}"))
}
