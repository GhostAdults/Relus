use crate::job_config_loader;
use crate::starter::start_job;
use crate::{init_and_watch_config, run_scheduler, run_serve};
use anyhow::{Context, Result};
use relus_cli::Commands;
use relus_common::JobConfig;
use relus_connector_rdbms::connector::{list_tables_mysql, list_tables_postgres};
use relus_connector_rdbms::pool::{detect_database_kind, get_db_pool, DatabaseKind};
use relus_engine::engine::contracts::{RunResult, RunStatus};
use relus_reader::rdbms_reader_util::util::client_tool::{extract_by_path, fetch_json};
use serde_json::json;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::{MySqlPool, PgPool, Row};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Executes one parsed CLI command.
pub async fn run(cmd: Commands) -> anyhow::Result<()> {
    match cmd {
        Commands::TestApi { config } => {
            let cfg = job_config_loader::load(&config)?;
            let api_config = cfg.source.parse_api_config()?;
            let value = fetch_json(&api_config).await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
            let items = api_config
                .items_json_path
                .as_deref()
                .and_then(|path| extract_by_path(&value, path))
                .cloned()
                .unwrap_or_else(|| value.clone());
            println!("{}", serde_json::to_string_pretty(&items)?);
        }
        Commands::Sync { config } => {
            init_and_watch_config();
            let result = start_job(job_config_loader::load(&config)?).await?;
            print_run_result(&result);
        }
        Commands::ListTables { db_url, db_type } => {
            let kind = detect_database_kind(&db_url, db_type)?;
            let (pool_key, _) = get_db_pool(&db_url, kind, 5, None, None).await?;
            match kind {
                DatabaseKind::Postgres => {
                    for table in list_tables_postgres(&pool_key).await? {
                        println!("{table}");
                    }
                }
                DatabaseKind::Mysql => {
                    for table in list_tables_mysql(&pool_key).await? {
                        println!("{table}");
                    }
                }
            }
        }
        Commands::DescribeTable {
            db_url,
            db_type,
            table,
        } => {
            relus_connector_rdbms::identifier::validate(&table)?;
            let kind = detect_database_kind(&db_url, db_type)?;
            let columns = match kind {
                DatabaseKind::Postgres => {
                    let pool = PgPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_postgres(&pool, &table).await?
                }
                DatabaseKind::Mysql => {
                    let pool = MySqlPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_mysql(&pool, &table).await?
                }
            };
            println!("{}", serde_json::to_string_pretty(&columns)?);
        }
        Commands::GenMapping {
            db_url,
            db_type,
            table,
            output,
        } => {
            relus_connector_rdbms::identifier::validate(&table)?;
            let kind = detect_database_kind(&db_url, db_type)?;
            let columns = match kind {
                DatabaseKind::Postgres => {
                    let pool = PgPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_postgres(&pool, &table).await?
                }
                DatabaseKind::Mysql => {
                    let pool = MySqlPoolOptions::new()
                        .max_connections(5)
                        .connect(&db_url)
                        .await?;
                    fetch_columns_mysql(&pool, &table).await?
                }
            };
            let mut mapping = BTreeMap::new();
            let mut types = BTreeMap::new();
            for column in columns {
                mapping.insert(column.name.clone(), String::new());
                types.insert(column.name, guess_type(&column.data_type).to_string());
            }
            let value = json!({"table": table, "key_columns": Vec::<String>::new(), "column_mapping": mapping, "column_types": types});
            fs::write(&output, serde_json::to_string_pretty(&value)?)
                .with_context(|| format!("写入 mapping 文件失败: {}", output.display()))?;
            println!("written {}", output.display());
        }
        Commands::SyncWithMapping { config } => {
            init_and_watch_config();
            let result = start_job(job_config_loader::load(&config)?).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Commands::Serve { host, port } => {
            init_and_watch_config();
            println!("Server listening on {host}:{port}");
            run_serve(host, port).await?;
        }
        Commands::Run {
            config,
            jobs_dir,
            host,
            port,
            no_repl,
        } => {
            init_and_watch_config();
            run_scheduler(collect_job_configs(config, jobs_dir)?, !no_repl, host, port).await?;
        }
    }
    Ok(())
}

fn print_run_result(result: &RunResult) {
    if result.status == RunStatus::Shutdown {
        println!(
            "Stream 任务已停止\n 已读出 {} records\n 已写入 {} records\n 耗时 {:.2}s",
            result.stats.records_read, result.stats.records_written, result.stats.elapsed_secs
        );
    } else {
        println!(
            "complete:\n 任务读出 {} records\n 任务写入 {} records\n 耗时 {:.2}s\n TP {:.0} rec/s",
            result.stats.records_read,
            result.stats.records_written,
            result.stats.elapsed_secs,
            result.stats.throughput
        );
    }
}

#[derive(serde::Serialize)]
struct ColInfo {
    name: String,
    data_type: String,
    nullable: bool,
}

async fn fetch_columns_postgres(pool: &PgPool, table: &str) -> Result<Vec<ColInfo>> {
    let rows = sqlx::query("select column_name, data_type, is_nullable from information_schema.columns where table_schema='public' and table_name=$1 order by ordinal_position")
        .bind(table).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            Ok(ColInfo {
                name: row.try_get("column_name")?,
                data_type: row.try_get("data_type")?,
                nullable: row.try_get::<String, _>("is_nullable")? == "YES",
            })
        })
        .collect()
}

async fn fetch_columns_mysql(pool: &MySqlPool, table: &str) -> Result<Vec<ColInfo>> {
    let rows = sqlx::query("select COLUMN_NAME, DATA_TYPE, IS_NULLABLE from information_schema.columns where table_schema = database() and table_name = ? order by ORDINAL_POSITION")
        .bind(table).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            Ok(ColInfo {
                name: row.try_get("COLUMN_NAME")?,
                data_type: row.try_get("DATA_TYPE")?,
                nullable: row.try_get::<String, _>("IS_NULLABLE")? == "YES",
            })
        })
        .collect()
}

fn guess_type(sql_type: &str) -> &'static str {
    let ty = sql_type.to_ascii_lowercase();
    if ty.contains("int") {
        "int"
    } else if ty.contains("numeric")
        || ty.contains("decimal")
        || ty.contains("double")
        || ty.contains("real")
        || ty.contains("float")
    {
        "float"
    } else if ty.contains("bool") {
        "bool"
    } else if ty.contains("timestamp") || ty == "datetime" || ty == "date" || ty == "time" {
        "timestamp"
    } else {
        "text"
    }
}

fn collect_job_configs(
    config_paths: Option<Vec<PathBuf>>,
    jobs_dir: Option<PathBuf>,
) -> Result<Vec<(String, JobConfig)>> {
    let mut configs = Vec::new();
    if let Some(paths) = config_paths {
        for path in paths {
            match load_job_config(&path) {
                Ok(config) => configs.push(config),
                Err(error) => eprintln!("Failed to load {}: {error}", path.display()),
            }
        }
    }
    if let Some(dir) = jobs_dir {
        for entry in
            fs::read_dir(&dir).with_context(|| format!("读取任务目录失败: {}", dir.display()))?
        {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                match load_job_config(&path) {
                    Ok(config) => configs.push(config),
                    Err(error) => eprintln!("Failed to load {}: {error}", path.display()),
                }
            }
        }
    }
    Ok(configs)
}

fn load_job_config(path: &Path) -> Result<(String, JobConfig)> {
    let config = job_config_loader::load(path)?;
    let job_id = config
        .job_id
        .clone()
        .or_else(|| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_owned)
        })
        .ok_or_else(|| anyhow::anyhow!("job_id is required (in config or from filename)"))?;
    Ok((job_id, config))
}
