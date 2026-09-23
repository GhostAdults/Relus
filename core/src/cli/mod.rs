use crate::job_config_loader;
use crate::{init_and_watch_config, run_scheduler, run_serve};
use anyhow::Context;
use relus_cli::Commands;
use relus_connector_rdbms::connector::RdbmsConnector;
use relus_connector_rdbms::metadata::logical_type;
use relus_engine::engine::contracts::{ExecutionOptions, RunResult, RunStatus};
use relus_engine::progress::IndicatifProgress;
use relus_reader::rdbms_reader_util::util::client_tool::{extract_by_path, fetch_json};
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

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
            let result = crate::starter::start_job_with_options(
                job_config_loader::load(&config)?,
                ExecutionOptions::default().with_progress(Arc::new(IndicatifProgress::new())),
            )
            .await?;
            print_run_result(&result);
        }
        Commands::ListTables { db_url, db_type } => {
            let connector = RdbmsConnector::connect_with_kind(&db_url, db_type).await?;
            for table in connector.list_tables().await? {
                println!("{table}");
            }
        }
        Commands::DescribeTable {
            db_url,
            db_type,
            table,
        } => {
            relus_connector_rdbms::identifier::validate(&table)?;
            let connector = RdbmsConnector::connect_with_kind(&db_url, db_type).await?;
            let columns = connector.describe(&table).await?;
            println!("{}", serde_json::to_string_pretty(&columns)?);
        }
        Commands::GenMapping {
            db_url,
            db_type,
            table,
            output,
        } => {
            relus_connector_rdbms::identifier::validate(&table)?;
            let connector = RdbmsConnector::connect_with_kind(&db_url, db_type).await?;
            let columns = connector.describe(&table).await?;
            let mut mapping = BTreeMap::new();
            let mut types = BTreeMap::new();
            for column in columns {
                mapping.insert(column.name.clone(), String::new());
                types.insert(column.name, logical_type(&column.data_type).to_string());
            }
            let value = json!({"table": table, "key_columns": Vec::<String>::new(), "column_mapping": mapping, "column_types": types});
            fs::write(&output, serde_json::to_string_pretty(&value)?)
                .with_context(|| format!("写入 mapping 文件失败: {}", output.display()))?;
            println!("written {}", output.display());
        }
        Commands::SyncWithMapping { config } => {
            init_and_watch_config();
            let result = crate::starter::start_job_with_options(
                job_config_loader::load(&config)?,
                ExecutionOptions::default().with_progress(Arc::new(IndicatifProgress::new())),
            )
            .await?;
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
            run_scheduler(
                job_config_loader::collect(config, jobs_dir)?,
                !no_repl,
                host,
                port,
            )
            .await?;
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
