//! Command-line interface definitions.
//!
//! This crate owns only command parsing. Runtime bootstrap and command
//! execution live in the `relus_core` binary, which keeps the dependency
//! direction one-way: the executable depends on this crate, never the reverse.

use clap::{Parser, Subcommand};
use relus_connector_rdbms::pool::DatabaseKind;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "Relus CLI")]
#[command(version)]
#[command(about = "Relus 数据迁移工具 relus-cli@tingfengyu", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    TestApi {
        #[arg(short, long)]
        config: PathBuf,
    },
    Sync {
        #[arg(short, long)]
        config: PathBuf,
    },
    ListTables {
        #[arg(short = 'u', long)]
        db_url: String,
        #[arg(short = 't', long)]
        db_type: Option<DatabaseKind>,
    },
    DescribeTable {
        #[arg(short = 'u', long)]
        db_url: String,
        #[arg(short = 't', long)]
        db_type: Option<DatabaseKind>,
        #[arg(short = 'b', long)]
        table: String,
    },
    GenMapping {
        #[arg(short = 'u', long)]
        db_url: String,
        #[arg(short = 't', long)]
        db_type: Option<DatabaseKind>,
        #[arg(short = 'b', long)]
        table: String,
        #[arg(short = 'o', long)]
        output: PathBuf,
    },
    SyncWithMapping {
        #[arg(short, long)]
        config: PathBuf,
    },
    Serve {
        #[arg(short = 'H', long, default_value = "127.0.0.1")]
        host: String,
        #[arg(short = 'p', long, default_value_t = 30001)]
        port: u16,
    },
    /// 启动 TaskScheduler 常驻运行，进入交互式 REPL
    #[command(visible_alias = "start")]
    Run {
        /// 指定一个或多个配置文件
        #[arg(short, long)]
        config: Option<Vec<PathBuf>>,
        /// 指定配置文件目录，加载目录下所有 *.json
        #[arg(short, long)]
        jobs_dir: Option<PathBuf>,
        /// Scheduler HTTP 控制面监听地址；未指定时读取系统配置
        #[arg(short = 'H', long)]
        host: Option<String>,
        /// Scheduler HTTP 控制面端口；未指定时读取系统配置
        #[arg(short = 'p', long)]
        port: Option<u16>,
        /// 禁用交互式 REPL，仅保留 API 控制面
        #[arg(long, default_value_t = false)]
        no_repl: bool,
    },
}
