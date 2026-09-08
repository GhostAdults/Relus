pub mod database_writer;
pub mod rdbms_writer_util;

pub use database_writer::{DatabaseJob, DatabaseWriter};
pub use rdbms_writer_util::rdbms_writer::{RdbmsConfig, RdbmsJob, RdbmsWriter, RowWriter};

use anyhow::{Error, Result};
use relus_common::job_config::{JobConfig, WriteMode};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::mpsc;

// ==========================================
// Writer trait 定义
// ==========================================

/// 一个 writer job 采取一对多关系 writer task
#[derive(Debug, Clone)]
pub struct WriteTask {
    pub task_id: usize,
    pub config: Arc<JobConfig>,
    pub mode: WriteMode,
    pub use_transaction: bool,
    pub batch_size: usize,
}

/// Job 切分结果
pub struct SplitWriterResult {
    pub tasks: Vec<WriteTask>,
}

/// Writer Job trait
#[async_trait::async_trait]
pub trait DataWriterJob: Send + Sync {
    async fn split(&self, writer_threads: usize) -> Result<SplitWriterResult>;
    fn description(&self) -> String;
}

/// Writer Task trait
#[async_trait::async_trait]
pub trait DataWriterTask: Send + Sync {
    async fn write_data(
        &self,
        task: WriteTask,
        rx: mpsc::Receiver<relus_common::PipelineMessage>,
    ) -> Result<usize>;
}

/// Writer = DataWriterJob + DataWriterTask
#[async_trait::async_trait]
pub trait DataWriter: DataWriterJob + DataWriterTask {}

#[async_trait::async_trait]
impl<T: DataWriterJob + DataWriterTask> DataWriter for T {}

// ==========================================
// Writer 全局注册表
// ==========================================

type WriterCreator = fn(Arc<JobConfig>) -> Result<Arc<dyn DataWriter>>;

/// Writer 插件
pub struct WriterPlugin {
    pub source_type: &'static str,
    pub create: WriterCreator,
}

inventory::collect!(WriterPlugin);

/// Writer 全局注册表
pub struct WriterRegistry {
    creators: RwLock<HashMap<String, WriterCreator>>,
}

impl WriterRegistry {
    pub fn instance() -> &'static Self {
        static INSTANCE: OnceLock<WriterRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let registry = WriterRegistry {
                creators: RwLock::new(HashMap::new()),
            };
            registry.collect_plugins();
            registry
        })
    }

    fn collect_plugins(&self) {
        let Ok(mut creators) = self.creators.write() else {
            eprintln!("Writer registry is unavailable; skipped plugin collection");
            return;
        };

        for plugin in inventory::iter::<WriterPlugin> {
            creators.insert(plugin.source_type.to_string(), plugin.create);
        }
    }

    /// 兼容旧调用方；首次调用时会触发注册表懒加载，之后不会重复收集。
    pub fn collect_and_register() {
        let _ = Self::instance();
    }

    pub fn prepare_writer(
        &self,
        source_type: &str,
        config: Arc<JobConfig>,
    ) -> Result<Arc<dyn DataWriter>,Error> {
        let creators = self
            .creators
            .read()
            .map_err(|err| anyhow::anyhow!("Writer registry lock is poisoned: {}", err))?;
        let creator = creators.get(source_type).ok_or_else(|| {
            anyhow::anyhow!(
                "未找到 Writer 类型: '{}'. 已注册: {:?}",
                source_type,
                creators.keys().collect::<Vec<_>>()
            )
        })?;
        creator(config)
    }

    pub fn list_writers(&self) -> Vec<String> {
        let Ok(creators) = self.creators.read() else {
            eprintln!("Writer registry is unavailable; cannot list writers");
            return Vec::new();
        };

        creators.keys().cloned().collect()
    }
}

// ==========================================
// 注册本 crate 的 Writer 实现
// ==========================================

inventory::submit! {
    WriterPlugin {
        source_type: "database",
        create: |config| {
            let writer = DatabaseWriter::init(config)?;
            Ok(Arc::new(writer))
        },
    }
}
