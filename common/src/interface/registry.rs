//! 数据源注册表
//!
//! 使用 `inventory` 实现编译期自注册。各 Reader/Writer crate 通过 `inventory::submit!`
//! 提交注册函数，core 通过 `collect_and_register()` 一次性收集，无需硬编码依赖。

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::interface::{Reader, Writer};
use crate::job_config::JobConfig;
use anyhow::Result;

/// Reader 创建函数类型
pub type ReaderCreator = fn(Arc<JobConfig>) -> Result<Box<dyn Reader>>;

/// Writer 创建函数类型
pub type WriterCreator = fn(Arc<JobConfig>) -> Result<Box<dyn Writer>>;

/// Reader 插件（由各 reader crate 通过 `inventory::submit!` 注册）
pub struct ReaderPlugin {
    pub register: fn(&GlobalRegistry),
}

inventory::collect!(ReaderPlugin);

/// Writer 插件
pub struct WriterPlugin {
    pub register: fn(&GlobalRegistry),
}

inventory::collect!(WriterPlugin);

/// 全局注册表
pub struct GlobalRegistry {
    readers: RwLock<HashMap<String, ReaderCreator>>,
    writers: RwLock<HashMap<String, WriterCreator>>,
}

impl GlobalRegistry {
    /// 获取全局单例
    pub fn instance() -> &'static Self {
        static INSTANCE: OnceLock<GlobalRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| GlobalRegistry {
            readers: RwLock::new(HashMap::new()),
            writers: RwLock::new(HashMap::new()),
        })
    }

    /// 从 inventory 收集所有已链接的 Reader/Writer 插件并注册
    pub fn collect_and_register() {
        for plugin in inventory::iter::<ReaderPlugin> {
            (plugin.register)(Self::instance());
        }
        for plugin in inventory::iter::<WriterPlugin> {
            (plugin.register)(Self::instance());
        }
    }

    /// 注册 Reader 创建器
    pub fn register_reader(&self, source_type: &str, creator: ReaderCreator) {
        self.readers
            .write()
            .unwrap()
            .insert(source_type.to_string(), creator);
    }

    /// 注册 Writer 创建器
    pub fn register_writer(&self, source_type: &str, creator: WriterCreator) {
        self.writers
            .write()
            .unwrap()
            .insert(source_type.to_string(), creator);
    }

    /// 创建 Reader 实例
    pub fn prepare_reader(
        &self,
        source_type: &str,
        config: Arc<JobConfig>,
    ) -> Result<Box<dyn Reader>> {
        let readers = self.readers.read().unwrap();
        let creator = readers.get(source_type).ok_or_else(|| {
            anyhow::anyhow!(
                "未找到 Reader 类型: '{}'. 已注册: {:?}",
                source_type,
                readers.keys().collect::<Vec<_>>()
            )
        })?;
        creator(config)
    }

    /// 创建 Writer 实例
    pub fn prepare_writer(
        &self,
        source_type: &str,
        config: Arc<JobConfig>,
    ) -> Result<Box<dyn Writer>> {
        let writers = self.writers.read().unwrap();
        let creator = writers.get(source_type).ok_or_else(|| {
            anyhow::anyhow!(
                "未找到 Writer 类型: '{}'. 已注册: {:?}",
                source_type,
                writers.keys().collect::<Vec<_>>()
            )
        })?;
        creator(config)
    }

    /// 列出所有已注册的 Reader 类型
    pub fn list_readers(&self) -> Vec<String> {
        self.readers.read().unwrap().keys().cloned().collect()
    }

    /// 列出所有已注册的 Writer 类型
    pub fn list_writers(&self) -> Vec<String> {
        self.writers.read().unwrap().keys().cloned().collect()
    }
}
