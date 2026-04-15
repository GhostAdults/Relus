pub mod database_writer;
pub mod rdbms_writer_util;

pub use database_writer::{DatabaseJob, DatabaseWriter};
pub use rdbms_writer_util::rdbms_writer::{RdbmsConfig, RdbmsJob, RdbmsWriter, RowWriter};
use relus_common::SourceType;
pub use relus_common::{interface, pipeline};

/// 注册本 crate 提供的所有 Writer 类型
pub fn register(registry: &interface::GlobalRegistry) {
    registry.register_writer(SourceType::Database.as_str(), |config| {
        let writer = DatabaseWriter::init(config)?;
        Ok(Box::new(writer))
    });
}

inventory::submit! {
    relus_common::interface::registry::WriterPlugin {
        register: register,
    }
}
