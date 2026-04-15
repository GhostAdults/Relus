pub mod api_reader;
pub mod database_reader;
pub mod rdbms_reader_util;

pub use api_reader::{ApiJob, ApiReader};
pub use database_reader::{DatabaseJob, DatabaseReader};
pub use rdbms_reader_util::rdbms_reader::{
    count_total_records, execute_query_stream, DbRowStream, RdbmsConfig, RdbmsReader,
};
pub use relus_common::interface;
pub use relus_common::types::SourceType;

/// 注册本 crate 提供的所有 Reader 类型
pub fn register(registry: &interface::GlobalRegistry) {
    registry.register_reader(SourceType::API.as_str(), |config| {
        let reader = ApiReader::init(config)?;
        Ok(Box::new(reader))
    });

    registry.register_reader(SourceType::Database.as_str(), |config| {
        let reader = DatabaseReader::init(config)?;
        Ok(Box::new(reader))
    });
}

inventory::submit! {
    relus_common::interface::registry::ReaderPlugin {
        register: register,
    }
}
