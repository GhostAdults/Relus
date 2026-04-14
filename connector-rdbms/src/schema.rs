//! Schema Registry 模块
//!
//! 提供动态元数据发现、Schema 缓存和演进检查功能

mod discoverer;
mod evolution;
mod rdbms_discoverer;
mod schema_cache;
mod types;

pub use discoverer::*;
pub use evolution::*;
pub use rdbms_discoverer::*;
pub use schema_cache::*;
pub use types::*;
