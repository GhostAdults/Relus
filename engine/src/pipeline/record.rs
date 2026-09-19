//! 数据记录行定义
//!
//! Record 是 MappingRow 的类型别名，作为系统内唯一数据记录类型

pub use relus_common::types::{MappingField, MappingSchema, UnifiedValue};

// Re-export MappingRow as Record for backward compatibility
pub use relus_common::types::MappingRow as Record;
