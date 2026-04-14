use serde::{Deserialize, Serialize};

/// 表元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub schema: Option<String>,
    pub name: String,
}

/// 列元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColMeta {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}
