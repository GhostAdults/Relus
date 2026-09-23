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

/// Maps a database-native type name to the logical type used by mapping files.
pub fn logical_type(sql_type: &str) -> &'static str {
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
