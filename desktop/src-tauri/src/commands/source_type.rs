use relus_common::types::SourceType;
use serde::Serialize;

/// 前端下拉选择使用的数据源类型选项。
#[derive(Serialize)]
pub struct SourceTypeOption {
    value: String,
    category: String,
}

/// 列出可供任务配置选择的数据源类型。
#[tauri::command]
pub async fn list_source_types() -> Vec<SourceTypeOption> {
    SourceType::NAMED
        .iter()
        .map(|source_type| SourceTypeOption {
            value: source_type.as_str().to_string(),
            category: source_type.category().to_string(),
        })
        .collect()
}
