use anyhow::{bail, Result};
use relus_common::DbConfig;

pub fn validate(identifier: &str) -> Result<()> {
    if identifier.is_empty()
        || !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        bail!("标识符仅允许字母、数字和下划线: {}", identifier);
    }
    Ok(())
}

pub fn validate_database_config(config: &DbConfig) -> Result<()> {
    if config.table.is_empty() {
        bail!("数据库配置必须包含 table 字段");
    }
    validate(&config.table)?;
    if let Some(key_columns) = &config.key_columns {
        for key in key_columns {
            validate(key)?;
        }
    }
    Ok(())
}
