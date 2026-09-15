use relus_common::JobConfig;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) enum JobConfigLoadError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: anyhow::Error,
    },
}

impl fmt::Display for JobConfigLoadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, .. } => {
                write!(formatter, "读取配置文件失败: {}", path.display())
            }
            Self::Parse { path, .. } => {
                write!(formatter, "配置文件解析失败: {}", path.display())
            }
        }
    }
}

impl std::error::Error for JobConfigLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source.as_ref()),
        }
    }
}

pub(crate) fn load(path: &Path) -> Result<JobConfig, JobConfigLoadError> {
    let data = std::fs::read_to_string(path).map_err(|source| JobConfigLoadError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    JobConfig::parse_json(&data).map_err(|source| JobConfigLoadError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn loads_compatible_job_config_from_file() {
        let mut file = tempfile::NamedTempFile::new().expect("temporary config file");
        write!(
            file,
            "{}",
            serde_json::json!({
                "input":{"name":"source","type":"api","config":{}},
                "output":{"name":"sink","type":"api","config":{}},
                "column_mapping":{},
                "column_types":null
            })
        )
        .expect("write config");

        let config = load(file.path()).expect("load config");

        assert_eq!(config.source.name, "source");
        assert_eq!(config.sink.name, "sink");
    }

    #[test]
    fn read_error_preserves_cli_message_and_structured_source() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("missing.json");

        let error = load(&path).expect_err("missing file must fail");

        assert!(matches!(error, JobConfigLoadError::Read { .. }));
        assert_eq!(
            error.to_string(),
            format!("读取配置文件失败: {}", path.display())
        );
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn parse_error_preserves_cli_message_and_structured_source() {
        let mut file = tempfile::NamedTempFile::new().expect("temporary config file");
        write!(file, "{{").expect("write malformed config");

        let error = load(file.path()).expect_err("malformed JSON must fail");

        assert!(matches!(error, JobConfigLoadError::Parse { .. }));
        assert_eq!(
            error.to_string(),
            format!("配置文件解析失败: {}", file.path().display())
        );
        assert!(std::error::Error::source(&error).is_some());
    }
}
