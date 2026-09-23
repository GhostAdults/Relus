use relus_common::JobConfig;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub enum JobConfigLoadError {
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: anyhow::Error,
    },
    MissingJobId {
        path: PathBuf,
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
            Self::MissingJobId { path } => {
                write!(
                    formatter,
                    "配置缺少 job_id 且无法从文件名推导: {}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for JobConfigLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source.as_ref()),
            Self::MissingJobId { .. } => None,
        }
    }
}

pub fn load(path: &Path) -> Result<JobConfig, JobConfigLoadError> {
    let data = std::fs::read_to_string(path).map_err(|source| JobConfigLoadError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    JobConfig::parse_json(&data).map_err(|source| JobConfigLoadError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

pub fn load_with_job_id(path: &Path) -> Result<(String, JobConfig), JobConfigLoadError> {
    let config = load(path)?;
    let job_id = config
        .job_id
        .clone()
        .or_else(|| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_owned)
        })
        .ok_or_else(|| JobConfigLoadError::MissingJobId {
            path: path.to_path_buf(),
        })?;
    Ok((job_id, config))
}

pub fn collect(
    config_paths: Option<Vec<PathBuf>>,
    jobs_dir: Option<PathBuf>,
) -> Result<Vec<(String, JobConfig)>, anyhow::Error> {
    let mut configs = Vec::new();
    if let Some(paths) = config_paths {
        for path in paths {
            match load_with_job_id(&path) {
                Ok(config) => configs.push(config),
                Err(error) => eprintln!("Failed to load {}: {error}", path.display()),
            }
        }
    }
    if let Some(dir) = jobs_dir {
        for entry in std::fs::read_dir(&dir)
            .map_err(|error| anyhow::anyhow!("读取任务目录失败: {}: {error}", dir.display()))?
        {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                match load_with_job_id(&path) {
                    Ok(config) => configs.push(config),
                    Err(error) => eprintln!("Failed to load {}: {error}", path.display()),
                }
            }
        }
    }
    Ok(configs)
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
