use super::cmd::{Schedule, TaskInfo};
use relus_common::job_config::JobConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub type SchedulerResult = Result<SchedulerResponse, SchedulerError>;

#[derive(Debug)]
pub enum SchedulerCommand {
    QueryTasks {
        job_id: Option<String>,
        reply: oneshot::Sender<SchedulerResult>,
    },
    SubmitTask {
        job_id: Option<String>,
        path: PathBuf,
        reply: oneshot::Sender<SchedulerResult>,
    },
    CancelTask {
        job_id: String,
        reply: oneshot::Sender<SchedulerResult>,
    },
    Shutdown {
        reply: oneshot::Sender<SchedulerResult>,
    },
}

#[derive(Debug, Clone)]
pub struct SchedulerControlHandle {
    tx: mpsc::Sender<SchedulerCommand>,
}

impl SchedulerControlHandle {
    #[cfg(test)]
    pub fn from_sender(tx: mpsc::Sender<SchedulerCommand>) -> Self {
        Self { tx }
    }

    pub fn new(tx: mpsc::Sender<SchedulerCommand>) -> Self {
        Self { tx }
    }

    pub async fn query_tasks(
        &self,
        job_id: Option<String>,
    ) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::QueryTasks { job_id, reply })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }

    pub async fn submit_task(&self, path: PathBuf) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::SubmitTask {
                job_id: None,
                path,
                reply,
            })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }

    pub async fn cancel_task(&self, job_id: String) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::CancelTask { job_id, reply })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }

    pub async fn shutdown(&self) -> Result<SchedulerResponse, SchedulerError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(SchedulerCommand::Shutdown { reply })
            .await
            .map_err(|_| SchedulerError::SchedulerUnavailable)?;
        rx.await.map_err(|_| SchedulerError::SchedulerUnavailable)?
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchedulerResponse {
    Tasks {
        tasks: Vec<TaskInfo>,
        repl_alive: bool,
    },
    TaskSubmitted {
        job_id: String,
    },
    TaskCancelled {
        job_id: String,
    },
    ShutdownRequested,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchedulerError {
    JobNotFound { job_id: String },
    JobAlreadyExists { job_id: String },
    InvalidConfig { message: String },
    MaxConcurrencyReached { running: usize, limit: usize },
    SchedulerUnavailable,
    Internal { message: String },
}

impl SchedulerError {
    pub fn message(&self) -> String {
        match self {
            SchedulerError::JobNotFound { job_id } => format!("Job '{}' not found.", job_id),
            SchedulerError::JobAlreadyExists { job_id } => {
                format!(
                    "Job '{}' is already active. Cancel it before submitting the same job again.",
                    job_id
                )
            }
            SchedulerError::InvalidConfig { message } => message.clone(),
            SchedulerError::MaxConcurrencyReached { running, limit } => {
                format!("Max concurrency reached ({}/{}).", running, limit)
            }
            SchedulerError::SchedulerUnavailable => "Scheduler is unavailable.".to_string(),
            SchedulerError::Internal { message } => message.clone(),
        }
    }
}

impl std::fmt::Display for SchedulerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for SchedulerError {}

pub fn load_job_config_from_path(
    path: &Path,
) -> Result<(String, Arc<JobConfig>, Schedule), SchedulerError> {
    let config = match crate::job_config_loader::load(path) {
        Ok(config) => config,
        Err(crate::job_config_loader::JobConfigLoadError::Read { source, .. }) => {
            return Err(SchedulerError::InvalidConfig {
                message: format!("File not found: {} ({})", path.display(), source),
            });
        }
        Err(crate::job_config_loader::JobConfigLoadError::Parse { source, .. }) => {
            return Err(SchedulerError::InvalidConfig {
                message: format!("Parse failed: {}", source),
            });
        }
    };

    let job_id = config
        .job_id
        .clone()
        .or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

    let schedule = config
        .schedule
        .as_ref()
        .map(Schedule::from_config)
        .transpose()
        .map_err(|message| SchedulerError::InvalidConfig { message })?
        .unwrap_or_default();

    Ok((job_id, Arc::new(config), schedule))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn scheduler_error_messages_are_structured() {
        assert_eq!(
            SchedulerError::JobNotFound {
                job_id: "job-1".to_string(),
            }
            .message(),
            "Job 'job-1' not found."
        );
        assert_eq!(
            SchedulerError::MaxConcurrencyReached {
                running: 3,
                limit: 3,
            }
            .message(),
            "Max concurrency reached (3/3)."
        );
    }

    #[test]
    fn scheduler_loader_uses_job_config_compatibility_parser() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(
            file,
            "{}",
            serde_json::json!({
                "input":{"name":"source","type":"api","config":{}},
                "output":{"name":"sink","type":"database","config":{}},
                "column_mapping":{},
                "column_types":null
            })
        )
        .unwrap();

        let (_, config, _) = load_job_config_from_path(&file.path().to_path_buf()).unwrap();
        assert_eq!(config.source.name, "source");
        assert_eq!(config.sink.name, "sink");
    }
}
