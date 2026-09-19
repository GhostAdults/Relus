use relus_api::server::StatusCode;
use relus_common::app_config::config_loader::flatten;
use relus_common::app_config::value::ConfigValue;
use relus_common::job_config::JobConfig;
use relus_common::resp::ApiResp;
use relus_common::{CreateConfigReq, UpdateConfigReq};
use relus_reader::StreamMode;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::core::engine::{
    contracts::{JobHandle, RunResult},
    coordinator::CoordinatorService,
};
use crate::core::planner::{Planner, PlanningDependencies, RegistryPlanningDependencies};

/// Prepares and synchronously waits for one synchronization job.
pub async fn start_task(
    config: Arc<JobConfig>,
    cancel_token: CancellationToken,
) -> anyhow::Result<RunResult> {
    start_task_with_coordinator(config, cancel_token, crate::application_coordinator()).await
}

pub(crate) async fn start_task_with_coordinator(
    config: Arc<JobConfig>,
    cancel_token: CancellationToken,
    coordinator: Arc<CoordinatorService>,
) -> anyhow::Result<RunResult> {
    start_task_with(
        config,
        cancel_token,
        &RegistryPlanningDependencies,
        coordinator,
    )
    .await
}

async fn start_task_with(
    config: Arc<JobConfig>,
    cancel_token: CancellationToken,
    planning: &dyn PlanningDependencies,
    coordinator: Arc<CoordinatorService>,
) -> anyhow::Result<RunResult> {
    let plan = Planner::prepare_with(config, planning).await?;
    let (handle, stream_mode) = submit_prepared_job(plan, coordinator)?;

    wait_for_run_result(handle, stream_mode, cancel_token).await
}

async fn wait_for_run_result(
    handle: JobHandle,
    stream_mode: StreamMode,
    cancel_token: CancellationToken,
) -> anyhow::Result<RunResult> {
    let result = tokio::select! {
        result = handle.wait() => result.map_err(anyhow::Error::msg)?,
        _ = cancel_token.cancelled() => {
            handle.cancel();
            handle.wait().await.map_err(anyhow::Error::msg)?
        }
    };
    Ok(RunResult::from_engine(result, stream_mode))
}

pub(crate) async fn submit_job_with(
    config: Arc<JobConfig>,
    planning: &dyn PlanningDependencies,
    coordinator: Arc<CoordinatorService>,
) -> anyhow::Result<JobHandle> {
    let plan = Planner::prepare_with(config, planning).await?;
    let (handle, _) = submit_prepared_job(plan, coordinator)?;
    Ok(handle)
}

fn submit_prepared_job(
    plan: crate::core::planner::ExecutionPlan,
    coordinator: Arc<CoordinatorService>,
) -> anyhow::Result<(JobHandle, StreamMode)> {
    let stream_mode = plan.stream_mode;

    info!(
        "[start_task] {} 模式, {} 个任务, 总记录数 {}",
        match stream_mode {
            StreamMode::Batch => "Batch",
            StreamMode::Streaming => "Streaming",
        },
        plan.reader_split.tasks.len(),
        plan.reader_split.total_records
    );

    Ok((coordinator.submit_job(plan)?, stream_mode))
}

/// Plans and submits a job to the shared application Engine without waiting.
pub async fn submit_job(config: Arc<JobConfig>) -> anyhow::Result<JobHandle> {
    submit_job_with(
        config,
        &RegistryPlanningDependencies,
        crate::application_coordinator(),
    )
    .await
}

pub fn job_handle(id: crate::core::engine::state::JobId) -> Option<JobHandle> {
    crate::application_coordinator().job_handle(id)
}

/// Starts an immediate task.
///
/// Scheduler callers that need a custom cancellation token should call
/// [`start_task`] directly.
pub async fn start_job(cfg: JobConfig) -> anyhow::Result<RunResult> {
    start_task(Arc::new(cfg), CancellationToken::new()).await
}

pub async fn create_config(req: CreateConfigReq) -> (StatusCode, ApiResp<Value>) {
    if let Some(mgr_arc) = crate::get_config_manager() {
        let mut mgr = mgr_arc.write();

        let mut config = match default_system_config() {
            Ok(config) => config,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiResp {
                        ok: false,
                        data: None,
                        error: Some(e.to_string()),
                    },
                );
            }
        };

        if !req.config.is_object() {
            return (
                StatusCode::BAD_REQUEST,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some("config must be a JSON object".to_string()),
                },
            );
        }

        merge_json_object(&mut config, &req.config);
        let mut user = HashMap::new();
        flatten("", &ConfigValue::from(&config), &mut user);
        mgr.user = user;
        mgr.build();

        match mgr.persist() {
            Ok(_) => (
                StatusCode::OK,
                ApiResp {
                    ok: true,
                    data: Some(config),
                    error: None,
                },
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResp {
                ok: false,
                data: None,
                error: Some("ConfigManager not initialized".to_string()),
            },
        )
    }
}

fn default_system_config() -> anyhow::Result<Value> {
    let content = include_str!("../../../cli/user_config/default.config.json");
    Ok(serde_json::from_str(content)?)
}

pub async fn update_config(req: UpdateConfigReq) -> (StatusCode, ApiResp<Value>) {
    if let Some(mgr_arc) = crate::get_config_manager() {
        let mut mgr = mgr_arc.write();

        if !req.updates.is_object() {
            return (
                StatusCode::BAD_REQUEST,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some("updates must be a JSON object".to_string()),
                },
            );
        }

        let mut config = relus_common::app_config::config_loader::unflatten(&mgr.user)
            .unwrap_or_else(|_| serde_json::json!({}));
        merge_json_object(&mut config, &req.updates);

        let mut user = HashMap::new();
        flatten("", &ConfigValue::from(&config), &mut user);
        mgr.user = user;
        mgr.build();

        match mgr.persist() {
            Ok(_) => (
                StatusCode::OK,
                ApiResp {
                    ok: true,
                    data: Some(config),
                    error: None,
                },
            ),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiResp {
                    ok: false,
                    data: None,
                    error: Some(e.to_string()),
                },
            ),
        }
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiResp {
                ok: false,
                data: None,
                error: Some("ConfigManager not initialized".to_string()),
            },
        )
    }
}

fn merge_json_object(target: &mut Value, updates: &Value) {
    let (Some(target_map), Some(update_map)) = (target.as_object_mut(), updates.as_object()) else {
        return;
    };

    for (key, value) in update_map {
        if value.is_null() {
            target_map.remove(key);
            continue;
        }

        match (target_map.get_mut(key), value) {
            (Some(target_value), Value::Object(_)) if target_value.is_object() => {
                merge_json_object(target_value, value);
            }
            _ => {
                target_map.insert(key.clone(), value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use relus_common::{job_config::WriteMode, PipelineMessage};
    use relus_reader::{DataReaderJob, DataReaderTask, JsonStream, ReadTask, SplitReaderResult};
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult, WriteTask};
    use tokio::sync::mpsc;

    const JOB: &str = r#"{
        "source":{"name":"source","type":"fake","config":{}},
        "target":{"name":"sink","type":"fake","config":{}},
        "column_mapping":{},"batch_size":1,"channel_buffer_size":1
    }"#;

    struct FakeReader {
        mode: StreamMode,
        pending: bool,
    }

    #[async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            Ok(SplitReaderResult {
                total_records: usize::from(self.pending),
                tasks: self.pending.then(read_task).into_iter().collect(),
                stream_mode: self.mode,
            })
        }

        fn description(&self) -> String {
            "bootstrap fake reader".into()
        }
    }

    #[async_trait]
    impl DataReaderTask for FakeReader {
        async fn read_data(&self, _: &ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
    }

    struct FakeWriter {
        config: Arc<JobConfig>,
        pending: bool,
    }

    #[async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            Ok(SplitWriterResult {
                tasks: self
                    .pending
                    .then(|| WriteTask {
                        task_id: 0,
                        config: Arc::clone(&self.config),
                        mode: WriteMode::Insert,
                        use_transaction: false,
                        batch_size: 1,
                    })
                    .into_iter()
                    .collect(),
            })
        }

        fn description(&self) -> String {
            "bootstrap fake writer".into()
        }
    }

    #[async_trait]
    impl DataWriterTask for FakeWriter {
        async fn write_data(
            &self,
            _: WriteTask,
            mut rx: mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            while rx.recv().await.is_some() {}
            Ok(0)
        }
    }

    struct FakePlanning {
        mode: StreamMode,
        pending: bool,
    }

    impl PlanningDependencies for FakePlanning {
        fn create_reader(&self, _: Arc<JobConfig>) -> Result<relus_reader::Source> {
            Ok(Arc::new(FakeReader {
                mode: self.mode,
                pending: self.pending,
            }))
        }

        fn create_writer(&self, config: Arc<JobConfig>) -> Result<relus_writer::Sink> {
            Ok(Arc::new(FakeWriter {
                config,
                pending: self.pending,
            }))
        }

        fn build_record_builder(
            &self,
            config: &JobConfig,
        ) -> Result<crate::pipeline::RecordBuilder> {
            crate::pipeline::RecordBuilder::new(
                config.column_mapping.clone(),
                config.column_types.clone(),
            )
        }
    }

    fn read_task() -> ReadTask {
        ReadTask {
            task_id: 0,
            conn: serde_json::json!({}),
            query_sql: None,
            offset: 0,
            limit: 1,
        }
    }

    fn config() -> Arc<JobConfig> {
        Arc::new(JobConfig::parse_json(JOB).expect("valid fake job"))
    }

    #[tokio::test]
    async fn bootstrap_waits_for_batch_success() {
        let result = start_task_with(
            config(),
            CancellationToken::new(),
            &FakePlanning {
                mode: StreamMode::Batch,
                pending: false,
            },
            Arc::new(CoordinatorService::new(
                crate::core::engine::state::StateRepository::new(),
            )),
        )
        .await
        .expect("batch result");
        assert_eq!(
            result.status,
            crate::core::engine::contracts::RunStatus::Success
        );
    }

    #[tokio::test]
    async fn bootstrap_maps_streaming_natural_exit_to_failed() {
        let result = start_task_with(
            config(),
            CancellationToken::new(),
            &FakePlanning {
                mode: StreamMode::Streaming,
                pending: false,
            },
            Arc::new(CoordinatorService::new(
                crate::core::engine::state::StateRepository::new(),
            )),
        )
        .await
        .expect("stream result");
        assert_eq!(
            result.status,
            crate::core::engine::contracts::RunStatus::Failed
        );
        assert_eq!(result.error.as_deref(), Some("Stream pipeline 非预期退出"));
    }

    #[tokio::test]
    async fn bootstrap_cancels_streaming_job_and_waits_for_shutdown() {
        let token = CancellationToken::new();
        let task = tokio::spawn(start_task_with(
            config(),
            token.clone(),
            &FakePlanning {
                mode: StreamMode::Streaming,
                pending: true,
            },
            Arc::new(CoordinatorService::new(
                crate::core::engine::state::StateRepository::new(),
            )),
        ));
        tokio::task::yield_now().await;
        token.cancel();

        let result = tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("bootstrap must wait to terminal state")
            .expect("bootstrap task")
            .expect("shutdown result");
        assert_eq!(
            result.status,
            crate::core::engine::contracts::RunStatus::Shutdown
        );
    }

    #[tokio::test]
    async fn async_submit_exposes_shared_handle_snapshot_and_repeatable_result() {
        let coordinator = Arc::new(CoordinatorService::new(
            crate::core::engine::state::StateRepository::new(),
        ));
        let handle = submit_job_with(
            config(),
            &FakePlanning {
                mode: StreamMode::Batch,
                pending: false,
            },
            Arc::clone(&coordinator),
        )
        .await
        .expect("submit handle");
        let first = handle.wait().await.expect("first result");
        let shared = coordinator
            .job_handle(handle.id())
            .expect("shared application handle");
        assert_eq!(shared.snapshot(), handle.snapshot());
        assert_eq!(shared.wait().await.expect("shared result"), first);
        assert_eq!(shared.wait().await.expect("repeat result"), first);
    }

    #[tokio::test]
    async fn late_cancellation_does_not_override_completed_success() {
        let coordinator = Arc::new(CoordinatorService::new(
            crate::core::engine::state::StateRepository::new(),
        ));
        let handle = submit_job_with(
            config(),
            &FakePlanning {
                mode: StreamMode::Batch,
                pending: false,
            },
            coordinator,
        )
        .await
        .expect("submit handle");
        let _ = handle.wait().await.expect("completed result");
        let token = CancellationToken::new();
        token.cancel();
        let result = wait_for_run_result(handle, StreamMode::Batch, token)
            .await
            .expect("compatibility result");
        assert_eq!(
            result.status,
            crate::core::engine::contracts::RunStatus::Success
        );
    }
}
