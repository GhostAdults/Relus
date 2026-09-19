use relus_common::job_config::JobConfig;
use relus_engine::engine::contracts::RunResult;

/// Submit and run a synchronization job immediately.
/// The command only deserializes the frontend payload and dispatches it to
/// the existing core service; job execution remains in `relus_services`.
#[tauri::command]
pub async fn start_job(job_config: JobConfig) -> Result<RunResult, String> {
    relus_core::relus_starter::start_job(job_config)
        .await
        .map_err(|error| error.to_string())
}
