//! Engine-owned execution contracts shared by asynchronous entry points.

use super::state::{Job, JobId, JobState, StateRepository, Task, TaskGroup};
use parking_lot::RwLock;
use relus_common::job_config::JobConfig;
use relus_reader::StreamMode;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

/// Counts in the prepared physical topology, not OS threads or live concurrency.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProgressTopology {
    /// Number of Reader task descriptors in the physical plan.
    pub readers: usize,
    /// Number of paired Writer task descriptors.
    pub writers: usize,
    /// Number of TaskGroup Workers in this Job.
    pub workers: usize,
}

/// Job-level presentation events. Callbacks must return promptly and must not
/// wait for this Job's result: `finished` runs before the result is published.
pub trait ProgressObserver: Send + Sync {
    fn planned(&self, _topology: ProgressTopology) {}
    fn started(&self, total_records: Option<u64>);
    fn records_read(&self, delta: u64);
    fn records_sent(&self, delta: u64);
    fn finished(&self, outcome: ProgressOutcome, records_read: u64, records_written: u64);
}

/// Presentation failures must not fail synchronization or strand result waiters.
/// Kept internal so every submission (including struct literals) is protected.
pub(crate) struct SafeProgressObserver {
    inner: Arc<dyn ProgressObserver>,
    disabled: std::sync::atomic::AtomicBool,
}

impl SafeProgressObserver {
    pub(crate) fn wrap(inner: Arc<dyn ProgressObserver>) -> Arc<dyn ProgressObserver> {
        Arc::new(Self {
            inner,
            disabled: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn notify(&self, finish: bool, callback: impl FnOnce(&dyn ProgressObserver)) {
        use std::sync::atomic::Ordering;
        if !finish && self.disabled.load(Ordering::Relaxed) {
            return;
        }
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(&*self.inner)))
            .is_err()
        {
            self.disabled.store(true, Ordering::Relaxed);
            tracing::warn!("progress observer panicked; execution result is unaffected");
        }
    }
}

impl ProgressObserver for SafeProgressObserver {
    fn planned(&self, topology: ProgressTopology) {
        self.notify(false, |observer| observer.planned(topology));
    }
    fn started(&self, total_records: Option<u64>) {
        self.notify(false, |observer| observer.started(total_records));
    }
    fn records_read(&self, delta: u64) {
        self.notify(false, |observer| observer.records_read(delta));
    }
    fn records_sent(&self, delta: u64) {
        self.notify(false, |observer| observer.records_sent(delta));
    }
    fn finished(&self, outcome: ProgressOutcome, records_read: u64, records_written: u64) {
        // Always give the adapter a chance to stop ticking and release the terminal.
        self.notify(true, |observer| {
            observer.finished(outcome, records_read, records_written)
        });
    }
}

#[derive(Clone, Default)]
pub struct ExecutionOptions {
    pub progress: Option<Arc<dyn ProgressObserver>>,
}

impl ExecutionOptions {
    pub fn with_progress(mut self, observer: Arc<dyn ProgressObserver>) -> Self {
        self.progress = Some(observer);
        self
    }
}

pub struct JobSubmission {
    pub config: Arc<JobConfig>,
    pub options: ExecutionOptions,
}

impl JobSubmission {
    pub fn new(config: Arc<JobConfig>) -> Self {
        Self {
            config,
            options: ExecutionOptions::default(),
        }
    }
    pub fn with_options(mut self, options: ExecutionOptions) -> Self {
        self.options = options;
        self
    }
}

impl From<Arc<JobConfig>> for JobSubmission {
    fn from(config: Arc<JobConfig>) -> Self {
        Self::new(config)
    }
}

/// Compatibility result returned by synchronous execution entry points.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub stats: RunnerStats,
    pub status: RunStatus,
    pub duration: Duration,
    pub error: Option<String>,
}

impl RunResult {
    pub fn from_engine(result: EngineExecutionResult) -> Self {
        let mut stats = RunnerStats {
            records_read: result.records_read,
            records_written: result.records_written,
            records_failed: result.records_failed,
            elapsed_secs: result.elapsed.as_secs_f64(),
            throughput: 0.0,
        };
        stats.calculate_throughput();

        let shutdown =
            result.cancelled || matches!(result.status, EngineExecutionStatus::Cancelled);
        let failed = matches!(result.status, EngineExecutionStatus::Failed);
        let status = if shutdown {
            RunStatus::Shutdown
        } else if failed && stats.records_written == 0 {
            RunStatus::Failed
        } else if failed || stats.records_failed > 0 {
            RunStatus::Partial
        } else if result.stream_mode == StreamMode::Streaming {
            RunStatus::Failed
        } else {
            RunStatus::Success
        };
        let error = if shutdown {
            None
        } else if failed {
            result.error
        } else if status == RunStatus::Failed {
            Some("Stream pipeline 非预期退出".to_string())
        } else {
            None
        };

        Self {
            stats,
            status,
            duration: result.elapsed,
            error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    Success,
    Failed,
    Partial,
    Shutdown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunnerStats {
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub elapsed_secs: f64,
    pub throughput: f64,
}

impl RunnerStats {
    pub fn calculate_throughput(&mut self) {
        if self.elapsed_secs > 0.0 {
            self.throughput = self.records_written as f64 / self.elapsed_secs;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineExecutionStatus {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EngineExecutionResult {
    pub status: EngineExecutionStatus,
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub cancelled: bool,
    pub stream_mode: StreamMode,
    pub elapsed: Duration,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JobSnapshot {
    pub job: Job,
    pub task_groups: Vec<TaskGroup>,
    pub tasks: Vec<Task>,
    pub result: Option<EngineExecutionResult>,
    pub error: Option<String>,
}

#[derive(Clone, Default)]
pub struct EngineResultStore {
    completion: Arc<RwLock<Option<Result<EngineExecutionResult, String>>>>,
    notify: Arc<Notify>,
}

impl EngineResultStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn complete(&self, result: EngineExecutionResult) {
        let mut completion = self.completion.write();
        if completion.is_none() {
            *completion = Some(Ok(result));
            self.notify.notify_waiters();
        }
    }

    pub fn fail(&self, error: impl Into<String>) {
        let mut completion = self.completion.write();
        if completion.is_none() {
            *completion = Some(Err(error.into()));
            self.notify.notify_waiters();
        }
    }

    pub fn result(&self) -> Option<EngineExecutionResult> {
        self.completion
            .read()
            .as_ref()
            .and_then(|value| value.as_ref().ok().cloned())
    }
    pub fn error(&self) -> Option<String> {
        self.completion
            .read()
            .as_ref()
            .and_then(|value| value.as_ref().err().cloned())
    }
    async fn wait(&self) -> Result<EngineExecutionResult, String> {
        loop {
            let notified = self.notify.notified();
            if let Some(result) = self.result() {
                return Ok(result);
            }
            if let Some(error) = self.error() {
                return Err(error);
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
pub struct JobHandle {
    id: JobId,
    repository: StateRepository,
    cancellation: CancellationToken,
    completion: EngineResultStore,
}

impl JobHandle {
    pub(crate) fn new(
        id: JobId,
        repository: StateRepository,
        cancellation: CancellationToken,
        completion: EngineResultStore,
    ) -> Self {
        Self {
            id,
            repository,
            cancellation,
            completion,
        }
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn state(&self) -> Option<JobState> {
        self.repository.job(self.id).map(|job| job.state)
    }

    pub fn snapshot(&self) -> Option<JobSnapshot> {
        let job = self.repository.job(self.id)?;
        let groups = self.repository.task_groups(self.id);
        let mut tasks = Vec::new();
        for group in &groups {
            tasks.extend(self.repository.tasks(group.id));
        }
        let result = self.completion.result();
        let error = self
            .completion
            .error()
            .or_else(|| result.as_ref().and_then(|r| r.error.clone()));
        Some(JobSnapshot {
            job,
            task_groups: groups,
            tasks,
            result,
            error,
        })
    }

    pub fn cancel(&self) -> bool {
        if self.completion.result().is_some() || self.completion.error().is_some() {
            return false;
        }
        self.cancellation.cancel();
        true
    }

    pub async fn wait(&self) -> Result<EngineExecutionResult, String> {
        self.completion.wait().await
    }
}

/// Helper for contract tests.
#[cfg(test)]
pub(crate) fn test_job_handle(repository: StateRepository) -> (JobHandle, EngineResultStore) {
    let id = JobId::new();
    repository.register_job(Job::new(id)).expect("fresh job id");
    repository
        .update_job(id, JobState::SUBMITTED)
        .expect("valid transition");
    let store = EngineResultStore::new();
    let handle = JobHandle::new(id, repository, CancellationToken::new(), store.clone());
    (handle, store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use relus_reader::StreamMode;

    fn result() -> EngineExecutionResult {
        EngineExecutionResult {
            status: EngineExecutionStatus::Succeeded,
            records_read: 3,
            records_written: 3,
            records_failed: 0,
            cancelled: false,
            stream_mode: StreamMode::Batch,
            elapsed: Duration::from_millis(2),
            error: None,
        }
    }

    #[tokio::test]
    async fn repeated_wait_returns_same_result() {
        let (handle, store) = test_job_handle(StateRepository::new());
        let first = handle.clone();
        let second = handle.clone();
        let producer = tokio::spawn(async move {
            store.complete(result());
        });
        assert_eq!(first.wait().await.unwrap(), second.wait().await.unwrap());
        producer.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_does_not_discard_completed_result() {
        let (handle, store) = test_job_handle(StateRepository::new());
        store.complete(result());
        assert!(!handle.cancel());
        assert_eq!(handle.wait().await.unwrap().records_written, 3);
    }

    #[tokio::test]
    async fn cancellation_after_failure_is_rejected() {
        let (handle, store) = test_job_handle(StateRepository::new());
        store.fail("failed");
        assert!(!handle.cancel());
        assert_eq!(handle.wait().await.unwrap_err(), "failed");
    }

    #[test]
    fn snapshot_contains_state_tree_and_result() {
        let (handle, store) = test_job_handle(StateRepository::new());
        store.complete(result());
        let snapshot = handle.snapshot().unwrap();
        assert_eq!(snapshot.job.id, handle.id());
        assert!(snapshot.task_groups.is_empty());
        assert_eq!(snapshot.result.unwrap().records_read, 3);
    }

    fn execution_result(
        status: EngineExecutionStatus,
        written: usize,
        failed: usize,
        cancelled: bool,
        stream_mode: StreamMode,
    ) -> EngineExecutionResult {
        EngineExecutionResult {
            status,
            records_read: written + failed,
            records_written: written,
            records_failed: failed,
            cancelled,
            stream_mode,
            elapsed: Duration::from_secs(2),
            error: (status == EngineExecutionStatus::Failed).then(|| "write failed".into()),
        }
    }

    #[test]
    fn converts_engine_results_to_legacy_statuses() {
        let success = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Succeeded,
            8,
            0,
            false,
            StreamMode::Batch,
        ));
        assert_eq!(success.status, RunStatus::Success);
        assert_eq!(success.stats.throughput, 4.0);

        let partial = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Failed,
            8,
            2,
            false,
            StreamMode::Batch,
        ));
        assert_eq!(partial.status, RunStatus::Partial);
        assert_eq!(partial.error.as_deref(), Some("write failed"));

        let failed = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Failed,
            0,
            2,
            false,
            StreamMode::Batch,
        ));
        assert_eq!(failed.status, RunStatus::Failed);
    }

    #[test]
    fn streaming_completion_and_cancellation_keep_legacy_semantics() {
        let completed = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Succeeded,
            0,
            0,
            false,
            StreamMode::Streaming,
        ));
        assert_eq!(completed.status, RunStatus::Failed);
        assert_eq!(
            completed.error.as_deref(),
            Some("Stream pipeline 非预期退出")
        );

        let cancelled = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Cancelled,
            3,
            0,
            true,
            StreamMode::Streaming,
        ));
        assert_eq!(cancelled.status, RunStatus::Shutdown);
        assert!(cancelled.error.is_none());

        let raced = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Succeeded,
            3,
            0,
            false,
            StreamMode::Streaming,
        ));
        assert_eq!(raced.status, RunStatus::Failed);
    }

    #[test]
    fn compatibility_result_json_shape_is_unchanged() {
        let result = RunResult::from_engine(execution_result(
            EngineExecutionStatus::Succeeded,
            8,
            0,
            false,
            StreamMode::Batch,
        ));
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["status"], "Success");
        assert_eq!(value["stats"]["records_read"], 8);
        assert!(value.get("duration").is_some());
        assert!(value.get("error").is_some());
    }
}
