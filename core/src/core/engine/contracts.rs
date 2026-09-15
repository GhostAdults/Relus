//! Engine-owned execution contracts shared by future asynchronous entry points.

use super::state::{Job, JobId, JobState, StateRepository, Task, TaskGroup};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

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
            if let Some(result) = self.result() {
                return Ok(result);
            }
            if let Some(error) = self.error() {
                return Err(error);
            }
            self.notify.notified().await;
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

/// Helper for contract tests and future Coordinator integration.
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

    fn result() -> EngineExecutionResult {
        EngineExecutionResult {
            status: EngineExecutionStatus::Succeeded,
            records_read: 3,
            records_written: 3,
            records_failed: 0,
            cancelled: false,
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
}
