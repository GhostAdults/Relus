use super::{
    job_master::RuntimeTaskGroup,
    runtime::{Runtime, RuntimeTaskHandle},
    state::{StateRepository, TaskGroupState, TaskId, TaskState},
    worker::{Worker, WorkerContext},
};
use anyhow::Result;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskGroupExecutionStatus {
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskGroupExecutionResult {
    pub group_id: super::state::TaskGroupId,
    pub status: TaskGroupExecutionStatus,
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub cancelled: bool,
    pub error_summary: Option<String>,
    pub elapsed: Duration,
}

impl TaskGroupExecutionResult {
    pub fn from_pipeline(
        group_id: super::state::TaskGroupId,
        stats: crate::pipeline::PreparedGroupStats,
    ) -> Self {
        let status = if stats.shutdown {
            TaskGroupExecutionStatus::Cancelled
        } else if stats.error.is_some() {
            TaskGroupExecutionStatus::Failed
        } else {
            TaskGroupExecutionStatus::Succeeded
        };
        Self {
            group_id,
            status,
            records_read: stats.total_read,
            records_written: stats.total_written,
            records_failed: stats.total_read.saturating_sub(stats.total_written),
            cancelled: stats.shutdown,
            error_summary: stats.error,
            elapsed: stats.elapsed,
        }
    }
}

#[async_trait]
pub trait TaskLifecycleObserver: Send + Sync {
    async fn deployed(&self, index: usize);
    async fn completed(&self, index: usize, outcome: TaskOutcome);
}

#[derive(Debug, Clone)]
pub enum TaskOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

struct StateObserver {
    repository: StateRepository,
    task_ids: Vec<TaskId>,
}
#[async_trait]
impl TaskLifecycleObserver for StateObserver {
    async fn deployed(&self, index: usize) {
        if let Some(id) = self.task_ids.get(index) {
            let _ = self.repository.update_task(*id, TaskState::RUNNING);
        }
    }
    async fn completed(&self, index: usize, outcome: TaskOutcome) {
        if let Some(id) = self.task_ids.get(index) {
            let state = match outcome {
                TaskOutcome::Succeeded => TaskState::SUCCEEDED,
                TaskOutcome::Failed => TaskState::FAILED,
                TaskOutcome::Cancelled => TaskState::CANCELLED,
            };
            let _ = self.repository.update_task(*id, state);
        }
    }
}

pub struct TaskExecutionService {
    repository: StateRepository,
    runtime: Arc<dyn Runtime>,
}

impl TaskExecutionService {
    pub fn new(repository: StateRepository, runtime: Arc<dyn Runtime>) -> Self {
        Self {
            repository,
            runtime,
        }
    }

    pub fn deploy(
        &self,
        group: RuntimeTaskGroup,
        context: WorkerContext,
        cancel: CancellationToken,
    ) -> Result<RuntimeTaskHandle> {
        self.repository
            .update_task_group(group.group.id, TaskGroupState::SUBMITTED)?;
        self.repository
            .update_task_group(group.group.id, TaskGroupState::INITIALIZING)?;
        for task in &group.tasks {
            self.repository
                .update_task(task.task.id, TaskState::SUBMITTED)?;
            self.repository
                .update_task(task.task.id, TaskState::INITIALIZING)?;
        }
        let observer: Arc<dyn TaskLifecycleObserver> = Arc::new(StateObserver {
            repository: self.repository.clone(),
            task_ids: group.tasks.iter().map(|t| t.task.id).collect(),
        });
        let task_ids: Vec<TaskId> = group.tasks.iter().map(|t| t.task.id).collect();
        let task_ids_for_future = task_ids.clone();
        let repo = self.repository.clone();
        let group_id = group.group.id;
        let cancel_for_task = cancel.clone();
        let fut = Box::pin(async move {
            if group.tasks.is_empty() {
                repo.update_task_group(group_id, TaskGroupState::RUNNING)
                    .map_err(anyhow::Error::from)?;
                repo.update_task_group(group_id, TaskGroupState::SUCCEEDED)
                    .map_err(anyhow::Error::from)?;
                return Ok(TaskGroupExecutionResult {
                    group_id,
                    status: TaskGroupExecutionStatus::Succeeded,
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled: false,
                    error_summary: None,
                    elapsed: Duration::ZERO,
                });
            }
            repo.update_task_group(group_id, TaskGroupState::RUNNING)
                .map_err(anyhow::Error::from)?;
            let result = Worker::run(group, context, cancel_for_task.clone(), observer).await;
            match &result {
                Ok(r) => {
                    let state = match r.status {
                        TaskGroupExecutionStatus::Succeeded => TaskGroupState::SUCCEEDED,
                        TaskGroupExecutionStatus::Failed => TaskGroupState::FAILED,
                        TaskGroupExecutionStatus::Cancelled => TaskGroupState::CANCELLED,
                    };
                    let _ = repo.update_task_group(group_id, state);
                    let task_state = match r.status {
                        TaskGroupExecutionStatus::Succeeded => TaskState::SUCCEEDED,
                        TaskGroupExecutionStatus::Failed => TaskState::FAILED,
                        TaskGroupExecutionStatus::Cancelled => TaskState::CANCELLED,
                    };
                    for id in &task_ids_for_future {
                        let _ = repo.update_task(*id, task_state);
                    }
                }
                Err(_) => {
                    let state = if cancel_for_task.is_cancelled() {
                        TaskGroupState::CANCELLED
                    } else {
                        TaskGroupState::FAILED
                    };
                    let _ = repo.update_task_group(group_id, state);
                    let task_state = if cancel_for_task.is_cancelled() {
                        TaskState::CANCELLED
                    } else {
                        TaskState::FAILED
                    };
                    for id in &task_ids_for_future {
                        let _ = repo.update_task(*id, task_state);
                    }
                }
            }
            result
        });
        match self.runtime.submit(group_id, cancel, fut) {
            Ok(handle) => Ok(handle),
            Err(error) => {
                let _ = self
                    .repository
                    .update_task_group(group_id, TaskGroupState::FAILED);
                for task_id in task_ids {
                    let _ = self.repository.update_task(task_id, TaskState::FAILED);
                }
                Err(error)
            }
        }
    }

    pub fn deploy_empty(
        &self,
        group: RuntimeTaskGroup,
        cancel: CancellationToken,
    ) -> Result<RuntimeTaskHandle> {
        self.deploy(
            group,
            WorkerContext {
                reader: Arc::new(EmptyReader),
                writer: Arc::new(EmptyWriter),
                pipeline: Default::default(),
                record_builder: Arc::new(crate::pipeline::RecordBuilder::new(
                    std::collections::BTreeMap::new(),
                    None,
                )?),
            },
            cancel,
        )
    }
    pub fn cancel(&self, group_id: super::state::TaskGroupId) -> bool {
        self.runtime.cancel(group_id)
    }
}

struct EmptyReader;
#[async_trait]
impl relus_reader::DataReaderJob for EmptyReader {
    async fn split(&self, _: usize) -> Result<relus_reader::SplitReaderResult> {
        unreachable!()
    }
    fn description(&self) -> String {
        "empty".into()
    }
}
#[async_trait]
impl relus_reader::DataReaderTask for EmptyReader {
    async fn read_data(&self, _: &relus_reader::ReadTask) -> Result<relus_reader::JsonStream> {
        unreachable!()
    }
}
struct EmptyWriter;
#[async_trait]
impl relus_writer::DataWriterJob for EmptyWriter {
    async fn split(&self, _: usize) -> Result<relus_writer::SplitWriterResult> {
        unreachable!()
    }
    fn description(&self) -> String {
        "empty".into()
    }
}
#[async_trait]
impl relus_writer::DataWriterTask for EmptyWriter {
    async fn write_data(
        &self,
        _: relus_writer::WriteTask,
        _: tokio::sync::mpsc::Receiver<relus_common::PipelineMessage>,
    ) -> Result<usize> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::engine::job_master::RuntimeTaskGroup;
    use crate::core::engine::runtime::TokioRuntime;
    use crate::core::engine::state::{
        Job, JobId, StateRepository, TaskGroup, TaskGroupId, TaskGroupState,
    };
    use async_trait::async_trait;
    use futures::stream;
    use relus_common::{
        job_config::{JobConfig, WriteMode},
        PipelineMessage,
    };
    use relus_reader::{DataReaderJob, DataReaderTask, JsonStream, ReadTask, SplitReaderResult};
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult, WriteTask};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn empty_task_group_completes_successfully() {
        let repository = StateRepository::new();
        let job = Job::new(JobId::new());
        repository.register_job(job.clone()).unwrap();
        let group = TaskGroup::new(TaskGroupId::new(), job.id);
        repository.register_task_group(group.clone()).unwrap();

        let service = TaskExecutionService::new(repository.clone(), Arc::new(TokioRuntime::new()));
        let handle = service
            .deploy_empty(
                RuntimeTaskGroup {
                    group: group.clone(),
                    tasks: vec![],
                    concurrency: 1,
                },
                CancellationToken::new(),
            )
            .unwrap();
        let result = handle.join().await.unwrap();

        assert_eq!(result.status, TaskGroupExecutionStatus::Succeeded);
        assert_eq!(
            repository.task_group(group.id).unwrap().state,
            TaskGroupState::SUCCEEDED
        );
    }

    struct FakeReader {
        splits: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            self.splits.fetch_add(1, Ordering::SeqCst);
            unreachable!()
        }
        fn description(&self) -> String {
            "fake".into()
        }
    }
    #[async_trait]
    impl DataReaderTask for FakeReader {
        async fn read_data(&self, _: &ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(stream::iter(vec![
                Ok(serde_json::json!({"v":1})),
                Ok(serde_json::json!({"v":2})),
            ])))
        }
    }
    struct FakeWriter {
        writes: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            unreachable!()
        }
        fn description(&self) -> String {
            "fake".into()
        }
    }
    #[async_trait]
    impl DataWriterTask for FakeWriter {
        async fn write_data(
            &self,
            _: WriteTask,
            mut rx: tokio::sync::mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            let mut n = 0;
            while let Some(msg) = rx.recv().await {
                if let PipelineMessage::DataBatch(rows) = msg {
                    n += rows.len();
                }
            }
            self.writes.fetch_add(n, Ordering::SeqCst);
            Ok(n)
        }
    }

    #[tokio::test]
    async fn deploys_prepared_group_without_resplitting_and_reaches_running() {
        let repo = StateRepository::new();
        let job = Job::new(JobId::new());
        repo.register_job(job.clone()).unwrap();
        let group = TaskGroup::new(TaskGroupId::new(), job.id);
        repo.register_task_group(group.clone()).unwrap();
        let task = super::super::state::Task::new(super::super::state::TaskId::new(), group.id);
        repo.register_task(task.clone()).unwrap();
        let cfg = Arc::new(JobConfig::parse_json(r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{}}"#).unwrap());
        let read = ReadTask {
            task_id: 0,
            conn: serde_json::json!({}),
            query_sql: None,
            offset: 0,
            limit: 2,
        };
        let write = WriteTask {
            task_id: 0,
            config: cfg,
            mode: WriteMode::Insert,
            use_transaction: false,
            batch_size: 2,
        };
        let splits = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let service = TaskExecutionService::new(repo.clone(), Arc::new(TokioRuntime::new()));
        let handle = service
            .deploy(
                RuntimeTaskGroup {
                    group: group.clone(),
                    tasks: vec![super::super::job_master::RuntimeTask {
                        task: task.clone(),
                        read_task: read,
                        write_task: write,
                    }],
                    concurrency: 1,
                },
                WorkerContext {
                    reader: Arc::new(FakeReader {
                        splits: splits.clone(),
                    }),
                    writer: Arc::new(FakeWriter {
                        writes: writes.clone(),
                    }),
                    pipeline: Default::default(),
                    record_builder: Arc::new(
                        crate::pipeline::RecordBuilder::new(
                            std::collections::BTreeMap::new(),
                            None,
                        )
                        .unwrap(),
                    ),
                },
                CancellationToken::new(),
            )
            .unwrap();
        let result = handle.join().await.unwrap();
        assert_eq!(result.status, TaskGroupExecutionStatus::Succeeded);
        assert_eq!(result.records_read, 2);
        assert_eq!(writes.load(Ordering::SeqCst), 2);
        assert_eq!(splits.load(Ordering::SeqCst), 0);
        assert_eq!(repo.task(task.id).unwrap().state, TaskState::SUCCEEDED);
        assert_eq!(
            repo.task_group(group.id).unwrap().state,
            TaskGroupState::SUCCEEDED
        );
    }

    struct PendingReader;
    #[async_trait]
    impl DataReaderJob for PendingReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            unreachable!()
        }
        fn description(&self) -> String {
            "pending".into()
        }
    }
    #[async_trait]
    impl DataReaderTask for PendingReader {
        async fn read_data(&self, _: &ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(stream::pending()))
        }
    }

    #[tokio::test]
    async fn cancellation_propagates_to_group_and_task() {
        let repo = StateRepository::new();
        let job = Job::new(JobId::new());
        repo.register_job(job.clone()).unwrap();
        let group = TaskGroup::new(TaskGroupId::new(), job.id);
        repo.register_task_group(group.clone()).unwrap();
        let task = super::super::state::Task::new(super::super::state::TaskId::new(), group.id);
        repo.register_task(task.clone()).unwrap();
        let cfg = Arc::new(JobConfig::parse_json(r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{}}"#).unwrap());
        let write = WriteTask {
            task_id: 0,
            config: cfg,
            mode: WriteMode::Insert,
            use_transaction: false,
            batch_size: 1,
        };
        let token = CancellationToken::new();
        let service = TaskExecutionService::new(repo.clone(), Arc::new(TokioRuntime::new()));
        let handle = service
            .deploy(
                RuntimeTaskGroup {
                    group: group.clone(),
                    tasks: vec![super::super::job_master::RuntimeTask {
                        task: task.clone(),
                        read_task: ReadTask {
                            task_id: 0,
                            conn: serde_json::json!({}),
                            query_sql: None,
                            offset: 0,
                            limit: 1,
                        },
                        write_task: write,
                    }],
                    concurrency: 1,
                },
                WorkerContext {
                    reader: Arc::new(PendingReader),
                    writer: Arc::new(FakeWriter {
                        writes: Arc::new(AtomicUsize::new(0)),
                    }),
                    pipeline: Default::default(),
                    record_builder: Arc::new(
                        crate::pipeline::RecordBuilder::new(
                            std::collections::BTreeMap::new(),
                            None,
                        )
                        .unwrap(),
                    ),
                },
                token.clone(),
            )
            .unwrap();
        tokio::task::yield_now().await;
        token.cancel();
        let result = handle.join().await.unwrap();
        assert_eq!(result.status, TaskGroupExecutionStatus::Cancelled);
        assert!(result.cancelled);
        assert_eq!(repo.task(task.id).unwrap().state, TaskState::CANCELLED);
        assert_eq!(
            repo.task_group(group.id).unwrap().state,
            TaskGroupState::CANCELLED
        );
    }
}
