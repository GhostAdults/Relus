use super::task_execution::TaskGroupExecutionResult;
use super::{
    contracts::{EngineExecutionResult, EngineExecutionStatus, EngineResultStore, JobHandle},
    job_master::JobMaster,
    runtime::{Runtime, TokioRuntime},
    state::{Job, JobId, JobState, StateRepository},
    task_execution::TaskExecutionService,
    worker::WorkerContext,
};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::time::Instant;
use std::{collections::HashMap, sync::Arc};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream;
    use relus_common::{
        job_config::{JobConfig, WriteMode},
        PipelineMessage,
    };
    use relus_reader::{
        DataReaderJob, DataReaderTask, JsonStream, ReadTask, SplitReaderResult, StreamMode,
    };
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult, WriteTask};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct EmptyReader;
    #[async_trait]
    impl DataReaderJob for EmptyReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            panic!("split must not be called")
        }
        fn description(&self) -> String {
            "empty".into()
        }
    }
    #[async_trait]
    impl DataReaderTask for EmptyReader {
        async fn read_data(&self, _: &relus_reader::ReadTask) -> Result<JsonStream> {
            unreachable!()
        }
    }
    struct EmptyWriter;
    #[async_trait]
    impl DataWriterJob for EmptyWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            panic!("split must not be called")
        }
        fn description(&self) -> String {
            "empty".into()
        }
    }
    #[async_trait]
    impl DataWriterTask for EmptyWriter {
        async fn write_data(
            &self,
            _: relus_writer::WriteTask,
            _: tokio::sync::mpsc::Receiver<relus_common::PipelineMessage>,
        ) -> Result<usize> {
            unreachable!()
        }
    }

    fn empty_plan() -> crate::core::planner::ExecutionPlan {
        crate::core::planner::ExecutionPlan {
            reader: Arc::new(EmptyReader),
            writer: Arc::new(EmptyWriter),
            pipeline: Default::default(),
            record_builder: Arc::new(
                crate::pipeline::RecordBuilder::new(std::collections::BTreeMap::new(), None)
                    .unwrap(),
            ),
            reader_split: SplitReaderResult {
                total_records: 0,
                tasks: vec![],
                stream_mode: StreamMode::Batch,
            },
            stream_mode: StreamMode::Batch,
        }
    }

    #[tokio::test]
    async fn submit_returns_handle_and_aggregates_empty_job() {
        let coordinator = CoordinatorService::new(StateRepository::new());
        let handle = coordinator.submit_job(empty_plan()).unwrap();
        assert!(coordinator.query_job(handle.id()).is_some());
        let result = handle.wait().await.unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Succeeded);
        assert_eq!(handle.state(), Some(JobState::SUCCEEDED));
    }

    #[derive(Clone, Copy)]
    enum ReadBehavior {
        Success,
        Pending,
    }
    struct FakeReader {
        behavior: ReadBehavior,
        splits: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            self.splits.fetch_add(1, Ordering::SeqCst);
            panic!("prepared plans must not split readers")
        }
        fn description(&self) -> String {
            "fake".into()
        }
    }
    #[async_trait]
    impl DataReaderTask for FakeReader {
        async fn read_data(&self, _: &ReadTask) -> Result<JsonStream> {
            Ok(match self.behavior {
                ReadBehavior::Success => {
                    Box::pin(stream::iter(vec![Ok(serde_json::json!({"v": 1}))]))
                }
                ReadBehavior::Pending => Box::pin(stream::pending()),
            })
        }
    }
    struct FakeWriter {
        split_failure: bool,
        write_failure: bool,
        writes: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            if self.split_failure {
                return Err(anyhow!("fake initialization failure"));
            }
            let config = Arc::new(JobConfig::parse_json(
                r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{}}"#,
            )?);
            Ok(SplitWriterResult {
                tasks: vec![WriteTask {
                    task_id: 0,
                    config,
                    mode: WriteMode::Insert,
                    use_transaction: false,
                    batch_size: 1,
                }],
            })
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
            if self.write_failure {
                return Err(anyhow!("fake execution failure"));
            }
            let mut written = 0;
            while let Some(message) = rx.recv().await {
                if let PipelineMessage::DataBatch(rows) = message {
                    written += rows.len();
                }
            }
            self.writes.fetch_add(written, Ordering::SeqCst);
            Ok(written)
        }
    }
    fn fake_plan(
        behavior: ReadBehavior,
        split_failure: bool,
        write_failure: bool,
        splits: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
    ) -> crate::core::planner::ExecutionPlan {
        crate::core::planner::ExecutionPlan {
            reader: Arc::new(FakeReader { behavior, splits }),
            writer: Arc::new(FakeWriter {
                split_failure,
                write_failure,
                writes,
            }),
            pipeline: Default::default(),
            record_builder: Arc::new(
                crate::pipeline::RecordBuilder::new(std::collections::BTreeMap::new(), None)
                    .unwrap(),
            ),
            reader_split: SplitReaderResult {
                total_records: 1,
                tasks: vec![ReadTask {
                    task_id: 0,
                    conn: serde_json::json!({}),
                    query_sql: None,
                    offset: 0,
                    limit: 1,
                }],
                stream_mode: StreamMode::Batch,
            },
            stream_mode: StreamMode::Batch,
        }
    }

    #[tokio::test]
    async fn submit_executes_prepared_plan_and_repeated_wait_is_stable() {
        let splits = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let coordinator = CoordinatorService::new(StateRepository::new());
        let handle = coordinator
            .submit_job(fake_plan(
                ReadBehavior::Success,
                false,
                false,
                splits.clone(),
                writes.clone(),
            ))
            .unwrap();
        assert!(matches!(
            handle.state(),
            Some(
                JobState::SUBMITTED
                    | JobState::INITIALIZING
                    | JobState::RUNNING
                    | JobState::SUCCEEDED
            )
        ));
        let first = handle.wait().await.unwrap();
        let second = handle.wait().await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.status, EngineExecutionStatus::Succeeded);
        assert_eq!((first.records_read, first.records_written), (1, 1));
        assert_eq!(splits.load(Ordering::SeqCst), 0);
        assert_eq!(writes.load(Ordering::SeqCst), 1);
        let snapshot = handle.snapshot().unwrap();
        assert_eq!(snapshot.job.state, JobState::SUCCEEDED);
        assert!(snapshot
            .task_groups
            .iter()
            .all(|group| group.state == super::super::state::TaskGroupState::SUCCEEDED));
        assert!(snapshot
            .tasks
            .iter()
            .all(|task| task.state == super::super::state::TaskState::SUCCEEDED));
        assert_eq!(snapshot.result, Some(first));
    }

    #[tokio::test]
    async fn initialization_failure_is_saved_in_snapshot() {
        let coordinator = CoordinatorService::new(StateRepository::new());
        let handle = coordinator
            .submit_job(fake_plan(
                ReadBehavior::Success,
                true,
                false,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ))
            .unwrap();
        let result = handle.wait().await.unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Failed);
        assert!(result
            .error
            .as_deref()
            .unwrap()
            .contains("fake initialization failure"));
        assert_eq!(handle.state(), Some(JobState::FAILED));
        assert_eq!(handle.snapshot().unwrap().error, result.error);
    }

    #[tokio::test]
    async fn execution_failure_is_saved_and_marks_tree_failed() {
        let coordinator = CoordinatorService::new(StateRepository::new());
        let handle = coordinator
            .submit_job(fake_plan(
                ReadBehavior::Success,
                false,
                true,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ))
            .unwrap();
        let result = handle.wait().await.unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Failed);
        assert_eq!(handle.state(), Some(JobState::FAILED));
        let snapshot = handle.snapshot().unwrap();
        assert!(snapshot
            .task_groups
            .iter()
            .all(|group| group.state == super::super::state::TaskGroupState::FAILED));
        assert!(snapshot
            .tasks
            .iter()
            .all(|task| task.state == super::super::state::TaskState::FAILED));
    }

    #[tokio::test]
    async fn cancellation_propagates_and_saves_shutdown_result() {
        let coordinator = CoordinatorService::new(StateRepository::new());
        let handle = coordinator
            .submit_job(fake_plan(
                ReadBehavior::Pending,
                false,
                false,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ))
            .unwrap();
        while handle.state() != Some(JobState::RUNNING) {
            tokio::task::yield_now().await;
        }
        assert!(handle.cancel());
        let result = handle.wait().await.unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Cancelled);
        assert_eq!(result.error.as_deref(), Some("Shutdown"));
        assert_eq!(handle.state(), Some(JobState::CANCELLED));
        assert!(!handle.cancel());
        let snapshot = handle.snapshot().unwrap();
        assert!(snapshot
            .task_groups
            .iter()
            .all(|group| group.state == super::super::state::TaskGroupState::CANCELLED));
        assert!(snapshot
            .tasks
            .iter()
            .all(|task| task.state == super::super::state::TaskState::CANCELLED));
    }
}

#[derive(Clone)]
pub struct CoordinatorService {
    repository: StateRepository,
    master: JobMaster,
    execution: Arc<TaskExecutionService>,
    jobs: Arc<RwLock<HashMap<JobId, CancellationToken>>>,
    results: Arc<RwLock<HashMap<JobId, EngineResultStore>>>,
}

impl CoordinatorService {
    pub fn new(repository: StateRepository) -> Self {
        let runtime: Arc<dyn Runtime> = Arc::new(TokioRuntime::new());
        Self {
            master: JobMaster::new(repository.clone()),
            execution: Arc::new(TaskExecutionService::new(repository.clone(), runtime)),
            repository,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            results: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub fn repository(&self) -> &StateRepository {
        &self.repository
    }
    pub fn submit_job(&self, plan: crate::core::planner::ExecutionPlan) -> Result<JobHandle> {
        let id = JobId::new();
        self.repository.register_job(Job::new(id))?;
        self.repository.update_job(id, JobState::SUBMITTED)?;
        let token = CancellationToken::new();
        let store = EngineResultStore::new();
        self.jobs.write().insert(id, token.clone());
        self.results.write().insert(id, store.clone());
        let handle = JobHandle::new(id, self.repository.clone(), token.clone(), store);
        let this = self.clone();
        tokio::spawn(async move {
            this.execute_and_store(id, plan, token).await;
            this.jobs.write().remove(&id);
        });
        Ok(handle)
    }

    async fn execute_and_store(
        &self,
        id: JobId,
        plan: crate::core::planner::ExecutionPlan,
        token: CancellationToken,
    ) {
        let started = Instant::now();
        let store = self.results.read().get(&id).cloned();
        let finish = |this: &CoordinatorService,
                      state: JobState,
                      mut result: EngineExecutionResult,
                      store: Option<EngineResultStore>| {
            if let Err(error) = this.transition_job(id, state) {
                result.status = EngineExecutionStatus::Failed;
                result.cancelled = false;
                result.error = Some(format!("failed to persist job lifecycle: {error}"));
            }
            if let Some(store) = store {
                store.complete(result);
            }
        };
        if token.is_cancelled() {
            finish(
                self,
                JobState::CANCELLED,
                EngineExecutionResult {
                    status: EngineExecutionStatus::Cancelled,
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled: true,
                    elapsed: started.elapsed(),
                    error: Some("Shutdown".into()),
                },
                store,
            );
            return;
        }
        if let Err(error) = self.transition_job(id, JobState::INITIALIZING) {
            finish(
                self,
                JobState::FAILED,
                EngineExecutionResult {
                    status: EngineExecutionStatus::Failed,
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled: false,
                    elapsed: started.elapsed(),
                    error: Some(format!("failed to initialize job lifecycle: {error}")),
                },
                store,
            );
            return;
        }
        let runtime = match self.master.build_with_job_id(plan, id).await {
            Ok(runtime) => runtime,
            Err(error) => {
                let message = error.to_string();
                let terminal = if token.is_cancelled() {
                    JobState::CANCELLED
                } else {
                    JobState::FAILED
                };
                finish(
                    self,
                    terminal,
                    EngineExecutionResult {
                        status: if token.is_cancelled() {
                            EngineExecutionStatus::Cancelled
                        } else {
                            EngineExecutionStatus::Failed
                        },
                        records_read: 0,
                        records_written: 0,
                        records_failed: 0,
                        cancelled: token.is_cancelled(),
                        elapsed: started.elapsed(),
                        error: Some(message),
                    },
                    store,
                );
                return;
            }
        };
        if token.is_cancelled() {
            finish(
                self,
                JobState::CANCELLED,
                EngineExecutionResult {
                    status: EngineExecutionStatus::Cancelled,
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled: true,
                    elapsed: started.elapsed(),
                    error: Some("Shutdown".into()),
                },
                store,
            );
            return;
        }
        if runtime.groups.is_empty() {
            let cancelled = token.is_cancelled();
            if !cancelled && self.transition_job(id, JobState::RUNNING).is_err() {
                finish(
                    self,
                    JobState::FAILED,
                    EngineExecutionResult {
                        status: EngineExecutionStatus::Failed,
                        records_read: 0,
                        records_written: 0,
                        records_failed: 0,
                        cancelled: false,
                        elapsed: started.elapsed(),
                        error: Some("failed to persist RUNNING lifecycle".into()),
                    },
                    store,
                );
                return;
            }
            finish(
                self,
                if cancelled {
                    JobState::CANCELLED
                } else {
                    JobState::SUCCEEDED
                },
                EngineExecutionResult {
                    status: if cancelled {
                        EngineExecutionStatus::Cancelled
                    } else {
                        EngineExecutionStatus::Succeeded
                    },
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled,
                    elapsed: started.elapsed(),
                    error: cancelled.then(|| "Shutdown".into()),
                },
                store,
            );
            return;
        }
        let mut handles = Vec::new();
        for group in runtime.groups {
            let context = WorkerContext {
                reader: Arc::clone(&runtime.reader),
                writer: Arc::clone(&runtime.writer),
                pipeline: runtime.pipeline.clone(),
                record_builder: Arc::clone(&runtime.record_builder),
            };
            match self.execution.deploy(group, context, token.clone()) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    let message = error.to_string();
                    let cancelled = token.is_cancelled();
                    token.cancel();
                    finish(
                        self,
                        if cancelled {
                            JobState::CANCELLED
                        } else {
                            JobState::FAILED
                        },
                        EngineExecutionResult {
                            status: if cancelled {
                                EngineExecutionStatus::Cancelled
                            } else {
                                EngineExecutionStatus::Failed
                            },
                            records_read: 0,
                            records_written: 0,
                            records_failed: 0,
                            cancelled,
                            elapsed: started.elapsed(),
                            error: Some(if cancelled {
                                "Shutdown".into()
                            } else {
                                message
                            }),
                        },
                        store,
                    );
                    return;
                }
            }
        }
        if let Err(error) = self.transition_job(id, JobState::RUNNING) {
            token.cancel();
            finish(
                self,
                JobState::FAILED,
                EngineExecutionResult {
                    status: EngineExecutionStatus::Failed,
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled: false,
                    elapsed: started.elapsed(),
                    error: Some(format!("failed to persist RUNNING lifecycle: {error}")),
                },
                store,
            );
            return;
        }
        let mut results = Vec::new();
        for handle in handles {
            let group_id = handle.group_id();
            match handle.join().await {
                Ok(result) => results.push(result),
                Err(error) => results.push(TaskGroupExecutionResult {
                    group_id,
                    status: super::task_execution::TaskGroupExecutionStatus::Failed,
                    records_read: 0,
                    records_written: 0,
                    records_failed: 0,
                    cancelled: token.is_cancelled(),
                    error_summary: Some(error.to_string()),
                    elapsed: started.elapsed(),
                }),
            }
        }
        let failed = results.iter().any(|r| {
            matches!(
                r.status,
                super::task_execution::TaskGroupExecutionStatus::Failed
            )
        });
        let cancelled = !failed && (token.is_cancelled() || results.iter().any(|r| r.cancelled));
        let state = if cancelled {
            JobState::CANCELLED
        } else if failed {
            JobState::FAILED
        } else {
            JobState::SUCCEEDED
        };
        let error = results.iter().find_map(|r| r.error_summary.clone());
        finish(
            self,
            state,
            EngineExecutionResult {
                status: if cancelled {
                    EngineExecutionStatus::Cancelled
                } else if failed {
                    EngineExecutionStatus::Failed
                } else {
                    EngineExecutionStatus::Succeeded
                },
                records_read: results.iter().map(|r| r.records_read).sum(),
                records_written: results.iter().map(|r| r.records_written).sum(),
                records_failed: results.iter().map(|r| r.records_failed).sum(),
                cancelled,
                elapsed: started.elapsed(),
                error: if cancelled {
                    Some("Shutdown".into())
                } else {
                    error
                },
            },
            store,
        );
    }

    fn transition_job(&self, id: JobId, state: JobState) -> Result<()> {
        if self
            .repository
            .job(id)
            .is_some_and(|job| job.state == state)
        {
            return Ok(());
        }
        self.repository.update_job(id, state).map_err(Into::into)
    }

    /// Register and synchronously execute a plan for compatibility callers.
    pub async fn run_job(
        &self,
        plan: crate::core::planner::ExecutionPlan,
        token: CancellationToken,
    ) -> Result<Vec<TaskGroupExecutionResult>> {
        let id = JobId::new();
        self.repository.register_job(Job::new(id))?;
        self.repository.update_job(id, JobState::SUBMITTED)?;
        self.execute_job(id, plan, token).await
    }

    /// Execute a prepared plan and return every TaskGroup result. This is the
    /// synchronous compatibility seam used by Runner; submit_job remains the
    /// fire-and-forget API for existing callers.
    pub async fn execute_job(
        &self,
        id: JobId,
        plan: crate::core::planner::ExecutionPlan,
        token: CancellationToken,
    ) -> Result<Vec<TaskGroupExecutionResult>> {
        if token.is_cancelled() {
            return Ok(Vec::new());
        }
        let _ = self.repository.update_job(id, JobState::INITIALIZING);
        let runtime = self.master.build_with_job_id(plan, id).await.map_err(|e| {
            let _ = self.repository.update_job(id, JobState::FAILED);
            e
        })?;
        if runtime.groups.is_empty() {
            let _ = self.repository.update_job(id, JobState::RUNNING);
            let _ = self.repository.update_job(id, JobState::SUCCEEDED);
            return Ok(Vec::new());
        }
        let mut handles = Vec::new();
        for group in runtime.groups {
            let context = WorkerContext {
                reader: Arc::clone(&runtime.reader),
                writer: Arc::clone(&runtime.writer),
                pipeline: runtime.pipeline.clone(),
                record_builder: Arc::clone(&runtime.record_builder),
            };
            handles.push(self.execution.deploy(group, context, token.clone())?);
        }
        let _ = self.repository.update_job(id, JobState::RUNNING);
        let mut results = Vec::new();
        for handle in handles {
            results.push(handle.join().await?);
        }
        let cancelled = token.is_cancelled() || results.iter().any(|r| r.cancelled);
        let failed = results.iter().any(|r| {
            matches!(
                r.status,
                super::task_execution::TaskGroupExecutionStatus::Failed
            )
        });
        let state = if cancelled {
            JobState::CANCELLED
        } else if failed {
            JobState::FAILED
        } else {
            JobState::SUCCEEDED
        };
        let _ = self.repository.update_job(id, state);
        Ok(results)
    }
    pub fn query_job(&self, id: JobId) -> Option<Job> {
        self.repository.job(id)
    }
    pub fn cancel_job(&self, id: JobId) -> Result<bool> {
        if let Some(job) = self.repository.job(id) {
            if !matches!(
                job.state,
                JobState::SUBMITTED | JobState::INITIALIZING | JobState::RUNNING
            ) {
                return Ok(false);
            }
        } else {
            return Err(anyhow!("job not found"));
        }
        let token = self
            .jobs
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("job not found"))?;
        token.cancel();
        Ok(true)
    }
}
