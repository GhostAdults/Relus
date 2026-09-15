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
    use relus_reader::{DataReaderJob, DataReaderTask, JsonStream, SplitReaderResult, StreamMode};
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult};

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
                      result: EngineExecutionResult,
                      store: Option<EngineResultStore>| {
            if let Some(store) = store {
                store.complete(result);
            }
            this.results.write().remove(&id);
        };
        if token.is_cancelled() {
            let _ = self.repository.update_job(id, JobState::CANCELLED);
            finish(
                self,
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
        let _ = self.repository.update_job(id, JobState::INITIALIZING);
        let runtime = match self.master.build_with_job_id(plan, id).await {
            Ok(runtime) => runtime,
            Err(error) => {
                let message = error.to_string();
                let _ = self.repository.update_job(
                    id,
                    if token.is_cancelled() {
                        JobState::CANCELLED
                    } else {
                        JobState::FAILED
                    },
                );
                finish(
                    self,
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
            let _ = self.repository.update_job(id, JobState::CANCELLED);
            finish(
                self,
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
            let _ = self.repository.update_job(
                id,
                if token.is_cancelled() {
                    JobState::CANCELLED
                } else {
                    JobState::RUNNING
                },
            );
            let cancelled = token.is_cancelled();
            if !cancelled {
                let _ = self.repository.update_job(id, JobState::SUCCEEDED);
            }
            finish(
                self,
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
                    let _ = self.repository.update_job(id, JobState::FAILED);
                    finish(
                        self,
                        EngineExecutionResult {
                            status: EngineExecutionStatus::Failed,
                            records_read: 0,
                            records_written: 0,
                            records_failed: 0,
                            cancelled: false,
                            elapsed: started.elapsed(),
                            error: Some(message),
                        },
                        store,
                    );
                    return;
                }
            }
        }
        let _ = self.repository.update_job(id, JobState::RUNNING);
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
        let error = results.iter().find_map(|r| r.error_summary.clone());
        finish(
            self,
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
        let token = self
            .jobs
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("job not found"))?;
        token.cancel();
        if let Some(job) = self.repository.job(id) {
            if matches!(
                job.state,
                JobState::SUBMITTED | JobState::INITIALIZING | JobState::RUNNING
            ) {
                let _ = self.repository.update_job(id, JobState::CANCELLED);
            }
        }
        Ok(true)
    }
}
