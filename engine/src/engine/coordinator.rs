use super::task_execution::TaskGroupExecutionResult;
use super::{
    contracts::{
        EngineExecutionResult, EngineExecutionStatus, EngineResultStore, ExecutionOptions,
        JobHandle, JobSubmission, ProgressObserver, ProgressOutcome, ProgressTopology,
        SafeProgressObserver,
    },
    job_master::{JobMaster, PlanningDependencies},
    runtime::{Runtime, TokioRuntime},
    state::{Job, JobId, JobState, StateRepository, TaskGroupState, TaskState},
    task_execution::TaskExecutionService,
    worker::WorkerContext,
};
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use relus_common::job_config::JobConfig;
use relus_reader::StreamMode;
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

    #[derive(Default)]
    struct RecordingProgress {
        started: AtomicUsize,
        read: AtomicUsize,
        sent: AtomicUsize,
        finished: AtomicUsize,
        topology: std::sync::Mutex<Option<ProgressTopology>>,
    }

    impl ProgressObserver for RecordingProgress {
        fn planned(&self, topology: ProgressTopology) {
            *self.topology.lock().unwrap() = Some(topology);
        }
        fn started(&self, _: Option<u64>) {
            self.started.fetch_add(1, Ordering::SeqCst);
        }
        fn records_read(&self, delta: u64) {
            self.read.fetch_add(delta as usize, Ordering::SeqCst);
        }
        fn records_sent(&self, delta: u64) {
            self.sent.fetch_add(delta as usize, Ordering::SeqCst);
        }
        fn finished(&self, _: ProgressOutcome, read: u64, written: u64) {
            assert_eq!(read, 1);
            assert_eq!(written, 1);
            self.finished.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct EmptyReader;
    #[async_trait]
    impl DataReaderJob for EmptyReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            Ok(SplitReaderResult {
                total_records: 0,
                tasks: Vec::new(),
                stream_mode: StreamMode::Batch,
            })
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
            Ok(SplitWriterResult { tasks: Vec::new() })
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

    struct EmptyPlanning;

    impl PlanningDependencies for EmptyPlanning {
        fn create_reader(&self, _: Arc<JobConfig>) -> Result<relus_reader::Source> {
            Ok(Arc::new(EmptyReader))
        }

        fn create_writer(&self, _: Arc<JobConfig>) -> Result<relus_writer::Sink> {
            Ok(Arc::new(EmptyWriter))
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

    fn config() -> Arc<JobConfig> {
        Arc::new(
            JobConfig::parse_json(
                r#"{"source":{"name":"s","type":"fake","config":{}},"target":{"name":"t","type":"fake","config":{}},"column_mapping":{},"batch_size":1,"channel_buffer_size":1}"#,
            )
            .unwrap(),
        )
    }

    fn empty_coordinator() -> CoordinatorService {
        CoordinatorService::with_planning_dependencies(
            StateRepository::new(),
            Arc::new(EmptyPlanning),
        )
    }

    #[tokio::test]
    async fn submit_returns_handle_and_aggregates_empty_job() {
        let coordinator = empty_coordinator();
        let handle = coordinator.submit_job(config()).unwrap();
        assert!(coordinator.query_job(handle.id()).is_some());
        let result = handle.wait().await.unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Succeeded);
        assert_eq!(handle.state(), Some(JobState::SUCCEEDED));
        let shared = coordinator.job_handle(handle.id()).expect("shared handle");
        assert!(!shared.cancel());
        assert_eq!(shared.wait().await.unwrap(), result);
        assert_eq!(shared.snapshot(), handle.snapshot());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_publishes_result_only_after_progress_finished() {
        struct GatedFinish {
            entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            released: std::sync::Mutex<bool>,
            gate: std::sync::Condvar,
            finished: AtomicUsize,
        }
        impl ProgressObserver for GatedFinish {
            fn started(&self, _: Option<u64>) {}
            fn records_read(&self, _: u64) {}
            fn records_sent(&self, _: u64) {}
            fn finished(&self, _: ProgressOutcome, _: u64, _: u64) {
                self.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                let guard = self.released.lock().unwrap();
                let _guard = self
                    .gate
                    .wait_timeout_while(guard, std::time::Duration::from_secs(5), |released| {
                        !*released
                    })
                    .unwrap();
                self.finished.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let observer = Arc::new(GatedFinish {
            entered: std::sync::Mutex::new(Some(entered_tx)),
            released: std::sync::Mutex::new(false),
            gate: std::sync::Condvar::new(),
            finished: AtomicUsize::new(0),
        });
        let coordinator = empty_coordinator();
        let handle = coordinator
            .submit_job(
                JobSubmission::new(config())
                    .with_options(ExecutionOptions::default().with_progress(observer.clone())),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();
        let mut waiting = std::pin::pin!(handle.wait());
        let early_result = futures::poll!(waiting.as_mut());
        let early_snapshot = handle.snapshot().unwrap();
        *observer.released.lock().unwrap() = true;
        observer.gate.notify_all();
        assert!(
            early_result.is_pending(),
            "wait returned before finished: {early_result:?}"
        );
        assert!(early_snapshot.result.is_none());
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Succeeded);
        assert_eq!(observer.finished.load(Ordering::SeqCst), 1);
        assert_eq!(handle.wait().await.unwrap(), result);
    }

    #[derive(Clone, Copy)]
    enum ReadBehavior {
        Success,
        Pending,
    }

    #[tokio::test]
    async fn observer_failures_do_not_change_execution_or_strand_waiters() {
        struct PanickingProgress {
            event: &'static str,
            finished: AtomicUsize,
        }
        impl PanickingProgress {
            fn event(&self, event: &str) {
                assert_ne!(self.event, event, "synthetic observer failure");
            }
        }
        impl ProgressObserver for PanickingProgress {
            fn planned(&self, _: ProgressTopology) {
                self.event("planned");
            }
            fn started(&self, _: Option<u64>) {
                self.event("started");
            }
            fn records_read(&self, _: u64) {
                self.event("read");
            }
            fn records_sent(&self, _: u64) {
                self.event("sent");
            }
            fn finished(&self, _: ProgressOutcome, _: u64, _: u64) {
                self.finished.fetch_add(1, Ordering::SeqCst);
                self.event("finished");
            }
        }
        for event in ["planned", "started", "read", "sent", "finished"] {
            let observer = Arc::new(PanickingProgress {
                event,
                finished: AtomicUsize::new(0),
            });
            let coordinator = fake_coordinator(
                ReadBehavior::Success,
                false,
                false,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                1,
            );
            let handle = coordinator
                .submit_job(JobSubmission::new(config()).with_options(
                    // Literal options are intentionally supported as well as the builder.
                    ExecutionOptions {
                        progress: Some(observer.clone()),
                    },
                ))
                .unwrap();
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.status, EngineExecutionStatus::Succeeded, "{event}");
            assert_eq!((result.records_read, result.records_written), (1, 1));
            assert_eq!(observer.finished.load(Ordering::SeqCst), 1);
            assert_eq!(handle.wait().await.unwrap(), result);
        }
    }

    #[tokio::test]
    async fn topology_event_uses_physical_tasks_and_groups_not_configured_limits() {
        #[derive(Default)]
        struct TopologyProgress(std::sync::Mutex<Vec<String>>);
        impl ProgressObserver for TopologyProgress {
            fn planned(&self, topology: ProgressTopology) {
                self.0.lock().unwrap().push(format!(
                    "{}:{}:{}",
                    topology.readers, topology.writers, topology.workers
                ));
            }
            fn started(&self, _: Option<u64>) {
                self.0.lock().unwrap().push("started".into());
            }
            fn records_read(&self, _: u64) {}
            fn records_sent(&self, _: u64) {}
            fn finished(&self, _: ProgressOutcome, _: u64, _: u64) {
                self.0.lock().unwrap().push("finished".into());
            }
        }
        let observer = Arc::new(TopologyProgress::default());
        let coordinator = fake_coordinator(
            ReadBehavior::Success,
            false,
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            5,
        );
        let handle = coordinator
            .submit_job(
                JobSubmission::new(config())
                    .with_options(ExecutionOptions::default().with_progress(observer.clone())),
            )
            .unwrap();
        let result = handle.wait().await.unwrap();
        assert_eq!((result.records_read, result.records_written), (5, 5));
        assert_eq!(
            *observer.0.lock().unwrap(),
            ["5:5:1", "started", "finished"]
        );
        assert_eq!(handle.snapshot().unwrap().task_groups.len(), 1);
    }
    struct FakeReader {
        behavior: ReadBehavior,
        splits: Arc<AtomicUsize>,
        task_count: usize,
    }
    #[async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            self.splits.fetch_add(1, Ordering::SeqCst);
            Ok(SplitReaderResult {
                total_records: self.task_count,
                tasks: (0..self.task_count)
                    .map(|task_id| ReadTask {
                        task_id,
                        conn: serde_json::json!({}),
                        query_sql: None,
                        offset: 0,
                        limit: 1,
                    })
                    .collect(),
                stream_mode: StreamMode::Batch,
            })
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
    struct BlockingInitWriter {
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl DataWriterJob for BlockingInitWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            self.release.notified().await;
            Err(anyhow!("released initialization"))
        }
        fn description(&self) -> String {
            "blocking-init".into()
        }
    }
    #[async_trait]
    impl DataWriterTask for BlockingInitWriter {
        async fn write_data(
            &self,
            _: WriteTask,
            _: tokio::sync::mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            unreachable!()
        }
    }
    #[async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, count: usize) -> Result<SplitWriterResult> {
            if self.split_failure {
                return Err(anyhow!("fake initialization failure"));
            }
            let config = Arc::new(JobConfig::parse_json(
                r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{}}"#,
            )?);
            Ok(SplitWriterResult {
                tasks: (0..count)
                    .map(|task_id| WriteTask {
                        task_id,
                        config: config.clone(),
                        mode: WriteMode::Insert,
                        use_transaction: false,
                        batch_size: 1,
                    })
                    .collect(),
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
    struct FakePlanning {
        behavior: ReadBehavior,
        split_failure: bool,
        write_failure: bool,
        splits: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
        task_count: usize,
        blocking_release: Option<Arc<tokio::sync::Notify>>,
    }

    impl PlanningDependencies for FakePlanning {
        fn create_reader(&self, _: Arc<JobConfig>) -> Result<relus_reader::Source> {
            Ok(Arc::new(FakeReader {
                behavior: self.behavior,
                splits: Arc::clone(&self.splits),
                task_count: self.task_count,
            }))
        }

        fn create_writer(&self, _: Arc<JobConfig>) -> Result<relus_writer::Sink> {
            if let Some(release) = &self.blocking_release {
                return Ok(Arc::new(BlockingInitWriter {
                    release: Arc::clone(release),
                }));
            }
            Ok(Arc::new(FakeWriter {
                split_failure: self.split_failure,
                write_failure: self.write_failure,
                writes: Arc::clone(&self.writes),
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

    fn fake_coordinator(
        behavior: ReadBehavior,
        split_failure: bool,
        write_failure: bool,
        splits: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
        task_count: usize,
    ) -> CoordinatorService {
        CoordinatorService::with_planning_dependencies(
            StateRepository::new(),
            Arc::new(FakePlanning {
                behavior,
                split_failure,
                write_failure,
                splits,
                writes,
                task_count,
                blocking_release: None,
            }),
        )
    }

    #[tokio::test]
    async fn submit_executes_prepared_plan_and_repeated_wait_is_stable() {
        let splits = Arc::new(AtomicUsize::new(0));
        let writes = Arc::new(AtomicUsize::new(0));
        let coordinator = fake_coordinator(
            ReadBehavior::Success,
            false,
            false,
            Arc::clone(&splits),
            Arc::clone(&writes),
            1,
        );
        let handle = coordinator.submit_job(config()).unwrap();
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
        assert_eq!(splits.load(Ordering::SeqCst), 1);
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
    async fn submission_options_aggregate_progress_events() {
        let progress = Arc::new(RecordingProgress::default());
        let coordinator = fake_coordinator(
            ReadBehavior::Success,
            false,
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            1,
        );
        let handle = coordinator
            .submit_job(
                JobSubmission::new(config())
                    .with_options(ExecutionOptions::default().with_progress(progress.clone())),
            )
            .unwrap();
        assert_eq!(
            handle.wait().await.unwrap().status,
            EngineExecutionStatus::Succeeded
        );
        assert_eq!(progress.started.load(Ordering::SeqCst), 1);
        assert_eq!(progress.read.load(Ordering::SeqCst), 1);
        assert_eq!(progress.sent.load(Ordering::SeqCst), 1);
        assert_eq!(progress.finished.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn submit_returns_before_initialization_completes() {
        let release = Arc::new(tokio::sync::Notify::new());
        let coordinator = CoordinatorService::with_planning_dependencies(
            StateRepository::new(),
            Arc::new(FakePlanning {
                behavior: ReadBehavior::Success,
                split_failure: false,
                write_failure: false,
                splits: Arc::new(AtomicUsize::new(0)),
                writes: Arc::new(AtomicUsize::new(0)),
                task_count: 1,
                blocking_release: Some(Arc::clone(&release)),
            }),
        );
        let handle = coordinator.submit_job(config()).unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), handle.wait())
                .await
                .is_err()
        );
        release.notify_one();
        assert_eq!(
            handle.wait().await.unwrap().status,
            EngineExecutionStatus::Failed
        );
    }

    #[tokio::test]
    async fn initialization_failure_is_saved_in_snapshot() {
        let coordinator = fake_coordinator(
            ReadBehavior::Success,
            true,
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            1,
        );
        let handle = coordinator.submit_job(config()).unwrap();
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
        let coordinator = fake_coordinator(
            ReadBehavior::Success,
            false,
            true,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            1,
        );
        let handle = coordinator.submit_job(config()).unwrap();
        let result = handle.wait().await.unwrap();
        assert_eq!(result.status, EngineExecutionStatus::Failed);
        let shared = coordinator.job_handle(handle.id()).expect("shared handle");
        assert!(!shared.cancel());
        assert_eq!(shared.wait().await.unwrap(), result);
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
        let coordinator = fake_coordinator(
            ReadBehavior::Pending,
            false,
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            1,
        );
        let handle = coordinator.submit_job(config()).unwrap();
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
    planning: Option<Arc<dyn PlanningDependencies>>,
    execution: Arc<TaskExecutionService>,
    jobs: Arc<RwLock<HashMap<JobId, CancellationToken>>>,
    results: Arc<RwLock<HashMap<JobId, EngineResultStore>>>,
}

impl CoordinatorService {
    pub fn new(repository: StateRepository) -> Self {
        Self::build(repository, None)
    }

    pub fn with_planning_dependencies(
        repository: StateRepository,
        planning: Arc<dyn PlanningDependencies>,
    ) -> Self {
        Self::build(repository, Some(planning))
    }

    fn build(repository: StateRepository, planning: Option<Arc<dyn PlanningDependencies>>) -> Self {
        let runtime: Arc<dyn Runtime> = Arc::new(TokioRuntime::new());
        Self {
            planning,
            execution: Arc::new(TaskExecutionService::new(repository.clone(), runtime)),
            repository,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            results: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub fn repository(&self) -> &StateRepository {
        &self.repository
    }
    pub fn submit_job<S>(&self, submission: S) -> Result<JobHandle>
    where
        S: Into<JobSubmission>,
    {
        let submission = submission.into();
        self.submit_job_with_options(submission.config, submission.options)
    }

    pub fn submit_job_with_options(
        &self,
        config: Arc<JobConfig>,
        options: ExecutionOptions,
    ) -> Result<JobHandle> {
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
            this.execute_and_store(
                id,
                config,
                token,
                options.progress.map(SafeProgressObserver::wrap),
            )
            .await;
            this.jobs.write().remove(&id);
        });
        Ok(handle)
    }

    pub fn job_handle(&self, id: JobId) -> Option<JobHandle> {
        let store = self.results.read().get(&id).cloned()?;
        let token = self.jobs.read().get(&id).cloned().unwrap_or_default();
        self.repository
            .job(id)
            .map(|_| JobHandle::new(id, self.repository.clone(), token, store))
    }

    async fn execute_and_store(
        &self,
        id: JobId,
        config: Arc<JobConfig>,
        token: CancellationToken,
        progress: Option<Arc<dyn ProgressObserver>>,
    ) {
        let started = Instant::now();
        let store = self.results.read().get(&id).cloned();
        let progress_for_finish = progress.clone();
        let finish = |this: &CoordinatorService,
                      state: JobState,
                      mut result: EngineExecutionResult,
                      store: Option<EngineResultStore>| {
            if let Err(error) = this.transition_job(id, state) {
                result.status = EngineExecutionStatus::Failed;
                result.cancelled = false;
                result.error = Some(format!("failed to persist job lifecycle: {error}"));
            }
            let outcome = match result.status {
                EngineExecutionStatus::Succeeded => ProgressOutcome::Succeeded,
                EngineExecutionStatus::Failed => ProgressOutcome::Failed,
                EngineExecutionStatus::Cancelled => ProgressOutcome::Cancelled,
            };
            let records_read = result.records_read as u64;
            let records_written = result.records_written as u64;
            if let Some(observer) = &progress_for_finish {
                observer.finished(outcome, records_read, records_written);
            }
            // A returned result authorizes CLI callers to print their summary.
            // Finish terminal output first so it cannot redraw over that summary.
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
                    stream_mode: StreamMode::Batch,
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
                    stream_mode: StreamMode::Batch,
                    elapsed: started.elapsed(),
                    error: Some(format!("failed to initialize job lifecycle: {error}")),
                },
                store,
            );
            return;
        }
        let master = match &self.planning {
            Some(planning) => {
                JobMaster::with_planning_dependencies(self.repository.clone(), Arc::clone(planning))
            }
            None => JobMaster::new(self.repository.clone()),
        };
        let initialization = master.initialize(config, id);
        tokio::pin!(initialization);
        let runtime = match tokio::select! {
            result = &mut initialization => result,
            _ = token.cancelled() => {
                finish(
                    self,
                    JobState::CANCELLED,
                    EngineExecutionResult {
                        status: EngineExecutionStatus::Cancelled,
                        records_read: 0,
                        records_written: 0,
                        records_failed: 0,
                        cancelled: true,
                        stream_mode: StreamMode::Batch,
                        elapsed: started.elapsed(),
                        error: Some("Shutdown".into()),
                    },
                    store,
                );
                return;
            }
        } {
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
                        stream_mode: StreamMode::Batch,
                        elapsed: started.elapsed(),
                        error: Some(message),
                    },
                    store,
                );
                return;
            }
        };
        let planned_total = runtime.total_records as u64;
        let stream_mode = runtime.stream_mode;
        let planned_streaming = stream_mode == StreamMode::Streaming;
        if let Some(observer) = &progress {
            let task_count = runtime.groups.iter().map(|group| group.tasks.len()).sum();
            observer.planned(ProgressTopology {
                readers: task_count,
                writers: task_count,
                workers: runtime.groups.len(),
            });
            if !runtime.groups.is_empty() {
                observer.started((!planned_streaming).then_some(planned_total));
            }
        }
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
                    stream_mode,
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
                        stream_mode,
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
                    stream_mode,
                    elapsed: started.elapsed(),
                    error: cancelled.then(|| "Shutdown".into()),
                },
                store,
            );
            return;
        }
        let mut handles = Vec::new();
        let mut deployment_failure = None;
        for group in runtime.groups {
            let group_id = group.group.id;
            let context = WorkerContext {
                reader: Arc::clone(&runtime.reader),
                writer: Arc::clone(&runtime.writer),
                pipeline: runtime.pipeline.clone(),
                record_builder: Arc::clone(&runtime.record_builder),
                progress: progress.clone(),
            };
            match self.execution.deploy(group, context, token.clone()) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    let message = error.to_string();
                    let cancelled = token.is_cancelled();
                    token.cancel();
                    deployment_failure = Some(TaskGroupExecutionResult {
                        group_id,
                        status: if cancelled {
                            super::task_execution::TaskGroupExecutionStatus::Cancelled
                        } else {
                            super::task_execution::TaskGroupExecutionStatus::Failed
                        },
                        records_read: 0,
                        records_written: 0,
                        records_failed: 0,
                        cancelled,
                        error_summary: Some(if cancelled {
                            "Shutdown".into()
                        } else {
                            message
                        }),
                        elapsed: started.elapsed(),
                    });
                    break;
                }
            }
        }
        if let Some(failure) = deployment_failure.as_mut() {
            if let Err(error) = self.fail_unstarted_groups(id) {
                let message = failure.error_summary.get_or_insert_default();
                message.push_str(&format!("; failed to converge unstarted groups: {error}"));
            }
        }
        if deployment_failure.is_none() {
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
                        stream_mode,
                        elapsed: started.elapsed(),
                        error: Some(format!("failed to persist RUNNING lifecycle: {error}")),
                    },
                    store,
                );
                return;
            }
        }
        let mut results = Vec::new();
        results.extend(deployment_failure);
        for handle in handles {
            let group_id = handle.group_id();
            match handle.join().await {
                Ok(result) => results.push(result),
                Err(error) => {
                    let mut message = error.to_string();
                    if let Err(state_error) = self
                        .repository
                        .update_task_group(group_id, TaskGroupState::FAILED)
                    {
                        message
                            .push_str(&format!("; failed to persist group failure: {state_error}"));
                    }
                    for task in self.repository.tasks(group_id) {
                        if let Err(state_error) =
                            self.repository.update_task(task.id, TaskState::FAILED)
                        {
                            message.push_str(&format!(
                                "; failed to persist task failure: {state_error}"
                            ));
                        }
                    }
                    results.push(TaskGroupExecutionResult {
                        group_id,
                        status: super::task_execution::TaskGroupExecutionStatus::Failed,
                        records_read: 0,
                        records_written: 0,
                        records_failed: 0,
                        cancelled: false,
                        error_summary: Some(message),
                        elapsed: started.elapsed(),
                    });
                }
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
        let final_result = EngineExecutionResult {
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
            stream_mode,
            elapsed: started.elapsed(),
            error: if cancelled {
                Some("Shutdown".into())
            } else {
                error
            },
        };
        finish(self, state, final_result, store);
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

    fn fail_unstarted_groups(&self, id: JobId) -> Result<()> {
        for group in self.repository.task_groups(id) {
            if group.state != TaskGroupState::CREATED {
                continue;
            }
            self.repository
                .update_task_group(group.id, TaskGroupState::SUBMITTED)?;
            self.repository
                .update_task_group(group.id, TaskGroupState::FAILED)?;
            for task in self.repository.tasks(group.id) {
                self.repository.update_task(task.id, TaskState::SUBMITTED)?;
                self.repository.update_task(task.id, TaskState::FAILED)?;
            }
        }
        Ok(())
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
