//! Per-job configuration preparation and conversion into a resource-bound
//! physical plan.

use anyhow::{anyhow, Context, Result};
use relus_common::app_config::value::ConfigValue;
use relus_common::job_config::JobConfig;
use relus_common::pipeline::PipelineConfig;
use relus_reader::{ReadTask, ReaderRegistry, Source, SplitReaderResult, StreamMode};
use relus_writer::{Sink, WriteTask, WriterRegistry};
use std::sync::Arc;

use super::state::{Job, JobId, StateRepository, Task, TaskGroup, TaskGroupId};
use crate::pipeline::RecordBuilder;

/// Internal seam used to replace Registry-backed preparation in tests.
pub trait PlanningDependencies: Send + Sync {
    fn create_reader(&self, config: Arc<JobConfig>) -> Result<Source>;
    fn create_writer(&self, config: Arc<JobConfig>) -> Result<Sink>;
    fn build_record_builder(&self, config: &JobConfig) -> Result<RecordBuilder>;
}

struct RegistryPlanningDependencies;

impl PlanningDependencies for RegistryPlanningDependencies {
    fn create_reader(&self, config: Arc<JobConfig>) -> Result<Source> {
        let source_type = config.source.source_type.clone();
        ReaderRegistry::instance()
            .prepare_reader(&source_type, config)
            .context("registry Reader creation failed")
    }

    fn create_writer(&self, config: Arc<JobConfig>) -> Result<Sink> {
        let source_type = config.sink.source_type.clone();
        WriterRegistry::instance()
            .prepare_writer(&source_type, config)
            .context("registry Writer creation failed")
    }

    fn build_record_builder(&self, config: &JobConfig) -> Result<RecordBuilder> {
        let source_type = config
            .source
            .source_type
            .parse()
            .unwrap_or_else(|err| match err {});
        Ok(
            RecordBuilder::new(config.column_mapping.clone(), config.column_types.clone())?
                .with_source_type(source_type),
        )
    }
}

/// Single-use prepared input for physical topology construction.
///
/// This is deliberately private: callers submit `JobConfig`, while JobMaster
/// owns both preparation and resource binding.
struct PreparedExecutionPlan {
    reader: Source,
    writer: Sink,
    pipeline: PipelineConfig,
    record_builder: Arc<RecordBuilder>,
    reader_split: SplitReaderResult,
    stream_mode: StreamMode,
}

/// A task pair and its lifecycle entry.  The descriptors are immutable; only
/// the state entry is updated by later execution services.
#[derive(Debug, Clone)]
pub struct RuntimeTask {
    pub task: Task,
    pub read_task: ReadTask,
    pub write_task: WriteTask,
}

#[derive(Debug, Clone)]
pub struct RuntimeTaskGroup {
    pub group: TaskGroup,
    pub tasks: Vec<RuntimeTask>,
    pub concurrency: usize,
}

/// A resource-bound physical execution plan produced by `JobMaster`.
///
/// `RuntimeJob` contains the concrete task topology, resources and allocation
/// decisions required for deployment. Lifecycle state is held by the
/// contained entities and `StateRepository`.
#[derive(Clone)]
pub struct RuntimeJob {
    pub job: Job,
    pub groups: Vec<RuntimeTaskGroup>,
    pub reader: relus_reader::Source,
    pub writer: relus_writer::Sink,
    pub pipeline: relus_common::pipeline::PipelineConfig,
    pub record_builder: std::sync::Arc<crate::pipeline::RecordBuilder>,
    pub total_records: usize,
    pub stream_mode: relus_reader::StreamMode,
}

/// Owns the complete initialization of one submitted Job.
///
/// The Coordinator creates one `JobMaster` per Job. The master validates the
/// configuration, prepares execution resources, and binds them into the
/// runtime task topology consumed by execution services.
#[derive(Clone)]
pub struct JobMaster {
    repository: StateRepository,
    planning: Arc<dyn PlanningDependencies>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;
    use relus_common::{job_config::JobConfig, PipelineMessage};
    use relus_reader::{DataReaderJob, DataReaderTask, JsonStream, SplitReaderResult, StreamMode};
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult};
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::sync::mpsc;

    const VALID_JOB: &str = r#"{
        "source":{"name":"fake-source","type":"fake","config":{}},
        "target":{"name":"fake-target","type":"fake","config":{}},
        "column_mapping":{},"column_types":null,
        "batch_size":10,"channel_buffer_size":10
    }"#;

    struct FakeReader {
        splits: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            self.splits.fetch_add(1, Ordering::SeqCst);
            Ok(SplitReaderResult {
                total_records: 0,
                tasks: vec![],
                stream_mode: StreamMode::Batch,
            })
        }
        fn description(&self) -> String {
            "fake".into()
        }
    }
    #[async_trait]
    impl DataReaderTask for FakeReader {
        async fn read_data(&self, _: &relus_reader::ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }
    struct FakeWriter {
        count: usize,
    }
    #[async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, n: usize) -> Result<SplitWriterResult> {
            let cfg = Arc::new(JobConfig::parse_json(r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{}}"#).unwrap());
            Ok(SplitWriterResult {
                tasks: (0..self.count.min(n))
                    .map(|i| relus_writer::WriteTask {
                        task_id: i,
                        config: Arc::clone(&cfg),
                        mode: relus_common::job_config::WriteMode::Insert,
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
            _: mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            Ok(0)
        }
    }

    fn plan(
        read_count: usize,
        writer_count: usize,
        splits: Arc<AtomicUsize>,
    ) -> PreparedExecutionPlan {
        let rb =
            crate::pipeline::RecordBuilder::new(std::collections::BTreeMap::new(), None).unwrap();
        PreparedExecutionPlan {
            reader: Arc::new(FakeReader { splits }),
            writer: Arc::new(FakeWriter {
                count: writer_count,
            }),
            pipeline: relus_common::pipeline::PipelineConfig {
                channel_number: 2,
                per_group_channel: 1,
                ..Default::default()
            },
            record_builder: Arc::new(rb),
            reader_split: SplitReaderResult {
                total_records: read_count,
                tasks: (0..read_count)
                    .map(|i| relus_reader::ReadTask {
                        task_id: i,
                        conn: json!({}),
                        query_sql: None,
                        offset: 0,
                        limit: 1,
                    })
                    .collect(),
                stream_mode: StreamMode::Batch,
            },
            stream_mode: StreamMode::Batch,
        }
    }

    #[tokio::test]
    async fn creates_pairs_groups_and_created_states_without_reader_split() {
        let splits = Arc::new(AtomicUsize::new(0));
        let runtime = JobMaster::new(StateRepository::new())
            .build(plan(3, 3, Arc::clone(&splits)))
            .await
            .unwrap();
        assert_eq!(splits.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.groups.len(), 2);
        assert_eq!(
            runtime.groups.iter().map(|g| g.tasks.len()).sum::<usize>(),
            3
        );
        assert!(runtime
            .groups
            .iter()
            .all(|g| g.group.state == super::super::state::TaskGroupState::CREATED));
        assert!(runtime
            .groups
            .iter()
            .flat_map(|g| &g.tasks)
            .all(|t| t.task.state == super::super::state::TaskState::CREATED));
    }

    #[tokio::test]
    async fn empty_split_produces_empty_groups() {
        let splits = Arc::new(AtomicUsize::new(0));
        let runtime = JobMaster::new(StateRepository::new())
            .build(plan(0, 0, splits))
            .await
            .unwrap();
        assert!(runtime.groups.is_empty());
    }

    struct FakePlanning {
        factory_calls: Arc<AtomicUsize>,
        split_count: Arc<AtomicUsize>,
        mode: StreamMode,
        fail_builder: bool,
    }

    struct PlanningReader {
        split_count: Arc<AtomicUsize>,
        mode: StreamMode,
    }

    #[async_trait]
    impl DataReaderJob for PlanningReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            self.split_count.fetch_add(1, Ordering::SeqCst);
            Ok(SplitReaderResult {
                total_records: 0,
                tasks: vec![],
                stream_mode: self.mode,
            })
        }

        fn description(&self) -> String {
            "planning fake reader".into()
        }
    }

    #[async_trait]
    impl DataReaderTask for PlanningReader {
        async fn read_data(&self, _: &ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    struct PlanningWriter;

    #[async_trait]
    impl DataWriterJob for PlanningWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            Ok(SplitWriterResult { tasks: vec![] })
        }

        fn description(&self) -> String {
            "planning fake writer".into()
        }
    }

    #[async_trait]
    impl DataWriterTask for PlanningWriter {
        async fn write_data(
            &self,
            _: WriteTask,
            _: mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            Ok(0)
        }
    }

    impl PlanningDependencies for FakePlanning {
        fn create_reader(&self, _: Arc<JobConfig>) -> Result<Source> {
            self.factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(PlanningReader {
                split_count: Arc::clone(&self.split_count),
                mode: self.mode,
            }))
        }

        fn create_writer(&self, _: Arc<JobConfig>) -> Result<Sink> {
            self.factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(PlanningWriter))
        }

        fn build_record_builder(&self, config: &JobConfig) -> Result<RecordBuilder> {
            if self.fail_builder {
                return Err(anyhow!("fake mapping error"));
            }
            RecordBuilder::new(config.column_mapping.clone(), config.column_types.clone())
        }
    }

    fn planning(mode: StreamMode) -> (Arc<FakePlanning>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let split_count = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(FakePlanning {
                factory_calls: Arc::clone(&factory_calls),
                split_count: Arc::clone(&split_count),
                mode,
                fail_builder: false,
            }),
            factory_calls,
            split_count,
        )
    }

    #[tokio::test]
    async fn initialize_prepares_resources_and_splits_reader_once() {
        let (planning, factory_calls, split_count) = planning(StreamMode::Streaming);
        let master = JobMaster::with_planning_dependencies(StateRepository::new(), planning);
        let runtime = master
            .initialize(
                Arc::new(JobConfig::parse_json(VALID_JOB).unwrap()),
                JobId::new(),
            )
            .await
            .unwrap();

        assert_eq!(runtime.stream_mode, StreamMode::Streaming);
        assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
        assert_eq!(split_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn preparation_failure_keeps_jobmaster_context() {
        let planning = Arc::new(FakePlanning {
            factory_calls: Arc::new(AtomicUsize::new(0)),
            split_count: Arc::new(AtomicUsize::new(0)),
            mode: StreamMode::Batch,
            fail_builder: true,
        });
        let master = JobMaster::with_planning_dependencies(StateRepository::new(), planning);
        let error = master
            .initialize(
                Arc::new(JobConfig::parse_json(VALID_JOB).unwrap()),
                JobId::new(),
            )
            .await
            .err()
            .unwrap();

        assert!(error.to_string().contains("build RecordBuilder"));
    }

    #[tokio::test]
    async fn invalid_database_config_fails_before_resource_creation() {
        let (planning, factory_calls, split_count) = planning(StreamMode::Batch);
        let master = JobMaster::with_planning_dependencies(StateRepository::new(), planning);
        let config = Arc::new(
            JobConfig::parse_json(
                r#"{
                    "source": {
                        "name": "database-source",
                        "type": "database",
                        "config": {"connections": [{}]}
                    },
                    "sink": {"name": "fake-target", "type": "fake", "config": {}},
                    "column_mapping": {},
                    "column_types": null
                }"#,
            )
            .unwrap(),
        );

        let error = master.initialize(config, JobId::new()).await.err().unwrap();

        assert!(error.to_string().contains("table"));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert_eq!(split_count.load(Ordering::SeqCst), 0);
    }
}

impl JobMaster {
    pub fn new(repository: StateRepository) -> Self {
        Self::with_planning_dependencies(repository, Arc::new(RegistryPlanningDependencies))
    }

    pub fn with_planning_dependencies(
        repository: StateRepository,
        planning: Arc<dyn PlanningDependencies>,
    ) -> Self {
        Self {
            repository,
            planning,
        }
    }

    #[cfg(test)]
    async fn build(&self, plan: PreparedExecutionPlan) -> Result<RuntimeJob> {
        self.build_with_job_id(plan, JobId::new()).await
    }

    async fn build_with_job_id(
        &self,
        plan: PreparedExecutionPlan,
        job_id: JobId,
    ) -> Result<RuntimeJob> {
        let PreparedExecutionPlan {
            reader,
            writer,
            pipeline,
            record_builder,
            reader_split,
            stream_mode,
        } = plan;
        let task_count = reader_split.tasks.len();
        let total_records = reader_split.total_records;
        let writer_split = if task_count == 0 {
            relus_writer::SplitWriterResult { tasks: Vec::new() }
        } else {
            writer.split(task_count).await?
        };
        if writer_split.tasks.len() < task_count {
            return Err(anyhow!(
                "Writer 只产出 {} 个任务，但有 {} 个 Reader 任务.",
                writer_split.tasks.len(),
                task_count
            ));
        }

        let job = Job::new(job_id);
        if self.repository.job(job.id).is_none() {
            self.repository.register_job(job.clone())?;
        }

        let groups = if task_count == 0 {
            Vec::new()
        } else {
            let need_channel = pipeline.channel_number.max(1).min(task_count);
            let per_group = pipeline.per_group_channel.max(1);
            let group_count = need_channel.div_ceil(per_group);
            let base = need_channel / group_count;
            let extra = need_channel % group_count;
            let groups_state: Vec<TaskGroup> = (0..group_count)
                .map(|_| TaskGroup::new(TaskGroupId::new(), job.id))
                .collect();
            for group in &groups_state {
                self.repository.register_task_group(group.clone())?;
            }
            let mut grouped: Vec<Vec<RuntimeTask>> = (0..group_count).map(|_| Vec::new()).collect();
            for (i, read_task) in reader_split.tasks.into_iter().enumerate() {
                let group_index = i % group_count;
                let state_task =
                    Task::new(super::state::TaskId::new(), groups_state[group_index].id);
                self.repository.register_task(state_task.clone())?;
                grouped[group_index].push(RuntimeTask {
                    task: state_task,
                    read_task,
                    write_task: writer_split.tasks[i].clone(),
                });
            }
            groups_state
                .into_iter()
                .zip(grouped)
                .enumerate()
                .map(|(index, (group, tasks))| {
                    Ok(RuntimeTaskGroup {
                        group,
                        tasks,
                        concurrency: base + usize::from(index < extra),
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };

        Ok(RuntimeJob {
            job,
            groups,
            reader,
            writer,
            pipeline,
            record_builder,
            total_records,
            stream_mode,
        })
    }

    pub async fn initialize(&self, config: Arc<JobConfig>, job_id: JobId) -> Result<RuntimeJob> {
        let plan = self.prepare(config).await?;
        self.build_with_job_id(plan, job_id).await
    }

    async fn prepare(&self, config: Arc<JobConfig>) -> Result<PreparedExecutionPlan> {
        validate_config(&config)
            .map_err(|error| anyhow!("planning: validate JobConfig: {error}"))?;
        let record_builder = Arc::new(
            self.planning
                .build_record_builder(&config)
                .context("planning: build RecordBuilder")?,
        );
        let pipeline = pipeline_config_from_system_config(&config);
        pipeline
            .validate()
            .context("planning: validate pipeline config")?;
        let reader = self
            .planning
            .create_reader(Arc::clone(&config))
            .context("planning: create Reader")?;
        let writer = self
            .planning
            .create_writer(Arc::clone(&config))
            .context("planning: create Writer")?;
        let reader_split = reader
            .split(pipeline.reader_threads)
            .await
            .context("planning: split Reader")?;
        let stream_mode = reader_split.stream_mode;
        Ok(PreparedExecutionPlan {
            reader,
            writer,
            pipeline,
            record_builder,
            reader_split,
            stream_mode,
        })
    }
}

fn pipeline_config_from_system_config(job_config: &JobConfig) -> PipelineConfig {
    let defaults = PipelineConfig::default();
    let (sys_reader, sys_buffer, sys_channel, sys_per_group, sys_batch, sys_tx) =
        relus_common::app_config::config_loader::get_config_manager()
            .map(|mgr| {
                let m = mgr.read();
                (
                    m.get("pipeline.reader_threads").and_then(as_positive_usize),
                    m.get("pipeline.buffer_size").and_then(as_positive_usize),
                    m.get("pipeline.channel_number").and_then(as_positive_usize),
                    m.get("pipeline.per_group_channel")
                        .and_then(as_positive_usize),
                    m.get("pipeline.batch_size").and_then(as_positive_usize),
                    m.get("pipeline.use_transaction")
                        .and_then(ConfigValue::as_bool),
                )
            })
            .unwrap_or((None, None, None, None, None, None));

    PipelineConfig {
        reader_threads: sys_reader.unwrap_or(defaults.reader_threads),
        buffer_size: job_config
            .channel_buffer_size
            .or(sys_buffer)
            .unwrap_or(defaults.buffer_size),
        channel_number: sys_channel.unwrap_or(defaults.channel_number),
        per_group_channel: sys_per_group.unwrap_or(defaults.per_group_channel),
        batch_size: job_config
            .batch_size
            .or(sys_batch)
            .unwrap_or(defaults.batch_size),
        use_transaction: sys_tx.unwrap_or(defaults.use_transaction),
    }
}

fn as_positive_usize(value: &ConfigValue) -> Option<usize> {
    value
        .as_i64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn validate_config(config: &JobConfig) -> Result<()> {
    anyhow::ensure!(
        !config.source.source_type.trim().is_empty(),
        "source.source_type must not be empty"
    );
    anyhow::ensure!(
        !config.sink.source_type.trim().is_empty(),
        "sink.source_type must not be empty"
    );
    if let Some(batch_size) = config.batch_size {
        anyhow::ensure!(batch_size > 0, "batch_size must be greater than zero");
    }
    if let Some(buffer_size) = config.channel_buffer_size {
        anyhow::ensure!(
            buffer_size > 0,
            "channel_buffer_size must be greater than zero"
        );
    }
    for data_source in [&config.source, &config.sink] {
        if data_source.source_type == "database" {
            let database_config = data_source
                .parse_database_config()
                .context("invalid database config")?;
            relus_connector_rdbms::identifier::validate_database_config(&database_config)?;
        }
    }
    if config.sink.source_type == "database" {
        for field in config.column_mapping.keys() {
            relus_connector_rdbms::identifier::validate(field)?;
        }
    }
    Ok(())
}
