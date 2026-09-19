//! Configuration parsing and preparation boundary.

use anyhow::{anyhow, Context, Result};
use relus_common::job_config::JobConfig;
use relus_reader::{DataReader, ReaderRegistry, SplitReaderResult, StreamMode};
use relus_writer::{DataWriter, WriterRegistry};
use std::sync::Arc;

use crate::pipeline::PipelineConfig;
use crate::pipeline::RecordBuilder;

pub trait PlanningDependencies: Send + Sync {
    fn create_reader(&self, config: Arc<JobConfig>) -> Result<Arc<dyn DataReader>>;
    fn create_writer(&self, config: Arc<JobConfig>) -> Result<Arc<dyn DataWriter>>;
    fn build_record_builder(&self, config: &JobConfig) -> Result<RecordBuilder>;
}

pub struct RegistryPlanningDependencies;

impl PlanningDependencies for RegistryPlanningDependencies {
    fn create_reader(&self, config: Arc<JobConfig>) -> Result<Arc<dyn DataReader>> {
        let source_type = config.source.source_type.clone();
        ReaderRegistry::instance()
            .prepare_reader(&source_type, config)
            .context("registry Reader creation failed")
    }

    fn create_writer(&self, config: Arc<JobConfig>) -> Result<Arc<dyn DataWriter>> {
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

/// A single-use Prepared Execution Plan produced from a validated JobConfig.
///
/// The resource-bound physical plan is the `RuntimeJob` built by `JobMaster`.
pub struct ExecutionPlan {
    /// The Registry selects the adapter at runtime, while `Arc` shares the
    /// selected instance across tasks. Making downstream functions generic
    /// over this already type-erased value would not restore static dispatch;
    /// unlike `AsRef<Path>`, this is behavior polymorphism, not input conversion.
    pub reader: Arc<dyn DataReader>,
    /// Writer counterpart to `reader`: `dyn` erases the runtime-selected type
    /// and `Arc` provides shared ownership across task execution.
    pub writer: Arc<dyn DataWriter>,
    pub pipeline: PipelineConfig,
    pub record_builder: Arc<RecordBuilder>,
    pub reader_split: SplitReaderResult,
    pub stream_mode: StreamMode,
}

/// Converts a parsed job configuration into the objects required by execution.
pub struct Planner;

impl Planner {
    pub async fn prepare(config: Arc<JobConfig>) -> Result<ExecutionPlan> {
        Self::prepare_with(config, &RegistryPlanningDependencies).await
    }

    pub(crate) async fn prepare_with(
        config: Arc<JobConfig>,
        dependencies: &dyn PlanningDependencies,
    ) -> Result<ExecutionPlan> {
        validate_config(&config)
            .map_err(|error| anyhow!("planning: validate JobConfig: {error}"))?;
        let record_builder = Arc::new(
            dependencies
                .build_record_builder(&config)
                .context("planning: build RecordBuilder")?,
        );
        let pipeline = PipelineConfig::from_system_config(&config);
        pipeline
            .validate()
            .context("planning: validate pipeline config")?;
        let reader = dependencies
            .create_reader(Arc::clone(&config))
            .context("planning: create Reader")?;
        let writer = dependencies
            .create_writer(Arc::clone(&config))
            .context("planning: create Writer")?;
        let reader_split = reader
            .split(pipeline.reader_threads)
            .await
            .context("planning: split Reader")?;
        let stream_mode = reader_split.stream_mode;
        Ok(ExecutionPlan {
            reader,
            writer,
            pipeline,
            record_builder,
            reader_split,
            stream_mode,
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use relus_common::PipelineMessage;
    use relus_reader::{DataReaderJob, DataReaderTask, JsonStream, ReadTask};
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult, WriteTask};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    const VALID_JOB: &str = r#"{
        "source":{"name":"fake-source","type":"fake","config":{}},
        "target":{"name":"fake-target","type":"fake","config":{}},
        "column_mapping":{},"column_types":null,
        "batch_size":10,"channel_buffer_size":10
    }"#;

    struct FakeReader {
        split_count: Arc<AtomicUsize>,
        mode: StreamMode,
    }

    #[async_trait::async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _reader_threads: usize) -> Result<SplitReaderResult> {
            self.split_count.fetch_add(1, Ordering::SeqCst);
            Ok(SplitReaderResult {
                total_records: 0,
                tasks: vec![],
                stream_mode: self.mode,
            })
        }
        fn description(&self) -> String {
            "fake reader".into()
        }
    }

    #[async_trait::async_trait]
    impl DataReaderTask for FakeReader {
        async fn read_data(&self, _task: &ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    struct FakeWriter;

    #[async_trait::async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, _writer_threads: usize) -> Result<SplitWriterResult> {
            Ok(SplitWriterResult { tasks: vec![] })
        }
        fn description(&self) -> String {
            "fake writer".into()
        }
    }

    #[async_trait::async_trait]
    impl DataWriterTask for FakeWriter {
        async fn write_data(
            &self,
            _task: WriteTask,
            _rx: mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            Ok(0)
        }
    }

    struct FakeDependencies {
        factory_calls: Arc<AtomicUsize>,
        split_count: Arc<AtomicUsize>,
        mode: StreamMode,
        fail_builder: bool,
    }

    impl PlanningDependencies for FakeDependencies {
        fn create_reader(&self, _config: Arc<JobConfig>) -> Result<Arc<dyn DataReader>> {
            self.factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(FakeReader {
                split_count: Arc::clone(&self.split_count),
                mode: self.mode,
            }))
        }
        fn create_writer(&self, _config: Arc<JobConfig>) -> Result<Arc<dyn DataWriter>> {
            self.factory_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(FakeWriter))
        }
        fn build_record_builder(&self, config: &JobConfig) -> Result<RecordBuilder> {
            if self.fail_builder {
                return Err(anyhow!("fake mapping error"));
            }
            RecordBuilder::new(config.column_mapping.clone(), config.column_types.clone())
        }
    }

    fn dependencies(mode: StreamMode) -> (FakeDependencies, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let split_count = Arc::new(AtomicUsize::new(0));
        (
            FakeDependencies {
                factory_calls: Arc::clone(&factory_calls),
                split_count: Arc::clone(&split_count),
                mode,
                fail_builder: false,
            },
            factory_calls,
            split_count,
        )
    }

    #[test]
    fn malformed_json_fails_before_factories() {
        let (deps, factory_calls, _) = dependencies(StreamMode::Batch);
        let error = JobConfig::parse_json("{").unwrap_err();
        assert!(error.to_string().contains("config.parse.json"));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        drop(deps);
    }

    #[tokio::test]
    async fn planning_prepares_mode_and_split_once() {
        let (deps, factory_calls, split_count) = dependencies(StreamMode::Streaming);
        let config = Arc::new(JobConfig::parse_json(VALID_JOB).unwrap());
        let plan = Planner::prepare_with(config, &deps).await.unwrap();
        assert_eq!(plan.stream_mode, StreamMode::Streaming);
        assert_eq!(plan.reader_split.total_records, 0);
        assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
        assert_eq!(split_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn executing_prepared_plan_does_not_split_again() {
        let (deps, _, split_count) = dependencies(StreamMode::Batch);
        let config = Arc::new(JobConfig::parse_json(VALID_JOB).unwrap());
        let _plan = Planner::prepare_with(config, &deps).await.unwrap();
        assert_eq!(split_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn record_builder_failure_has_planning_context() {
        let (mut deps, factory_calls, _) = dependencies(StreamMode::Batch);
        deps.fail_builder = true;
        let config = Arc::new(JobConfig::parse_json(VALID_JOB).unwrap());
        let error = Planner::prepare_with(config, &deps).await.err().unwrap();
        assert!(error.to_string().contains("build RecordBuilder"));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn invalid_database_config_fails_before_factories() {
        let (deps, factory_calls, split_count) = dependencies(StreamMode::Batch);
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
            .expect("valid JSON structure"),
        );

        let error = Planner::prepare_with(config, &deps)
            .await
            .err()
            .expect("empty database table must fail planning");

        assert!(error.to_string().contains("table"));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert_eq!(split_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn non_database_mapping_fields_are_not_sql_identifiers() {
        let (deps, factory_calls, _) = dependencies(StreamMode::Batch);
        let mut config = JobConfig::parse_json(VALID_JOB).expect("valid job config");
        config
            .column_mapping
            .insert("profile.name".to_string(), "name".to_string());

        let result = Planner::prepare_with(Arc::new(config), &deps).await;

        if let Err(error) = result {
            panic!("API fields may contain dots: {error}");
        }
        assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
    }
}
