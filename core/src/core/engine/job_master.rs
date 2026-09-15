//! Static conversion of a prepared [`ExecutionPlan`] into a resource-bound
//! physical execution plan.

use anyhow::{anyhow, Result};
use relus_reader::ReadTask;
use relus_writer::WriteTask;

use super::state::{Job, JobId, StateRepository, Task, TaskGroup, TaskGroupId};
use crate::core::planner::ExecutionPlan;

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
/// `ExecutionPlan` is the Planner preparation result; `RuntimeJob` adds the
/// concrete task topology, resources and allocation decisions required for
/// deployment. Lifecycle state is currently held by the contained entities
/// and `StateRepository`; a future design may separate this into
/// `PhysicalExecutionPlan` and `RuntimeJobState`.
#[derive(Clone)]
pub struct RuntimeJob {
    pub job: Job,
    pub groups: Vec<RuntimeTaskGroup>,
    pub reader: std::sync::Arc<dyn relus_reader::DataReader>,
    pub writer: std::sync::Arc<dyn relus_writer::DataWriter>,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub record_builder: std::sync::Arc<crate::pipeline::RecordBuilder>,
    pub stream_mode: relus_reader::StreamMode,
}

/// Builds a runtime topology from the one-shot plan.  Reader splitting is
/// deliberately absent here: `reader_split` is consumed as prepared input.
#[derive(Clone)]
pub struct JobMaster {
    repository: StateRepository,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
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

    fn plan(read_count: usize, writer_count: usize, splits: Arc<AtomicUsize>) -> ExecutionPlan {
        let rb =
            crate::pipeline::RecordBuilder::new(std::collections::BTreeMap::new(), None).unwrap();
        ExecutionPlan {
            reader: Arc::new(FakeReader { splits }),
            writer: Arc::new(FakeWriter {
                count: writer_count,
            }),
            pipeline: crate::pipeline::PipelineConfig {
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
}

impl JobMaster {
    pub fn new(repository: StateRepository) -> Self {
        Self { repository }
    }

    pub fn repository(&self) -> &StateRepository {
        &self.repository
    }

    pub async fn build(&self, plan: ExecutionPlan) -> Result<RuntimeJob> {
        self.build_with_job_id(plan, JobId::new()).await
    }

    pub async fn build_with_job_id(
        &self,
        plan: ExecutionPlan,
        job_id: JobId,
    ) -> Result<RuntimeJob> {
        let ExecutionPlan {
            reader,
            writer,
            pipeline,
            record_builder,
            reader_split,
            stream_mode,
        } = plan;
        let task_count = reader_split.tasks.len();
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
            stream_mode,
        })
    }

    pub async fn initialize(&self, plan: ExecutionPlan) -> Result<RuntimeJob> {
        self.build(plan).await
    }
}
