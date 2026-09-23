use super::job_master::RuntimeTaskGroup;
use super::task_execution::{TaskGroupExecutionResult, TaskLifecycleObserver};
use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn prepare_task_group(group: &RuntimeTaskGroup) -> crate::pipeline::PreparedTaskGroup {
    crate::pipeline::PreparedTaskGroup {
        group_id: 0,
        concurrency: group.concurrency.max(1),
        tasks: group
            .tasks
            .iter()
            .map(|task| crate::pipeline::PreparedPipelineTask {
                read_task: task.read_task.clone(),
                write_task: task.write_task.clone(),
            })
            .collect(),
    }
}

#[derive(Clone)]
pub struct WorkerContext {
    pub reader: relus_reader::Source,
    pub writer: relus_writer::Sink,
    pub pipeline: relus_common::pipeline::PipelineConfig,
    pub record_builder: Arc<crate::pipeline::RecordBuilder>,
    pub progress: Option<Arc<dyn crate::engine::contracts::ProgressObserver>>,
}

pub struct Worker;

impl Worker {
    pub async fn run(
        group: RuntimeTaskGroup,
        context: WorkerContext,
        cancel: CancellationToken,
        observer: Arc<dyn TaskLifecycleObserver>,
    ) -> Result<TaskGroupExecutionResult> {
        let prepared = prepare_task_group(&group);
        let stats = crate::pipeline::run_prepared_task_group(
            prepared,
            context.reader,
            context.writer,
            context.pipeline,
            context.record_builder,
            cancel.clone(),
            observer,
            context.progress.clone(),
        )
        .await?;
        Ok(TaskGroupExecutionResult::from_pipeline(
            group.group.id,
            stats,
        ))
    }
}
