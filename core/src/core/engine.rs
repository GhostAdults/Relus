//! Execution Engine primitives.
pub mod channel;
pub mod contracts;
pub mod coordinator;
pub mod job_master;
pub mod runtime;
pub mod state;
pub mod task_execution;
pub mod worker;

/// Extension seams reserved for future policies; execution does not invoke
/// or enable any strategy by default.
pub trait MetricsSink: Send + Sync {
    fn record(&self, _name: &str, _value: u64) {}
}
pub trait RetryPolicy: Send + Sync {
    fn should_retry(&self, _attempt: usize) -> bool {
        false
    }
}
pub trait CheckpointStore: Send + Sync {
    fn checkpoint(&self, _key: &str, _offset: u64) -> anyhow::Result<()> {
        Ok(())
    }
}

use self::task_execution::{TaskGroupExecutionResult, TaskGroupExecutionStatus};
use crate::core::runner::{RunResult, RunStatus, RunnerStats};
use crate::pipeline::PipelineStats;
use relus_reader::StreamMode;
use std::time::Duration;

/// Aggregate TaskGroup outcomes into the historical Runner DTOs.
pub fn aggregate_run_result(
    groups: &[TaskGroupExecutionResult],
    stream_mode: StreamMode,
    duration: Duration,
    cancelled: bool,
) -> RunResult {
    let mut pipeline = PipelineStats::default();
    pipeline.records_read = groups.iter().map(|g| g.records_read).sum();
    pipeline.records_written = groups.iter().map(|g| g.records_written).sum();
    pipeline.records_failed = groups.iter().map(|g| g.records_failed).sum();
    pipeline.elapsed_secs = duration.as_secs_f64();
    pipeline.shutdown = cancelled || groups.iter().any(|g| g.cancelled);
    pipeline.calculate_throughput();
    let shutdown = pipeline.shutdown;
    let stats = RunnerStats::from_pipeline(pipeline);
    let failed = groups
        .iter()
        .any(|g| matches!(g.status, TaskGroupExecutionStatus::Failed));
    let partial = stats.records_failed > 0;
    let status = if shutdown {
        RunStatus::Shutdown
    } else if failed && stats.records_written == 0 {
        RunStatus::Failed
    } else if failed || partial {
        RunStatus::Partial
    } else if stream_mode == StreamMode::Streaming {
        RunStatus::Failed
    } else {
        RunStatus::Success
    };
    let error = if failed {
        groups.iter().find_map(|g| g.error_summary.clone())
    } else if status == RunStatus::Failed {
        Some("Stream pipeline 非预期退出".to_string())
    } else {
        None
    };
    RunResult {
        stats,
        status,
        duration,
        error,
    }
}

#[cfg(test)]
mod aggregation_tests {
    use super::*;
    use crate::core::engine::state::TaskGroupId;

    #[test]
    fn aggregates_task_groups_into_compatible_result() {
        let id = TaskGroupId::new();
        let groups = vec![TaskGroupExecutionResult {
            group_id: id,
            status: TaskGroupExecutionStatus::Succeeded,
            records_read: 10,
            records_written: 8,
            records_failed: 2,
            cancelled: false,
            error_summary: None,
            elapsed: Duration::from_secs(1),
        }];
        let result =
            aggregate_run_result(&groups, StreamMode::Batch, Duration::from_secs(2), false);
        assert_eq!(result.status, RunStatus::Partial);
        assert_eq!(result.stats.records_read, 10);
        assert_eq!(result.stats.records_written, 8);
        assert_eq!(result.stats.records_failed, 2);
    }

    #[test]
    fn streaming_empty_completion_keeps_legacy_failure_status() {
        let result =
            aggregate_run_result(&[], StreamMode::Streaming, Duration::from_secs(1), false);
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.error.as_deref(), Some("Stream pipeline 非预期退出"));
    }

    #[test]
    fn cancellation_takes_precedence_over_stream_status() {
        let result = aggregate_run_result(&[], StreamMode::Streaming, Duration::from_secs(1), true);
        assert_eq!(result.status, RunStatus::Shutdown);
        assert!(result.error.is_none());
    }
}
