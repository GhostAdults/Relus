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
