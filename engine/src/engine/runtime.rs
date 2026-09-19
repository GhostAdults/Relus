use super::state::TaskGroupId;
use super::task_execution::TaskGroupExecutionResult;
use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub struct RuntimeTaskHandle {
    group_id: TaskGroupId,
    receiver: oneshot::Receiver<Result<TaskGroupExecutionResult, String>>,
}

impl RuntimeTaskHandle {
    pub fn group_id(&self) -> TaskGroupId {
        self.group_id
    }
    pub async fn join(self) -> Result<TaskGroupExecutionResult> {
        self.receiver
            .await
            .map_err(|_| anyhow!("runtime task ended without a result"))?
            .map_err(|e| anyhow!(e))
    }
}

pub trait Runtime: Send + Sync {
    fn submit(
        &self,
        group_id: TaskGroupId,
        cancel: CancellationToken,
        task: BoxFuture<'static, Result<TaskGroupExecutionResult>>,
    ) -> Result<RuntimeTaskHandle>;
    fn cancel(&self, group_id: TaskGroupId) -> bool;
}

#[derive(Clone, Default)]
pub struct TokioRuntime {
    cancellations: Arc<Mutex<HashMap<TaskGroupId, CancellationToken>>>,
}

impl TokioRuntime {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn active_tasks(&self) -> usize {
        self.cancellations.lock().len()
    }
}

impl Runtime for TokioRuntime {
    fn submit(
        &self,
        group_id: TaskGroupId,
        cancel: CancellationToken,
        task: BoxFuture<'static, Result<TaskGroupExecutionResult>>,
    ) -> Result<RuntimeTaskHandle> {
        let (tx, rx) = oneshot::channel();
        self.cancellations.lock().insert(group_id, cancel);
        let cancellations = Arc::clone(&self.cancellations);
        tokio::spawn(async move {
            let result = task.await.map_err(|e| e.to_string());
            let _ = tx.send(result);
            cancellations.lock().remove(&group_id);
        });
        Ok(RuntimeTaskHandle {
            group_id,
            receiver: rx,
        })
    }

    fn cancel(&self, group_id: TaskGroupId) -> bool {
        self.cancellations
            .lock()
            .get(&group_id)
            .map(|token| token.cancel())
            .is_some()
    }
}
