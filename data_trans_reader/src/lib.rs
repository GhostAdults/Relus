use anyhow::Result;
use tokio::sync::mpsc;

/// 任务（Job Split 切分后的子任务）
#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: usize,
    pub offset: usize,
    pub limit: usize,
}

/// 任务切分结果
pub struct JobSplitResult {
    pub total_records: usize,
    pub tasks: Vec<Task>,
}

/// Job trait - 定义数据读取任务的通用接口
#[async_trait::async_trait]
pub trait Job<M>: Send + Sync
where
    M: Send + 'static,
{
    /// 切分任务为多个子任务
    async fn split(&self, reader_threads: usize) -> Result<JobSplitResult>;

    /// 执行单个任务，读取数据并发送到 Channel
    async fn execute_task(&self, task: Task, tx: mpsc::Sender<M>) -> Result<usize>;

    /// 获取任务描述（用于日志）
    fn description(&self) -> String;
}

/// db.reader 驱动抽象：用于为 Reader 提供数据库连接池
pub trait DbReaderDriver: Send + Sync {
    type Pool: Send + Sync;

    fn pool(&self) -> &Self::Pool;
}
