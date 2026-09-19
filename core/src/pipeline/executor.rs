//! Pipeline Executor 模块
//!
//! Reader → RecordBuilder → Channel → Writer execution for prepared groups.
//!
//! Physical splitting, writer allocation, pairing, and group concurrency are
//! completed before this module is called. The executor only consumes those
//! prepared pairs and reports execution outcomes.
//!
//! Core 层负责 stream 消费、buffer 切分、RecordBuilder mapping 和 channel 发送

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use indicatif::ProgressBar;
use relus_common::constant::pipeline::{
    DEFAULT_BATCH_SIZE, DEFAULT_BUFFER_SIZE, DEFAULT_CHANNEL_NUMBER, DEFAULT_PER_GROUP_CHANNEL,
    DEFAULT_READER_THREADS,
};
use relus_common::pipeline::PipelineMessage;
use relus_reader::{DataReader, ReadTask};
use relus_writer::{DataWriter, WriteTask};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::core::engine::task_execution::{TaskLifecycleObserver, TaskOutcome};
use crate::core::progress::create_progress_bars;
use crate::pipeline::RecordBuilder;

// ==========================================
// 配置
// ==========================================

struct PipelineRunContext<R: DataReader + ?Sized, W: DataWriter + ?Sized> {
    reader: Arc<R>,
    writer: Arc<W>,
    buffer_size: usize,
    batch_size: usize,
    record_builder: Arc<RecordBuilder>,
    cancel_token: CancellationToken,
    progress: PipelineProgress,
}

impl<R: DataReader + ?Sized, W: DataWriter + ?Sized> Clone for PipelineRunContext<R, W> {
    fn clone(&self) -> Self {
        Self {
            reader: Arc::clone(&self.reader),
            writer: Arc::clone(&self.writer),
            buffer_size: self.buffer_size,
            batch_size: self.batch_size,
            record_builder: Arc::clone(&self.record_builder),
            cancel_token: self.cancel_token.clone(),
            progress: self.progress.clone(),
        }
    }
}

#[derive(Clone)]
struct PipelineProgress {
    reader_bar: ProgressBar,
    writer_bar: ProgressBar,
}

struct PairWork {
    pair_id: usize,
    lifecycle_id: usize,
    read_task: ReadTask,
    write_task: WriteTask,
}

struct GroupWork {
    group_id: usize,
    tasks: Vec<PairWork>,
    concurrency: usize,
    observer: Option<Arc<dyn TaskLifecycleObserver>>,
}

struct PairResult {
    pair_id: usize,
    lifecycle_id: usize,
    read_count: usize,
    write_count: usize,
    error: Option<anyhow::Error>,
    shutdown: bool,
}

struct GroupResult {
    total_read: usize,
    total_written: usize,
    error: Option<anyhow::Error>,
    shutdown: bool,
}

pub struct PreparedPipelineTask {
    pub read_task: ReadTask,
    pub write_task: WriteTask,
}
pub struct PreparedTaskGroup {
    pub group_id: usize,
    pub tasks: Vec<PreparedPipelineTask>,
    pub concurrency: usize,
}
pub struct PreparedGroupStats {
    pub total_read: usize,
    pub total_written: usize,
    pub shutdown: bool,
    pub error: Option<String>,
    pub elapsed: std::time::Duration,
}

pub(crate) async fn run_prepared_task_group<R, W>(
    prepared: PreparedTaskGroup,
    reader: Arc<R>,
    writer: Arc<W>,
    config: PipelineConfig,
    record_builder: Arc<RecordBuilder>,
    cancel_token: CancellationToken,
    observer: Arc<dyn TaskLifecycleObserver>,
) -> Result<PreparedGroupStats>
where
    R: DataReader + ?Sized + 'static,
    W: DataWriter + ?Sized + 'static,
{
    let started = Instant::now();
    if prepared.tasks.is_empty() {
        return Ok(PreparedGroupStats {
            total_read: 0,
            total_written: 0,
            shutdown: false,
            error: None,
            elapsed: started.elapsed(),
        });
    }
    let total = prepared.tasks.iter().map(|task| task.read_task.limit).sum();
    let progress_ctx = create_progress_bars(total)?;
    let tasks = prepared
        .tasks
        .into_iter()
        .enumerate()
        .map(|(index, task)| PairWork {
            pair_id: index,
            lifecycle_id: index,
            read_task: task.read_task,
            write_task: task.write_task,
        })
        .collect();
    let ctx = PipelineRunContext {
        reader,
        writer,
        buffer_size: config.buffer_size,
        batch_size: config.batch_size,
        record_builder,
        cancel_token: cancel_token.clone(),
        progress: PipelineProgress {
            reader_bar: progress_ctx.reader_bar.clone(),
            writer_bar: progress_ctx.writer_bar.clone(),
        },
    };
    let result = run_task_group(
        GroupWork {
            group_id: prepared.group_id,
            tasks,
            concurrency: prepared.concurrency,
            observer: Some(observer),
        },
        ctx,
    )
    .await;
    progress_ctx.finish()?;
    Ok(PreparedGroupStats {
        total_read: result.total_read,
        total_written: result.total_written,
        shutdown: result.shutdown,
        error: result.error.map(|e| e.to_string()),
        elapsed: started.elapsed(),
    })
}

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（决定 Task 数量）
    pub reader_threads: usize,
    /// Channel 缓冲区大小
    pub buffer_size: usize,
    /// 全局并发 channel 数
    pub channel_number: usize,
    /// 每个 TaskGroup 内并发 channel 数
    pub per_group_channel: usize,
    /// 批处理大小
    pub batch_size: usize,
    /// 是否使用事务
    pub use_transaction: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            reader_threads: DEFAULT_READER_THREADS,
            buffer_size: DEFAULT_BUFFER_SIZE,
            channel_number: DEFAULT_CHANNEL_NUMBER,
            per_group_channel: DEFAULT_PER_GROUP_CHANNEL,
            batch_size: DEFAULT_BATCH_SIZE,
            use_transaction: true,
        }
    }
}

impl PipelineConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.reader_threads > 0,
            "reader_threads must be greater than zero"
        );
        anyhow::ensure!(
            self.buffer_size > 0,
            "buffer_size must be greater than zero"
        );
        anyhow::ensure!(
            self.channel_number > 0,
            "channel_number must be greater than zero"
        );
        anyhow::ensure!(
            self.per_group_channel > 0,
            "per_group_channel must be greater than zero"
        );
        anyhow::ensure!(self.batch_size > 0, "batch_size must be greater than zero");
        Ok(())
    }

    /// 从系统配置读取 pipeline 参数
    ///
    /// 优先级：系统配置 (default.config.json) > 常量默认值
    pub fn from_system_config(job_config: &relus_common::JobConfig) -> Self {
        let (sys_reader, sys_buffer, sys_channel, sys_per_group, sys_batch, sys_tx) =
            crate::get_config_manager()
                .map(|mgr| {
                    let m = mgr.read();
                    (
                        m.get("pipeline.reader_threads").and_then(as_positive_usize),
                        m.get("pipeline.buffer_size").and_then(as_positive_usize),
                        m.get("pipeline.channel_number").and_then(as_positive_usize),
                        m.get("pipeline.per_group_channel")
                            .and_then(as_positive_usize),
                        m.get("pipeline.batch_size").and_then(as_positive_usize),
                        m.get("pipeline.use_transaction").and_then(|v| v.as_bool()),
                    )
                })
                .unwrap_or((None, None, None, None, None, None));

        Self {
            reader_threads: sys_reader.unwrap_or(DEFAULT_READER_THREADS),
            buffer_size: job_config
                .channel_buffer_size
                .or(sys_buffer)
                .unwrap_or(DEFAULT_BUFFER_SIZE),
            channel_number: sys_channel.unwrap_or(DEFAULT_CHANNEL_NUMBER),
            per_group_channel: sys_per_group.unwrap_or(DEFAULT_PER_GROUP_CHANNEL),
            batch_size: job_config
                .batch_size
                .or(sys_batch)
                .unwrap_or(DEFAULT_BATCH_SIZE),
            use_transaction: sys_tx.unwrap_or(true),
        }
    }
}

fn as_positive_usize(value: &relus_common::app_config::value::ConfigValue) -> Option<usize> {
    value
        .as_i64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}

/// 管道执行统计
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    pub records_read: usize,
    pub records_written: usize,
    pub records_failed: usize,
    pub elapsed_secs: f64,
    pub throughput: f64,
    pub shutdown: bool,
}

impl PipelineStats {
    pub fn calculate_throughput(&mut self) {
        if self.elapsed_secs > 0.0 {
            self.throughput = self.records_written as f64 / self.elapsed_secs;
        }
    }

    pub fn records_failed(&self) -> usize {
        self.records_read.saturating_sub(self.records_written)
    }
}

/// 从 Reader 获取 JsonStream，消费并通过 RecordBuilder mapping 后发送到 channel
/// consume_stream_and_send
async fn csas<R>(
    pair_id: usize,
    reader: Arc<R>,
    task: &ReadTask,
    batch_size: usize,
    builder: &RecordBuilder,
    tx: &mpsc::Sender<PipelineMessage>,
    reader_bar: &ProgressBar,
) -> Result<usize>
where
    R: DataReader + ?Sized,
{
    let stream = reader.read_data(task).await?;
    let mut sent = 0;
    let mut buffer = Vec::with_capacity(batch_size);

    futures::pin_mut!(stream);
    while let Some(result) = stream.next().await {
        let json_val = result?;
        buffer.push(json_val);

        if buffer.len() >= batch_size {
            let count = buffer.len();
            let message = builder.build_message(&buffer)?;
            tx.send(message)
                .await
                .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
            sent += count;
            reader_bar.inc(count as u64);
            buffer.clear();
        }
    }

    // 发送残余数据
    if !buffer.is_empty() {
        let count = buffer.len();
        let message = builder.build_message(&buffer)?;
        tx.send(message)
            .await
            .map_err(|e| anyhow::anyhow!("发送失败: {}", e))?;
        sent += count;
        reader_bar.inc(count as u64);
    }

    info!("Reader-{} 已发送 {} 条（core mapping）", pair_id, sent);
    Ok(sent)
}

async fn run_task_pair<R, W>(
    pair: PairWork,
    ctx: PipelineRunContext<R, W>,
    observer: Option<Arc<dyn TaskLifecycleObserver>>,
) -> PairResult
where
    R: DataReader + ?Sized + 'static,
    W: DataWriter + ?Sized + 'static,
{
    let PairWork {
        pair_id,
        lifecycle_id,
        read_task,
        write_task,
    } = pair;

    let (tx, rx) = crate::core::engine::channel::Channel::new(ctx.buffer_size).pair();
    let was_cancelled = ctx.cancel_token.is_cancelled();

    let r = Arc::clone(&ctx.reader);
    let reader_for_cancel = Arc::clone(&ctx.reader);
    let builder = Arc::clone(&ctx.record_builder);
    let reader_cancel = ctx.cancel_token.clone();
    let r_bar = ctx.progress.reader_bar.clone();
    let batch_size = ctx.batch_size;
    let r_handle = tokio::spawn(async move {
        tokio::select! {
            result = csas(pair_id, r, &read_task, batch_size, &builder, &tx, &r_bar) => {
                match result {
                    Ok(count) => {
                        let _ = tx.send(PipelineMessage::ReaderFinished).await;
                        Ok(count)
                    }
                    Err(e) => {
                        error!("Reader-{} 失败: {}", pair_id, e);
                        let _ = tx.send(PipelineMessage::Error(e.to_string())).await;
                        Err(e)
                    }
                }
            }
            () = reader_cancel.cancelled() => {
                warn!("Reader-{} being eliminated.【outside】", pair_id);
                reader_for_cancel.shutdown();
                Err(anyhow::anyhow!("Reader-{} process terminates unexpectedly.", pair_id))
            }
        }
    });

    // 中间转发 task: rx → writer_bar.inc → tx2，Writer 拿 rx2
    let (tx2, rx2) = crate::core::engine::channel::Channel::new(ctx.buffer_size).pair();
    let w_bar = ctx.progress.writer_bar.clone();
    let relay_handle = tokio::spawn(async move {
        let mut rx = rx;
        while let Some(msg) = rx.recv().await {
            if let PipelineMessage::DataBatch(rows) = &msg {
                w_bar.inc(rows.len() as u64);
            }
            if tx2.send(msg).await.is_err() {
                break;
            }
        }
    });

    let w = Arc::clone(&ctx.writer);
    let writer_cancel = ctx.cancel_token.clone();
    let w_handle = tokio::spawn(async move {
        tokio::select! {
            result = w.write_data(write_task, rx2) => {
                if let Err(ref e) = result {
                    error!("Writer-{} 失败: {}", pair_id, e);
                }
                result
            }
            () = writer_cancel.cancelled() => {
                warn!("Writer-{} 正常关闭", pair_id);
                Ok(0)
            }
        }
    });

    if let Some(ref observer) = observer {
        observer.deployed(lifecycle_id).await;
    }

    // 确保 relay task 不泄漏
    let _ = relay_handle.await;

    let reader_result = r_handle
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("Reader-{} 任务崩溃: {}", pair_id, e)));
    let writer_result = w_handle
        .await
        .unwrap_or_else(|e| Err(anyhow::anyhow!("Writer-{} 任务崩溃: {}", pair_id, e)));

    let (read_count, read_err) = match reader_result {
        Ok(n) => (n, None),
        Err(e) => (0, Some(e)),
    };
    let (write_count, write_err) = match writer_result {
        Ok(n) => (n, None),
        Err(e) => (0, Some(e)),
    };

    let error = match (read_err, write_err) {
        (Some(r), Some(w)) => Some(anyhow::anyhow!("R/W FULL FAIL: {}; {}", r, w)),
        (Some(e), None) | (None, Some(e)) => Some(e),
        _ => None,
    };

    PairResult {
        pair_id,
        lifecycle_id,
        read_count,
        write_count,
        error,
        shutdown: was_cancelled || ctx.cancel_token.is_cancelled(),
    }
}

async fn run_task_group<R, W>(group: GroupWork, ctx: PipelineRunContext<R, W>) -> GroupResult
where
    R: DataReader + ?Sized + 'static,
    W: DataWriter + ?Sized + 'static,
{
    let GroupWork {
        group_id,
        tasks,
        concurrency,
        observer,
    } = group;
    let mut queue: VecDeque<PairWork> = VecDeque::from(tasks);
    let mut running = FuturesUnordered::new();

    let mut total_read = 0usize;
    let mut total_written = 0usize;
    let mut first_error: Option<anyhow::Error> = None;
    let mut group_shutdown = false;

    while running.len() < concurrency {
        if let Some(pair) = queue.pop_front() {
            running.push(run_task_pair(pair, ctx.clone(), observer.clone()));
        } else {
            break;
        }
    }

    while let Some(pair_result) = running.next().await {
        if pair_result.shutdown {
            group_shutdown = true;
        }

        let pair_failed = pair_result.error.is_some();
        let pair_shutdown = pair_result.shutdown;
        let lifecycle_id = pair_result.lifecycle_id;
        if let Some(e) = pair_result.error {
            error!(
                "TaskGroup-{} 的 Pair-{} 失败: {}",
                group_id, pair_result.pair_id, e
            );
            if first_error.is_none() && !pair_result.shutdown {
                first_error = Some(e);
            }
            ctx.cancel_token.cancel();
        } else {
            total_read += pair_result.read_count;
            total_written += pair_result.write_count;
            info!(
                "TaskGroup-{} 的 Pair-{} 完成，读取 {} 条，写入 {} 条",
                group_id, pair_result.pair_id, pair_result.read_count, pair_result.write_count
            );
        }

        if let Some(ref observer) = observer {
            observer
                .completed(
                    lifecycle_id,
                    if pair_shutdown {
                        TaskOutcome::Cancelled
                    } else if pair_failed {
                        TaskOutcome::Failed
                    } else {
                        TaskOutcome::Succeeded
                    },
                )
                .await;
        }

        if first_error.is_none() {
            while running.len() < concurrency {
                if let Some(pair) = queue.pop_front() {
                    running.push(run_task_pair(pair, ctx.clone(), observer.clone()));
                } else {
                    break;
                }
            }
        }
    }

    GroupResult {
        total_read,
        total_written,
        error: first_error,
        shutdown: group_shutdown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream;
    use relus_common::PipelineMessage;
    use relus_reader::{DataReaderJob, DataReaderTask, JsonStream, SplitReaderResult, StreamMode};
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult};

    struct FakeReader;

    #[async_trait]
    impl DataReaderJob for FakeReader {
        async fn split(&self, _: usize) -> Result<SplitReaderResult> {
            Ok(SplitReaderResult {
                total_records: 0,
                tasks: Vec::new(),
                stream_mode: StreamMode::Batch,
            })
        }

        fn description(&self) -> String {
            "pipeline typed fake reader".into()
        }
    }

    #[async_trait]
    impl DataReaderTask for FakeReader {
        async fn read_data(&self, _: &ReadTask) -> Result<JsonStream> {
            Ok(Box::pin(stream::empty()))
        }
    }

    struct FakeWriter;

    struct NoopObserver;

    #[async_trait]
    impl TaskLifecycleObserver for NoopObserver {
        async fn deployed(&self, _: usize) {}
        async fn completed(&self, _: usize, _: TaskOutcome) {}
    }

    #[async_trait]
    impl DataWriterJob for FakeWriter {
        async fn split(&self, _: usize) -> Result<SplitWriterResult> {
            Ok(SplitWriterResult { tasks: Vec::new() })
        }

        fn description(&self) -> String {
            "pipeline typed fake writer".into()
        }
    }

    #[async_trait]
    impl DataWriterTask for FakeWriter {
        async fn write_data(
            &self,
            _: relus_writer::WriteTask,
            _: tokio::sync::mpsc::Receiver<PipelineMessage>,
        ) -> Result<usize> {
            Ok(0)
        }
    }

    fn config() -> PipelineConfig {
        PipelineConfig {
            reader_threads: 1,
            buffer_size: 1,
            channel_number: 1,
            per_group_channel: 1,
            batch_size: 1,
            use_transaction: false,
        }
    }

    #[tokio::test]
    async fn generic_pipeline_entry_accepts_concrete_and_source_sink_types() {
        let builder =
            Arc::new(RecordBuilder::new(std::collections::BTreeMap::new(), None).unwrap());
        let observer: Arc<dyn TaskLifecycleObserver> = Arc::new(NoopObserver);
        let stats = run_prepared_task_group(
            PreparedTaskGroup {
                group_id: 0,
                tasks: Vec::new(),
                concurrency: 1,
            },
            Arc::new(FakeReader),
            Arc::new(FakeWriter),
            config(),
            Arc::clone(&builder),
            CancellationToken::new(),
            Arc::clone(&observer),
        )
        .await
        .unwrap();
        assert_eq!(stats.total_read, 0);

        let source: relus_reader::Source = Arc::new(FakeReader);
        let sink: relus_writer::Sink = Arc::new(FakeWriter);
        let stats = run_prepared_task_group(
            PreparedTaskGroup {
                group_id: 0,
                tasks: Vec::new(),
                concurrency: 1,
            },
            source,
            sink,
            config(),
            builder,
            CancellationToken::new(),
            observer,
        )
        .await
        .unwrap();
        assert_eq!(stats.total_written, 0);
    }
}
