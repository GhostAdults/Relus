//! Data数据同步管道
//! Reader → Channel → Writer 架构，支持多线程并发
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use data_trans_common::job_config::JobConfig;
use data_trans_common::pipeline::PipelineMessage;
use data_trans_common::types::DataSourceType;
use data_trans_reader::api_reader::ApiJob;
use data_trans_reader::database_reader::DatabaseJob;
use data_trans_reader::Job as ReaderJob;
use data_trans_writer::database_writer::DatabaseJob as DatabaseWriterJob;
use data_trans_writer::Job as WriterJob;

// ==========================================
// 配置
// ==========================================

/// 管道配置
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Reader 线程数（分片读取任务数）
    pub reader_threads: usize,

    /// Writer 线程数（用于并发写入）
    pub writer_threads: usize,

    /// Channel 缓冲区大小
    pub channel_buffer_size: usize,

    /// 每个批次的大小
    pub batch_size: usize,

    /// 是否使用事务
    pub use_transaction: bool,

    /// 数据源类型
    pub data_source: DataSourceType,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            reader_threads: 1,         // 默认单 Reader（API 通常只需要一次请求）
            writer_threads: 4,         // 默认 4 个 Writer
            channel_buffer_size: 1000, // 缓冲 1000 条数据
            batch_size: 100,           // 每批 100 条
            use_transaction: true,
            data_source: DataSourceType::Api, // 默认使用 API 数据源
        }
    }
}

// ==========================================
// 统计信息
// ==========================================

/// 管道统计信息
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// 读取的记录数
    pub records_read: usize,

    /// 写入的记录数
    pub records_written: usize,

    /// 失败的记录数
    pub records_failed: usize,

    /// 总耗时（秒）
    pub elapsed_secs: f64,

    /// 吞吐量（记录/秒）
    pub throughput: f64,
}

impl PipelineStats {
    pub fn calculate_throughput(&mut self) {
        if self.elapsed_secs > 0.0 {
            self.throughput = self.records_written as f64 / self.elapsed_secs;
        }
    }
}

// ==========================================
// 管道执行器
// ==========================================

/// 管道执行器：协调 Reader 和 Writer
pub struct Pipeline {
    config: Arc<JobConfig>,
    pipeline_config: Arc<PipelineConfig>,
}

impl Pipeline {
    pub fn new(config: JobConfig, pipeline_config: PipelineConfig) -> Self {
        Self {
            config: Arc::new(config),
            pipeline_config: Arc::new(pipeline_config),
        }
    }

    /// 执行管道
    pub async fn execute(&self) -> Result<PipelineStats> {
        let start_time = Instant::now();
        println!("\n启动数据同步管道");
        println!(
            "配置: {} Reader, {} Writer, Channel 缓冲 {}",
            self.pipeline_config.reader_threads,
            self.pipeline_config.writer_threads,
            self.pipeline_config.channel_buffer_size
        );

        // 创建 Channel
        let (tx, rx) = mpsc::channel::<PipelineMessage>(self.pipeline_config.channel_buffer_size);

        let reader_handles = match &self.pipeline_config.data_source {
            DataSourceType::Database { .. } => {
                println!("数据库数据源，启动任务切分...");

                let job = Arc::new(DatabaseJob::new(Arc::clone(&self.config))?);

                println!("Job: {}", job.description());

                let split_result = job.split(self.pipeline_config.reader_threads).await?;
                println!("总记录数: {}", split_result.total_records);
                println!("切分为 {} 个任务", split_result.tasks.len());

                let mut handles = Vec::new();
                for task in split_result.tasks {
                    let job = Arc::clone(&job);
                    let tx = tx.clone();

                    let handle = tokio::spawn(async move { job.execute_task(task, tx).await });

                    handles.push(handle);
                }

                handles
            }

            DataSourceType::Api => {
                println!("API 数据源，使用单 Reader");

                let job: Arc<dyn ReaderJob<PipelineMessage>> =
                    Arc::new(ApiJob::new(Arc::clone(&self.config)));
                println!("Job: {}", job.description());

                let split_result = job.split(1).await?;
                let task = split_result.tasks[0].clone();

                let tx = tx.clone();
                let handle = tokio::spawn(async move { job.execute_task(task, tx).await });

                vec![handle]
            }
        };

        // 释放原始 tx，确保所有 Reader 完成后 Channel 能正确关闭
        drop(tx);

        let writer_handles = self.start_writers(rx).await?;

        // 等待所有 Reader 完成
        let mut total_read = 0;
        for (i, handle) in reader_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_read += count;
                    println!("Reader-{} 完成，读取 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    eprintln!("Reader-{} 失败: {}", i, e);
                }
                Err(e) => {
                    eprintln!("Reader-{} 任务崩溃: {}", i, e);
                }
            }
        }

        // 等待所有 Writer 完成
        let mut total_written = 0;
        for (i, handle) in writer_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(count)) => {
                    total_written += count;
                    println!("Writer-{} 完成，写入 {} 条", i, count);
                }
                Ok(Err(e)) => {
                    eprintln!("Writer-{} 失败: {}", i, e);
                }
                Err(e) => {
                    eprintln!("Writer-{} 任务崩溃: {}", i, e);
                }
            }
        }

        let elapsed = start_time.elapsed();
        let mut stats = PipelineStats {
            records_read: total_read,
            records_written: total_written,
            records_failed: total_read.saturating_sub(total_written),
            elapsed_secs: elapsed.as_secs_f64(),
            throughput: 0.0,
        };
        stats.calculate_throughput();

        println!("\n同步完成");
        println!("   读取: {} 条", stats.records_read);
        println!("   写入: {} 条", stats.records_written);
        println!("   失败: {} 条", stats.records_failed);
        println!("   耗时: {:.2} 秒", stats.elapsed_secs);
        println!("   吞吐: {:.2} 条/秒", stats.throughput);

        Ok(stats)
    }

    /// 启动多个 Writer（使用分发策略）
    async fn start_writers(
        &self,
        rx: mpsc::Receiver<PipelineMessage>,
    ) -> Result<Vec<JoinHandle<Result<usize>>>> {
        let writer_count = self.pipeline_config.writer_threads;

        // 创建 Writer Job
        let writer_job: Arc<dyn WriterJob<PipelineMessage>> =
            Arc::new(DatabaseWriterJob::new(Arc::clone(&self.config))?);

        println!("Writer Job: {}", writer_job.description());

        // 切分 Writer 任务
        let split_result = writer_job.split(writer_count).await?;
        let write_tasks = split_result.tasks;

        let mut writer_txs = Vec::new();
        let mut writer_handles = Vec::new();

        for (i, task) in write_tasks.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel::<PipelineMessage>(100);
            writer_txs.push(tx);

            let job = Arc::clone(&writer_job);
            let handle = tokio::spawn(async move { job.execute_task(task, rx).await });
            writer_handles.push((i, handle));
        }

        // 启动分发器任务：从主 Channel 读取，轮询分发到各 Writer
        let reader_total = self.pipeline_config.reader_threads;
        tokio::spawn(async move {
            let mut current_writer = 0;
            let mut reader_finished_count = 0;

            let mut rx = rx;

            while let Some(msg) = rx.recv().await {
                match &msg {
                    PipelineMessage::DataBatch(_) => {
                        // 轮询分发到 Writer
                        let tx = &writer_txs[current_writer];
                        if tx.send(msg).await.is_err() {
                            eprintln!("分发器: Writer-{} Channel 已关闭", current_writer);
                        }
                        current_writer = (current_writer + 1) % writer_count;
                    }
                    PipelineMessage::ReaderFinished => {
                        reader_finished_count += 1;
                        println!(
                            "分发器: 收到 Reader 完成信号 ({}/{})",
                            reader_finished_count, reader_total
                        );

                        // 所有 Reader 完成后，通知所有 Writer
                        if reader_finished_count >= reader_total {
                            println!("分发器: 所有 Reader 完成，通知所有 Writer");
                            for tx in &writer_txs {
                                let _ = tx.send(PipelineMessage::ReaderFinished).await;
                            }
                            break;
                        }
                    }
                    PipelineMessage::Error(err) => {
                        eprintln!("分发器: 收到错误 - {}", err);
                        // 通知所有 Writer 停止
                        for tx in &writer_txs {
                            let _ = tx.send(msg.clone()).await;
                        }
                        break;
                    }
                }
            }

            // 关闭所有 Writer 的 Channel
            drop(writer_txs);
            println!("分发器: 已关闭所有 Writer Channel");
        });

        Ok(writer_handles.into_iter().map(|(_, h)| h).collect())
    }
}

// ==========================================
// 便捷入口函数
// ==========================================

/// 使用管道执行同步（默认）
pub async fn sync_with_pipeline(config: JobConfig) -> Result<PipelineStats> {
    let pipeline_config = PipelineConfig::default();
    let pipeline = Pipeline::new(config, pipeline_config);
    pipeline.execute().await
}

/// 使用管道执行同步（切分任务）
pub async fn sync_with_pipeline_task(
    config: JobConfig,
    pipeline_config: PipelineConfig,
) -> Result<PipelineStats> {
    let pipeline = Pipeline::new(config, pipeline_config);
    pipeline.execute().await
}
