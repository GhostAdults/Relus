use super::*;
use indicatif::{InMemoryTerm, TermLike};

fn terminal() -> (IndicatifProgress, InMemoryTerm) {
    let screen = InMemoryTerm::new(24, 80);
    let progress = IndicatifProgress::with_terminal(true);
    progress
        .multi
        .as_ref()
        .unwrap()
        .set_draw_target(ProgressDrawTarget::term_like(Box::new(screen.clone())));
    (progress, screen)
}

fn tick(progress: &IndicatifProgress) {
    let state = progress.state.lock().unwrap();
    let bars = state.bars.as_ref().unwrap();
    bars.reader.disable_steady_tick();
    bars.writer.disable_steady_tick();
    bars.reader.tick();
    bars.writer.tick();
}

fn assert_two_bars(screen: &InMemoryTerm) {
    let output = screen.contents();
    assert_eq!(output.matches("Reader [").count(), 1, "{output}");
    assert_eq!(output.matches("Writer [").count(), 1, "{output}");
    assert_eq!(output.matches("Relus Data Sync").count(), 1, "{output}");
    assert!(!output.contains("Tasks"), "{output}");
}

#[test]
fn first_frame_is_owned_by_multi_progress() {
    let (progress, screen) = terminal();
    progress.started(Some(720_300));
    // No extra tick: started itself must draw through the managed target.
    assert_two_bars(&screen);
}

#[test]
fn completion_leaves_only_two_bars_before_summary() {
    let (progress, screen) = terminal();
    progress.started(Some(720_300));
    progress.started(Some(720_300));
    progress.records_read(720_300);
    progress.records_sent(702_299);
    progress.records_sent(18_001);
    progress.finished(ProgressOutcome::Succeeded, 720_300, 720_300);
    let finished = screen.contents();
    progress.records_read(100);
    progress.records_sent(100);
    progress.started(Some(100));
    progress.finished(ProgressOutcome::Failed, 0, 0);
    drop(progress);
    assert_eq!(
        screen.contents(),
        finished,
        "late events/drop redrew the terminal"
    );
    screen
        .write_line("complete: 720300 records (synthetic)")
        .unwrap();
    assert_two_bars(&screen);
    let output = screen.contents();
    assert_eq!(output.matches("720,300/720,300").count(), 2, "{output}");
    assert_eq!(output.matches("Done").count(), 2, "{output}");
    assert!(output.find("Writer [").unwrap() < output.find("complete:").unwrap());
    assert!(
        output.lines().any(|line| line.starts_with("complete:")),
        "{output}"
    );
}

#[test]
fn batch_stats_occupy_the_next_line_without_wrapping() {
    let (progress, screen) = terminal();
    progress.started(Some(100_000));
    progress.records_read(82_104);
    progress.records_sent(82_104);
    progress.finished(ProgressOutcome::Cancelled, 82_104, 82_104);
    let output = screen.contents();
    let lines: Vec<_> = output.lines().collect();
    for name in ["Reader [", "Writer ["] {
        let index = lines
            .iter()
            .position(|line| line.contains(name))
            .expect(&output);
        assert!(lines[index].contains("82,104/100,000"), "{output}");
        assert!(!lines[index].contains("Speed:"), "{output}");
        assert!(
            lines[index + 1].trim_start().starts_with("Speed:"),
            "{output}"
        );
        assert!(lines[index + 1].contains("Elapsed:"), "{output}");
        assert!(lines[index + 1].contains("ETA:"), "{output}");
    }
}

#[test]
fn failed_and_cancelled_bars_preserve_actual_counts() {
    for (outcome, message) in [
        (ProgressOutcome::Failed, "Failed"),
        (ProgressOutcome::Cancelled, "Cancelled"),
    ] {
        let (progress, screen) = terminal();
        progress.started(Some(10));
        progress.records_read(10);
        progress.records_sent(8);
        progress.finished(outcome, 7, 6);
        assert_eq!(progress.counts(), (7, 6));
        assert_two_bars(&screen);
        let output = screen.contents();
        assert!(output.contains("7/10"), "{output}");
        assert!(output.contains("6/10"), "{output}");
        assert!(!output.contains("100%"), "{output}");
        assert_eq!(output.matches(message).count(), 2, "{output}");
    }
}

#[test]
fn streaming_keeps_event_counts_without_batch_percentage() {
    let (progress, screen) = terminal();
    progress.started(None);
    progress.records_read(123);
    progress.records_sent(120);
    progress.finished(ProgressOutcome::Cancelled, 123, 120);
    let output = screen.contents();
    assert!(output.contains("123 events"), "{output}");
    assert!(output.contains("120 events"), "{output}");
    assert!(!output.contains('%'), "{output}");
}

#[test]
fn non_tty_start_does_not_allocate_standalone_bars() {
    let progress = IndicatifProgress::with_terminal(false);
    progress.started(Some(10));
    assert!(progress.state.lock().unwrap().bars.is_none());
    progress.records_read(10);
    progress.records_sent(8);
    progress.finished(ProgressOutcome::Failed, 7, 6);
    assert_eq!(progress.counts(), (7, 6));
}

#[test]
fn empty_job_completion_does_not_draw() {
    let (progress, screen) = terminal();
    progress.finished(ProgressOutcome::Succeeded, 0, 0);
    drop(progress);
    assert_eq!(screen.contents(), "");
}

#[test]
fn topology_footer_is_below_bars_during_and_after_execution() {
    let (progress, screen) = terminal();
    progress.planned(ProgressTopology {
        readers: 4,
        writers: 8,
        workers: 12,
    });
    progress.started(Some(100_000));
    progress.records_read(82_104);
    progress.records_sent(80_000);
    tick(&progress);
    let output = screen.contents();
    let footer = "Readers: 4    Writers: 8    Workers: 12";
    assert_eq!(output.matches(footer).count(), 1, "{output}");
    assert!(output.find("Writer [").unwrap() < output.find(footer).unwrap());
    assert!(output.contains("82,104/100,000"), "{output}");
    assert!(output.contains("80,000/100,000"), "{output}");
    progress.finished(ProgressOutcome::Succeeded, 100_000, 100_000);
    assert_two_bars(&screen);
    let output = screen.contents();
    assert_eq!(output.matches(footer).count(), 1, "{output}");
    assert!(output.find("Writer [").unwrap() < output.find(footer).unwrap());
    drop(progress);
    screen.write_line("complete: synthetic").unwrap();
    assert!(
        screen
            .contents()
            .lines()
            .any(|line| line.starts_with("complete:")),
        "{}",
        screen.contents()
    );
}

#[test]
fn concurrent_updates_and_late_callbacks_do_not_duplicate_display() {
    let (progress, screen) = terminal();
    progress.started(Some(800));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let progress = &progress;
            scope.spawn(move || {
                for _ in 0..100 {
                    progress.records_read(1);
                    progress.records_sent(1);
                }
            });
        }
    });
    assert_eq!(progress.counts(), (800, 800));
    tick(&progress);
    assert_two_bars(&screen);
    progress.finished(ProgressOutcome::Succeeded, 800, 790);
    let output = screen.contents();
    progress.records_read(5);
    progress.records_sent(5);
    assert_eq!(progress.counts(), (800, 790));
    assert_eq!(screen.contents(), output);
    assert!(output.contains("790/800"), "{output}");
    assert_two_bars(&screen);
}

#[test]
fn terminal_io_failure_does_not_escape_observer() {
    #[derive(Debug)]
    struct BrokenTerminal;
    impl TermLike for BrokenTerminal {
        fn width(&self) -> u16 {
            80
        }
        fn move_cursor_up(&self, _: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn move_cursor_down(&self, _: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn move_cursor_left(&self, _: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn move_cursor_right(&self, _: usize) -> std::io::Result<()> {
            Ok(())
        }
        fn write_line(&self, _: &str) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn write_str(&self, _: &str) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn clear_line(&self) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&self) -> std::io::Result<()> {
            Err(std::io::ErrorKind::BrokenPipe.into())
        }
    }
    let progress = IndicatifProgress::with_terminal(true);
    progress
        .multi
        .as_ref()
        .unwrap()
        .set_draw_target(ProgressDrawTarget::term_like(Box::new(BrokenTerminal)));
    progress.started(Some(1));
    progress.records_read(1);
    progress.records_sent(1);
    progress.finished(ProgressOutcome::Succeeded, 1, 1);
    assert_eq!(progress.counts(), (1, 1));
    assert!(progress.state.lock().unwrap().bars.is_none());
}

#[test]
fn wrapped_terminal_frames_still_leave_exactly_two_bars() {
    for width in [60, 80, 120] {
        let screen = InMemoryTerm::new(40, width);
        let progress = IndicatifProgress::with_terminal(true);
        progress
            .multi
            .as_ref()
            .unwrap()
            .set_draw_target(ProgressDrawTarget::term_like(Box::new(screen.clone())));
        progress.planned(ProgressTopology {
            readers: 4,
            writers: 4,
            workers: 2,
        });
        progress.started(Some(720_300));
        for amount in [300_000, 400_000, 20_300] {
            progress.records_read(amount);
            progress.records_sent(amount);
            tick(&progress);
            assert_two_bars(&screen);
        }
        progress.finished(ProgressOutcome::Succeeded, 720_300, 720_300);
        drop(progress);
        assert_two_bars(&screen);
        assert_eq!(screen.contents().matches("Workers: 2").count(), 1);
    }
}

#[test]
#[ignore = "interactive synthetic preview; run explicitly with --ignored --nocapture"]
fn terminal_preview() {
    let progress = IndicatifProgress::new();
    progress.planned(ProgressTopology {
        readers: 4,
        writers: 4,
        workers: 2,
    });
    progress.started(Some(720_300));
    for amount in [180_000, 180_000, 180_000, 180_300] {
        std::thread::sleep(Duration::from_millis(200));
        progress.records_read(amount);
        progress.records_sent(amount);
    }
    progress.finished(ProgressOutcome::Succeeded, 720_300, 720_300);
    println!("complete: 720300 records (synthetic, no database)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submitted_job_finishes_the_real_adapter_before_summary() {
    use crate::{
        engine::job_master::PlanningDependencies,
        engine::{
            contracts::{ExecutionOptions, JobSubmission},
            coordinator::CoordinatorService,
            state::StateRepository,
        },
        pipeline::RecordBuilder,
    };
    use async_trait::async_trait;
    use relus_common::{job_config::JobConfig, PipelineMessage};
    use relus_reader::{
        DataReaderJob, DataReaderTask, JsonStream, ReadTask, SplitReaderResult, StreamMode,
    };
    use relus_writer::{DataWriterJob, DataWriterTask, SplitWriterResult, WriteTask};
    use std::sync::Arc;

    struct FakeSource;
    #[async_trait]
    impl DataReaderJob for FakeSource {
        async fn split(&self, _: usize) -> anyhow::Result<SplitReaderResult> {
            Ok(SplitReaderResult {
                total_records: 3,
                tasks: (0..3)
                    .map(|task_id| ReadTask {
                        task_id,
                        conn: serde_json::json!({}),
                        query_sql: None,
                        offset: 0,
                        limit: 1,
                    })
                    .collect(),
                stream_mode: StreamMode::Batch,
            })
        }
        fn description(&self) -> String {
            "progress-fixture".into()
        }
    }
    #[async_trait]
    impl DataReaderTask for FakeSource {
        async fn read_data(&self, _: &ReadTask) -> anyhow::Result<JsonStream> {
            Ok(Box::pin(futures::stream::iter(vec![Ok(
                serde_json::json!({"value": 1}),
            )])))
        }
    }
    struct FakeSink;
    #[async_trait]
    impl DataWriterJob for FakeSink {
        async fn split(&self, count: usize) -> anyhow::Result<SplitWriterResult> {
            let config = Arc::new(JobConfig::parse_json(
                r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{}}"#,
            )?);
            Ok(SplitWriterResult {
                tasks: (0..count)
                    .map(|task_id| WriteTask {
                        task_id,
                        config: Arc::clone(&config),
                        mode: relus_common::job_config::WriteMode::Insert,
                        use_transaction: false,
                        batch_size: 1,
                    })
                    .collect(),
            })
        }
        fn description(&self) -> String {
            "progress-fixture".into()
        }
    }
    #[async_trait]
    impl DataWriterTask for FakeSink {
        async fn write_data(
            &self,
            _: WriteTask,
            mut receiver: tokio::sync::mpsc::Receiver<PipelineMessage>,
        ) -> anyhow::Result<usize> {
            let mut written = 0;
            while let Some(message) = receiver.recv().await {
                if let PipelineMessage::DataBatch(rows) = message {
                    written += rows.len();
                }
            }
            Ok(written)
        }
    }

    let (progress, screen) = terminal();
    let progress = Arc::new(progress);
    struct FakePlanning;
    impl PlanningDependencies for FakePlanning {
        fn create_reader(&self, _: Arc<JobConfig>) -> anyhow::Result<relus_reader::Source> {
            Ok(Arc::new(FakeSource))
        }
        fn create_writer(&self, _: Arc<JobConfig>) -> anyhow::Result<relus_writer::Sink> {
            Ok(Arc::new(FakeSink))
        }
        fn build_record_builder(&self, _: &JobConfig) -> anyhow::Result<RecordBuilder> {
            RecordBuilder::new(Default::default(), None)
        }
    }
    let config = Arc::new(JobConfig::parse_json(
        r#"{"source":{"name":"s","type":"x","config":{}},"target":{"name":"t","type":"x","config":{}},"column_mapping":{},"batch_size":1,"channel_buffer_size":1}"#,
    ).unwrap());
    let coordinator = CoordinatorService::with_planning_dependencies(
        StateRepository::new(),
        Arc::new(FakePlanning),
    );
    let handle = coordinator
        .submit_job(
            JobSubmission::new(config)
                .with_options(ExecutionOptions::default().with_progress(progress.clone())),
        )
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), handle.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((result.records_read, result.records_written), (3, 3));
    assert_eq!(progress.counts(), (3, 3));
    assert!(progress.state.lock().unwrap().bars.is_none());
    assert_two_bars(&screen);
    let output = screen.contents();
    assert_eq!(output.matches("Done").count(), 2, "{output}");
    assert!(
        output.contains("Readers: 3    Writers: 3    Workers: 1"),
        "{output}"
    );
    screen.write_line("complete: 3 records").unwrap();
    let final_output = screen.contents();
    assert_eq!(handle.wait().await.unwrap(), result);
    drop(progress);
    assert_eq!(screen.contents(), final_output);
    assert!(
        final_output
            .lines()
            .any(|line| line.starts_with("complete:")),
        "{final_output}"
    );
    assert_two_bars(&screen);
}
