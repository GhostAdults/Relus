use crate::engine::contracts::{ProgressObserver, ProgressOutcome, ProgressTopology};
use indicatif::{
    HumanCount, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle,
};
use std::io::IsTerminal;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Owns the entire Job display; no standalone bar may draw outside `multi`.
pub struct IndicatifProgress {
    multi: Option<MultiProgress>,
    state: Mutex<DisplayState>,
}

#[derive(Default, PartialEq, Eq)]
enum Lifecycle {
    #[default]
    Idle,
    Running,
    Finished,
}

#[derive(Default)]
struct DisplayState {
    lifecycle: Lifecycle,
    bars: Option<Bars>,
    read: u64,
    written: u64,
    total: Option<u64>,
    topology: Option<ProgressTopology>,
    started_at: Option<Instant>,
}

struct Bars {
    reader: ProgressBar,
    writer: ProgressBar,
}

impl IndicatifProgress {
    pub fn new() -> Self {
        Self::with_terminal(std::io::stderr().is_terminal())
    }

    fn with_terminal(terminal: bool) -> Self {
        Self {
            multi: terminal.then(|| MultiProgress::with_draw_target(ProgressDrawTarget::stderr())),
            state: Mutex::new(DisplayState::default()),
        }
    }

    fn advance(&self, delta: u64, reader: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.lifecycle != Lifecycle::Running {
            return;
        }
        // Serialize accounting with rendering: concurrent events cannot render
        // an older position after a newer one, or change authoritative totals.
        let count = if reader {
            state.read = state.read.saturating_add(delta);
            state.read
        } else {
            state.written = state.written.saturating_add(delta);
            state.written
        };
        if let Some(bars) = &state.bars {
            let bar = if reader { &bars.reader } else { &bars.writer };
            bar.set_position(count);
        }
    }

    #[cfg(test)]
    fn counts(&self) -> (u64, u64) {
        let state = self.state.lock().unwrap();
        (state.read, state.written)
    }
}

impl Default for IndicatifProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressObserver for IndicatifProgress {
    fn planned(&self, topology: ProgressTopology) {
        if let Ok(mut state) = self.state.lock() {
            if state.lifecycle == Lifecycle::Idle {
                state.topology = Some(topology);
            }
        }
    }

    fn started(&self, total_records: Option<u64>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.lifecycle != Lifecycle::Idle {
            return;
        }
        let started = Instant::now();
        state.lifecycle = Lifecycle::Running;
        state.total = total_records;
        state.started_at = Some(started);
        // Non-TTY still accounts for events, but never constructs a ProgressBar.
        let Some(multi) = &self.multi else { return };
        let Some(reader_style) = style(total_records, started, None, None) else {
            return;
        };
        let Some(writer_style) = style(total_records, started, None, state.topology) else {
            return;
        };
        let reader = hidden_bar(total_records, "Reader", reader_style);
        let writer = hidden_bar(total_records, "Writer", writer_style);
        // Configuring a bar can draw; attach only fully configured hidden bars.
        multi.add(reader.clone());
        multi.add(writer.clone());
        let _ = multi.println(
            "Relus Data Sync\n────────────────────────────────────────────────────────────\n\n",
        );
        reader.tick();
        writer.tick();
        reader.enable_steady_tick(Duration::from_millis(100));
        writer.enable_steady_tick(Duration::from_millis(100));
        state.bars = Some(Bars { reader, writer });
    }

    fn records_read(&self, delta: u64) {
        self.advance(delta, true);
    }

    fn records_sent(&self, delta: u64) {
        self.advance(delta, false);
    }

    fn finished(&self, outcome: ProgressOutcome, records_read: u64, records_written: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if state.lifecycle == Lifecycle::Finished {
            return;
        }
        state.lifecycle = Lifecycle::Finished;
        state.read = records_read;
        state.written = records_written;
        let Some(bars) = state.bars.take() else {
            return;
        };
        bars.reader.disable_steady_tick();
        bars.writer.disable_steady_tick();
        let started = state.started_at.unwrap_or_else(Instant::now);
        let elapsed = started.elapsed();
        for (bar, count, topology) in [
            (&bars.reader, records_read, None),
            (&bars.writer, records_written, state.topology),
        ] {
            if let Some(final_style) = style(state.total, started, Some(elapsed), topology) {
                bar.set_style(final_style);
            }
            bar.set_position(count);
            // finish_with_message forces position=len. Abandon preserves actual
            // counts for failed/cancelled jobs (and successful count corrections).
            bar.abandon_with_message(outcome_message(outcome));
        }
        // Drop both finished bars here, before Engine publishes its result.
        // Indicatif leaves their last frame on screen without another printout.
    }
}

fn hidden_bar(total: Option<u64>, prefix: &'static str, style: ProgressStyle) -> ProgressBar {
    let bar = ProgressBar::with_draw_target(total, ProgressDrawTarget::hidden());
    bar.set_style(style);
    bar.set_prefix(prefix);
    bar
}

fn style(
    total: Option<u64>,
    started: Instant,
    finished_elapsed: Option<Duration>,
    topology: Option<ProgressTopology>,
) -> Option<ProgressStyle> {
    let mut template = if total.is_some() {
        "{prefix} [{bar:30.cyan/green}] {percent:>3}% {human_pos}/{human_len} {msg}\n{stats}"
            .to_string()
    } else {
        "{spinner:.cyan} {prefix} {human_pos} events {msg}\n{stats}".to_string()
    };
    if let Some(topology) = topology {
        template.push_str(&format!(
            "\n\nReaders: {}    Writers: {}    Workers: {}\n\n",
            topology.readers, topology.writers, topology.workers,
        ));
    } else {
        // An actual blank line needs two newlines: the first ends the stats
        // line, the second adds a separate empty line to Indicatif's frame.
        template.push_str("\n\n");
    }
    Some(
        ProgressStyle::with_template(&template)
            .ok()?
            .progress_chars("██░")
            .with_key(
                "stats",
                move |state: &ProgressState, output: &mut dyn std::fmt::Write| {
                    let elapsed = finished_elapsed.unwrap_or_else(|| started.elapsed());
                    let seconds = elapsed.as_secs_f64();
                    // A sub-centisecond sample cannot provide a useful rows/s estimate.
                    let speed = if seconds >= 0.01 {
                        state.pos() as f64 / seconds
                    } else {
                        0.0
                    };
                    let eta = match (total, finished_elapsed) {
                        (Some(total), None) if total > state.pos() && speed > 0.0 => {
                            format_duration((total - state.pos()) as f64 / speed)
                        }
                        _ => "--:--".into(),
                    };
                    let _ = write!(
                        output,
                        "Speed: {} rows/s    Elapsed: {}    ETA: {}",
                        HumanCount(speed.round() as u64),
                        format_duration(seconds),
                        eta
                    );
                },
            ),
    )
}

fn format_duration(seconds: f64) -> String {
    let centiseconds = (seconds.max(0.0) * 100.0).round() as u64;
    format!(
        "{:02}:{:02}.{:02}",
        centiseconds / 6_000,
        (centiseconds / 100) % 60,
        centiseconds % 100
    )
}

fn outcome_message(outcome: ProgressOutcome) -> &'static str {
    match outcome {
        ProgressOutcome::Succeeded => "Done",
        ProgressOutcome::Failed => "Failed",
        ProgressOutcome::Cancelled => "Cancelled",
    }
}

#[cfg(test)]
#[path = "progress/render_tests.rs"]
mod render_tests;
