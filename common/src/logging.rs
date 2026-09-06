use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use chrono::Local;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::EnvFilter;

static LOGGER_INIT_RESULT: OnceLock<std::result::Result<(), String>> = OnceLock::new();

struct ChronoLocalTimer;

impl FormatTime for ChronoLocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// Creates `logs/YYYY-MM-DD.log` next to the executable and initializes tracing.
pub fn init_file_logger() -> Result<()> {
    match LOGGER_INIT_RESULT
        .get_or_init(|| initialize_file_logger().map_err(|error| error.to_string()))
    {
        Ok(()) => Ok(()),
        Err(error) => Err(anyhow::anyhow!(error.clone())),
    }
}

fn initialize_file_logger() -> Result<()> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."));

    let log_dir = exe_dir.join("logs");
    std::fs::create_dir_all(&log_dir)
        .with_context(|| format!("Failed to create logs directory: {}", log_dir.display()))?;

    let log_name = format!("{}.log", Local::now().format("%Y-%m-%d"));
    let log_path = log_dir.join(log_name);
    let log_file = std::fs::File::create(&log_path)
        .with_context(|| format!("Failed to create log file: {}", log_path.display()))?;
    let log_file = Mutex::new(log_file);

    if let Err(error) = tracing_subscriber::fmt()
        .with_timer(ChronoLocalTimer)
        .with_writer(move || {
            let file = match log_file.lock() {
                Ok(file) => file,
                Err(error) => {
                    eprintln!("Log file lock is poisoned: {error}");
                    return Box::new(std::io::sink()) as Box<dyn Write + Send>;
                }
            };
            match file.try_clone() {
                Ok(file) => Box::new(std::io::BufWriter::new(file)) as Box<dyn Write + Send>,
                Err(error) => {
                    eprintln!("Failed to clone log file handle: {error}");
                    Box::new(std::io::sink()) as Box<dyn Write + Send>
                }
            }
        })
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .try_init()
    {
        return Err(anyhow::anyhow!(
            "Failed to initialize tracing subscriber: {error}"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::init_file_logger;

    #[test]
    fn file_logger_initialization_is_idempotent() {
        assert!(init_file_logger().is_ok());
        assert!(init_file_logger().is_ok());
    }
}
