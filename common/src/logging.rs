use chrono::Local;
use std::io::Write;
use std::sync::Mutex;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::EnvFilter;

struct ChronoLocalTimer;

impl FormatTime for ChronoLocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z"))
    }
}

/// 在 exe 所在目录下创建 `logs/YYYY-MM-DD.log`，并以本地时间格式初始化 tracing 日志。
///
/// # Panics
///
/// 当无法获取 exe 路径、无法创建目录或无法创建日志文件时 panic。
pub fn init_file_logger() {
    let exe_dir = std::env::current_exe()
        .expect("无法获取可执行文件路径")
        .parent()
        .expect("无法获取可执行文件所在目录")
        .to_path_buf();

    let log_dir = exe_dir.join("logs");
    std::fs::create_dir_all(&log_dir).expect("无法创建 logs 目录");

    let log_name = format!("{}.log", Local::now().format("%Y-%m-%d"));
    let log_path = log_dir.join(&log_name);
    let log_file = std::fs::File::create(&log_path)
        .unwrap_or_else(|_| panic!("无法创建日志文件: {}", log_path.display()));
    let log_file = Mutex::new(log_file);

    tracing_subscriber::fmt()
        .with_timer(ChronoLocalTimer)
        .with_writer(move || {
            let file = log_file
                .lock()
                .unwrap()
                .try_clone()
                .expect("clone log file");
            Box::new(std::io::BufWriter::new(file)) as Box<dyn Write + Send>
        })
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
}
