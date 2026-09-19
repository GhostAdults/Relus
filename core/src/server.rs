//! Application runtime bootstrap shared by executable entry points.

use std::future::Future;
use std::process::ExitCode;

/// Starts the application runtime and executes the supplied command dispatcher.
///
/// The concrete CLI command type stays in the `relus_cli` crate; this module
/// owns logging, Tokio runtime sizing, application-state initialization, and
/// process-level success/failure mapping.
pub fn main<F, Fut>(dispatch: F) -> ExitCode
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let _ = relus_common::logging::init_file_logger();
    let default_parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let worker_limit = std::cmp::min(default_parallelism, 16);
    let blocking_limit = 4 * worker_limit;

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_limit)
        .max_blocking_threads(blocking_limit)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("Failed to initialize runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(async {
        let _ = crate::application_state();
        dispatch().await
    }) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
