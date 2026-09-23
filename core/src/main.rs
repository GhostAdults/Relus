use relus_cli::Cli;

fn main() -> std::process::ExitCode {
    let command = Cli::parse_with_version(env!("CARGO_PKG_VERSION")).command;
    relus_core::server::run(|| relus_core::cli::run(command))
}
