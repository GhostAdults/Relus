use clap::Parser;
use relus_cli::Cli;

fn main() -> std::process::ExitCode {
    let command = Cli::parse().command;
    relus_core::server::run(|| relus_core::cli::run(command))
}
