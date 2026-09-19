use super::control::{SchedulerControlHandle, SchedulerError, SchedulerResponse};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const PROMPT: &str = "relus_cli> ";
const AVAILABLE_COMMANDS_HINT: &str =
    "Available commands: status [job_id], submit <path>, cancel <job_id>, exit";

#[derive(Debug, Parser)]
#[command(
    name = "relus-repl",
    no_binary_name = true,
    disable_help_flag = true,
    disable_help_subcommand = true,
    disable_version_flag = true
)]
struct ReplArgs {
    #[command(subcommand)]
    command: ParsedReplCommand,
}

#[derive(Debug, Subcommand)]
enum ParsedReplCommand {
    #[command(disable_help_flag = true)]
    Status {
        #[arg(allow_hyphen_values = true)]
        job_id: Option<String>,
        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        _extra: Vec<String>,
    },
    #[command(disable_help_flag = true)]
    Submit {
        #[arg(allow_hyphen_values = true)]
        path: String,
        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        _extra: Vec<String>,
    },
    #[command(disable_help_flag = true)]
    Cancel {
        #[arg(allow_hyphen_values = true)]
        job_id: String,
        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        _extra: Vec<String>,
    },
    #[command(alias = "quit", disable_help_flag = true)]
    Exit {
        #[arg(allow_hyphen_values = true, trailing_var_arg = true)]
        _extra: Vec<String>,
    },
}

/// REPL 命令
#[derive(Debug)]
pub enum ReplCommand {
    Status { job_id: Option<String> },
    Submit { path: String },
    Cancel { job_id: String },
    Exit,
    Invalid { raw: String, hint: String },
}

/// 简易引号感知 tokenizer
///
/// 支持双引号包裹含空格的参数，如 `submit "C:\my path\job.json"`
/// 不支持转义引号（`\"`），满足当前场景即可。
fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in input.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ' ' | '\t' if !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

impl From<ParsedReplCommand> for ReplCommand {
    fn from(command: ParsedReplCommand) -> Self {
        match command {
            ParsedReplCommand::Status { job_id, .. } => Self::Status { job_id },
            ParsedReplCommand::Submit { path, .. } => Self::Submit { path },
            ParsedReplCommand::Cancel { job_id, .. } => Self::Cancel { job_id },
            ParsedReplCommand::Exit { .. } => Self::Exit,
        }
    }
}

impl ReplCommand {
    pub fn parse(input: &str) -> Self {
        let tokens = tokenize(input);
        let Some(command_name) = tokens.first().cloned() else {
            return Self::Invalid {
                raw: String::new(),
                hint: String::new(),
            };
        };

        if tokens.get(1).is_some_and(|argument| argument == "--") {
            return match command_name.as_str() {
                "status" => Self::Status {
                    job_id: Some("--".to_string()),
                },
                "submit" => Self::Submit {
                    path: "--".to_string(),
                },
                "cancel" => Self::Cancel {
                    job_id: "--".to_string(),
                },
                "exit" | "quit" => Self::Exit,
                _ => Self::unknown(command_name),
            };
        }

        match ReplArgs::try_parse_from(tokens) {
            Ok(args) => args.command.into(),
            Err(_) => match command_name.as_str() {
                "submit" => Self::Invalid {
                    raw: command_name,
                    hint: "Usage: submit <path>".to_string(),
                },
                "cancel" => Self::Invalid {
                    raw: command_name,
                    hint: "Usage: cancel <job_id>".to_string(),
                },
                _ => Self::unknown(command_name),
            },
        }
    }

    fn unknown(command_name: String) -> Self {
        Self::Invalid {
            hint: format!(
                "(error) Unknown command '{}'. {}",
                command_name, AVAILABLE_COMMANDS_HINT
            ),
            raw: command_name,
        }
    }
}

/// 交互式 REPL 循环
///
/// 在独立线程中运行，通过 channel 将用户命令发送给 TaskScheduler。
pub struct ReplLoop {
    control: SchedulerControlHandle,
    cancel: CancellationToken,
}

impl ReplLoop {
    pub fn new(control: SchedulerControlHandle, cancel: CancellationToken) -> Self {
        Self { control, cancel }
    }

    /// 阻塞式 readline 循环，应运行在独立线程中。
    pub fn run(self: Arc<Self>) {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                warn!("Failed to init REPL runtime: {}", e);
                return;
            }
        };

        let mut rl = match rustyline::DefaultEditor::new() {
            Ok(editor) => editor,
            Err(e) => {
                warn!("Failed to init readline, REPL disabled: {}", e);
                return;
            }
        };

        while !self.cancel.is_cancelled() {
            let line = rl.readline(PROMPT);
            match line {
                Ok(input) => {
                    let trimmed = input.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let _ = rl.add_history_entry(trimmed);
                    let cmd = ReplCommand::parse(trimmed);
                    let is_exit = matches!(cmd, ReplCommand::Exit);
                    let output = rt.block_on(self.execute_command(cmd));
                    if !output.is_empty() {
                        println!("{}", output);
                    }
                    if is_exit {
                        break;
                    }
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    let output = rt.block_on(self.execute_command(ReplCommand::Exit));
                    if !output.is_empty() {
                        println!("{}", output);
                    }
                    break;
                }
                Err(rustyline::error::ReadlineError::Eof) => {
                    break;
                }
                Err(e) => {
                    warn!("readline error: {}", e);
                    break;
                }
            }
        }
    }

    async fn execute_command(&self, cmd: ReplCommand) -> String {
        match cmd {
            ReplCommand::Status { job_id } => self
                .control
                .query_tasks(job_id)
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Submit { path } => self
                .control
                .submit_task(path.into())
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Cancel { job_id } => self
                .control
                .cancel_task(job_id)
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Exit => self
                .control
                .shutdown()
                .await
                .map(format_repl_response)
                .unwrap_or_else(format_repl_error),
            ReplCommand::Invalid { raw: _, hint } => hint,
        }
    }
}

fn format_repl_response(response: SchedulerResponse) -> String {
    match response {
        SchedulerResponse::Tasks { tasks, repl_alive } => {
            if tasks.is_empty() {
                return "No tasks.".to_string();
            }

            let mut lines = vec![format!(
                "repl: {}",
                if repl_alive { "alive" } else { "dead" }
            )];
            lines.push(format!(
                "{:<20} {:<12} {:<6} {:>12} {:>12} {:>10}",
                "job_id", "phase", "cdc", "read", "written", "elapsed"
            ));
            lines.push("-".repeat(74));

            for task in tasks {
                let (read, written, elapsed) = match task.stats {
                    Some(stats) => (
                        stats.records_read.to_string(),
                        stats.records_written.to_string(),
                        format!("{:.1}s", stats.elapsed_secs),
                    ),
                    None => ("-".to_string(), "-".to_string(), "-".to_string()),
                };
                lines.push(format!(
                    "{:<20} {:<12} {:<6} {:>12} {:>12} {:>10}",
                    task.job_id, task.phase, task.is_cdc, read, written, elapsed
                ));
            }

            lines.join("\n")
        }
        SchedulerResponse::TaskSubmitted { job_id } => format!("Job '{}' submitted.", job_id),
        SchedulerResponse::TaskCancelled { job_id } => {
            format!("Job '{}' cancel signal sent.", job_id)
        }
        SchedulerResponse::ShutdownRequested => "Scheduler shutdown requested.".to_string(),
    }
}

fn format_repl_error(error: SchedulerError) -> String {
    error.message()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple() {
        assert_eq!(tokenize("status"), vec!["status"]);
        assert_eq!(tokenize("cancel job_1"), vec!["cancel", "job_1"]);
    }

    #[test]
    fn tokenize_quoted_path() {
        assert_eq!(
            tokenize(r#"submit "C:\my path\job.json""#),
            vec!["submit", r"C:\my path\job.json"]
        );
    }

    #[test]
    fn tokenize_empty() {
        assert_eq!(tokenize(""), Vec::<String>::new());
        assert_eq!(tokenize("   "), Vec::<String>::new());
    }

    #[test]
    fn parse_valid_commands() {
        assert!(matches!(ReplCommand::parse("exit"), ReplCommand::Exit));
        assert!(matches!(ReplCommand::parse("quit"), ReplCommand::Exit));
        assert!(matches!(
            ReplCommand::parse("status"),
            ReplCommand::Status { job_id: None }
        ));
        assert!(matches!(
            ReplCommand::parse("status job_1"),
            ReplCommand::Status { job_id: Some(job_id) } if job_id == "job_1"
        ));
        assert!(matches!(
            ReplCommand::parse("submit job.json"),
            ReplCommand::Submit { path } if path == "job.json"
        ));
        assert!(matches!(
            ReplCommand::parse("cancel job_1"),
            ReplCommand::Cancel { job_id } if job_id == "job_1"
        ));
    }

    #[test]
    fn parse_preserves_original_errors() {
        assert!(matches!(
            ReplCommand::parse(""),
            ReplCommand::Invalid { raw, hint } if raw.is_empty() && hint.is_empty()
        ));
        assert!(matches!(
            ReplCommand::parse("submit"),
            ReplCommand::Invalid { raw, hint }
                if raw == "submit" && hint == "Usage: submit <path>"
        ));
        assert!(matches!(
            ReplCommand::parse("cancel"),
            ReplCommand::Invalid { raw, hint }
                if raw == "cancel" && hint == "Usage: cancel <job_id>"
        ));
        assert!(matches!(
            ReplCommand::parse("foo bar"),
            ReplCommand::Invalid { raw, hint }
                if raw == "foo"
                    && hint == "(error) Unknown command 'foo'. Available commands: status [job_id], submit <path>, cancel <job_id>, exit"
        ));
    }

    #[test]
    fn parse_quoted_path() {
        assert!(matches!(
            ReplCommand::parse(r#"submit "C:\my path\job.json""#),
            ReplCommand::Submit { path } if path == r"C:\my path\job.json"
        ));
    }

    #[test]
    fn parse_hyphen_values() {
        assert!(matches!(
            ReplCommand::parse("status --help"),
            ReplCommand::Status { job_id: Some(job_id) } if job_id == "--help"
        ));
        assert!(matches!(
            ReplCommand::parse("submit --config.json"),
            ReplCommand::Submit { path } if path == "--config.json"
        ));
        assert!(matches!(
            ReplCommand::parse("cancel -1"),
            ReplCommand::Cancel { job_id } if job_id == "-1"
        ));
        assert!(matches!(
            ReplCommand::parse("status --"),
            ReplCommand::Status { job_id: Some(job_id) } if job_id == "--"
        ));
        assert!(matches!(
            ReplCommand::parse("submit --"),
            ReplCommand::Submit { path } if path == "--"
        ));
        assert!(matches!(
            ReplCommand::parse("cancel --"),
            ReplCommand::Cancel { job_id } if job_id == "--"
        ));
    }

    #[test]
    fn parse_ignores_extra_arguments() {
        assert!(matches!(
            ReplCommand::parse("status job_1 ignored"),
            ReplCommand::Status { job_id: Some(job_id) } if job_id == "job_1"
        ));
        assert!(matches!(
            ReplCommand::parse("submit job.json ignored"),
            ReplCommand::Submit { path } if path == "job.json"
        ));
        assert!(matches!(
            ReplCommand::parse("cancel job_1 ignored"),
            ReplCommand::Cancel { job_id } if job_id == "job_1"
        ));
        assert!(matches!(
            ReplCommand::parse("quit ignored"),
            ReplCommand::Exit
        ));
    }

    #[test]
    fn parse_commands_are_case_sensitive() {
        assert!(matches!(
            ReplCommand::parse("Status"),
            ReplCommand::Invalid { raw, .. } if raw == "Status"
        ));
        assert!(matches!(
            ReplCommand::parse("QUIT"),
            ReplCommand::Invalid { raw, .. } if raw == "QUIT"
        ));
    }
}
