// weave CLI — Docker-like CLI for DAG pipeline management.
//
// Subcommands:
//   weave serve --bind 127.0.0.1:9928     (hidden, daemon-internal)
//   weave daemon start [--bind ADDR]
//   weave daemon stop
//   weave pipeline apply -f pipe.yaml
//   weave pipeline ls|list
//   weave pipeline inspect <name>
//   weave run <name> [-i k=v] [-i ...]
//   weave task ls|list
//   weave task snapshot list <task-id>
//   weave task snapshot show <task-id> <seq>
//   weave system prune [--force] [--dry-run]

use clap::{Parser, Subcommand};

mod cli;
mod server;

const DEFAULT_BIND: &str = "127.0.0.1:9928";

#[derive(Parser)]
#[command(name = "weave", version = env!("CARGO_PKG_VERSION"), about = "DAG batch processing engine")]
struct Cli {
    /// Daemon address (default: 127.0.0.1:9928)
    #[arg(long, default_value = DEFAULT_BIND, global = true)]
    daemon: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the weave HTTP server (internal, called by daemon)
    #[command(hide = true)]
    Serve {
        #[arg(long, default_value = DEFAULT_BIND)]
        bind: String,
        /// Maximum number of concurrently running tasks (default: unlimited).
        /// Also configurable via WEAVE_MAX_CONCURRENT_TASKS env var.
        #[arg(long, env = "WEAVE_MAX_CONCURRENT_TASKS")]
        max_concurrent_tasks: Option<usize>,
        /// Acknowledge the risk of binding to a non-loopback address without authentication
        #[arg(long)]
        allow_remote: bool,
    },

    /// Manage the weave daemon process
    #[command(subcommand)]
    Daemon(DaemonCmd),

    /// Manage pipelines
    #[command(subcommand)]
    Pipeline(PipelineCmd),

    /// Run a pipeline
    Run {
        /// Pipeline name
        name: String,
        /// Inputs: -i key=value
        #[arg(short = 'i', value_parser = parse_key_val)]
        input: Vec<(String, String)>,
        /// Watch progress with TUI
        #[arg(long)]
        watch: bool,
        /// Plain text output (for CI/CD, agents)
        #[arg(long)]
        text_output: bool,
    },

    /// Validate a pipeline YAML locally (no daemon required)
    Check {
        /// YAML file path
        #[arg(short = 'f', long = "file")]
        file: String,
    },

    /// Manage tasks
    #[command(subcommand)]
    Task(TaskCmd),

    /// System operations
    #[command(subcommand)]
    System(SystemCmd),
}

#[derive(Subcommand)]
enum DaemonCmd {
    /// Start the daemon in background
    Start {
        #[arg(long, default_value = DEFAULT_BIND)]
        bind: String,
        /// Maximum number of concurrently running tasks (default: unlimited).
        /// Also configurable via WEAVE_MAX_CONCURRENT_TASKS env var.
        #[arg(long, env = "WEAVE_MAX_CONCURRENT_TASKS")]
        max_concurrent_tasks: Option<usize>,
        /// Acknowledge the risk of binding to a non-loopback address without authentication
        #[arg(long)]
        allow_remote: bool,
    },
    /// Stop the daemon
    Stop,
    /// Restart the daemon
    Restart {
        #[arg(long, default_value = DEFAULT_BIND)]
        bind: String,
        /// Maximum number of concurrently running tasks (default: unlimited).
        /// Also configurable via WEAVE_MAX_CONCURRENT_TASKS env var.
        #[arg(long, env = "WEAVE_MAX_CONCURRENT_TASKS")]
        max_concurrent_tasks: Option<usize>,
        /// Acknowledge the risk of binding to a non-loopback address without authentication
        #[arg(long)]
        allow_remote: bool,
    },
    /// View daemon logs
    Log {
        /// Follow (tail -f) mode
        #[arg(short = 'f', long = "live")]
        live: bool,
    },
}

#[derive(Subcommand)]
enum PipelineCmd {
    /// Create or update a pipeline. Accepts YAML from --file, --data, or stdin.
    Apply {
        /// YAML file path
        #[arg(short = 'f', long = "file")]
        file: Option<String>,
        /// YAML string literal
        #[arg(short = 'd', long = "data")]
        data: Option<String>,
    },
    /// List all pipelines (alias: list)
    #[command(alias = "list")]
    Ls,
    /// Show pipeline definition
    Inspect {
        name: String,
    },
    /// Delete a pipeline
    Delete {
        name: String,
    },
}

#[derive(Subcommand)]
enum TaskCmd {
    /// List all tasks (alias: list)
    #[command(alias = "list")]
    Ls,
    /// Manage task snapshots
    #[command(subcommand)]
    Snapshot(SnapshotCmd),
}

#[derive(Subcommand)]
enum SnapshotCmd {
    /// List snapshots for a task
    List {
        task_id: String,
    },
    /// Show a specific snapshot
    Show {
        task_id: String,
        seq: u64,
    },
}

#[derive(Subcommand)]
enum SystemCmd {
    /// Prune old tasks and data
    Prune {
        #[arg(long)]
        force: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// List available operators
    Operators,
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let (k, v) = s.split_once('=').ok_or_else(|| format!("invalid KEY=value: {s}"))?;
    Ok((k.to_string(), v.to_string()))
}

async fn daemon_log(addr: &str, live: bool) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap();
    let mut offset = 0usize;

    loop {
        let url = format!("http://{addr}/system/logs?offset={offset}");
        match client.get(&url).send().await {
            Ok(resp) => {
                let new_offset_hdr = resp
                    .headers()
                    .get("X-Log-Offset")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<usize>().ok());
                if let Ok(body) = resp.text().await {
                    let new_offset = new_offset_hdr.unwrap_or(offset + body.len());
                    if !body.is_empty() {
                        print!("{body}");
                    }
                    offset = new_offset;
                }
                if !live {
                    return;
                }
            }
            Err(e) => {
                eprintln!("daemon log: {e}");
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn exit_on_err(r: Result<(), String>) {
    if let Err(e) = r {
        eprintln!("错误: {e}");
        std::process::exit(1);
    }
}

fn check_pipeline(file: &str) -> Result<(), String> {
    let yaml = std::fs::read_to_string(file)
        .map_err(|e| format!("读取文件 {file}: {e}"))?;
    let def = weave::dsl::parser::parse(&yaml)
        .map_err(|e| format!("YAML 解析失败: {e}"))?;
    let report = weave::dsl::validator::validate(&def, &Default::default());

    if !report.errors.is_empty() {
        eprintln!("错误 ({} 项):", report.errors.len());
        for err in &report.errors {
            eprintln!("  [{}] {}", err.code, err.message);
        }
    }
    if !report.warnings.is_empty() {
        eprintln!("警告 ({} 项):", report.warnings.len());
        for warn in &report.warnings {
            eprintln!("  [{}] {}", warn.code, warn.message);
        }
    }

    if report.is_ok() {
        println!("✅ {} — 校验通过（{} step{}，{} slot{}，{} 警告）",
            def.name,
            def.steps.len(),
            if def.steps.len() == 1 { "" } else { "s" },
            def.slots.len(),
            if def.slots.len() == 1 { "" } else { "s" },
            report.warnings.len(),
        );
        Ok(())
    } else {
        Err(format!("校验失败：{} 个错误", report.errors.len()))
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let daemon = &cli.daemon;
    match cli.command {
        Commands::Serve { bind, max_concurrent_tasks, allow_remote } => {
            let mut args = vec!["serve".to_string(), "--bind".to_string(), bind];
            if let Some(n) = max_concurrent_tasks {
                args.push("--max-concurrent-tasks".to_string());
                args.push(n.to_string());
            }
            if allow_remote {
                args.push("--allow-remote".to_string());
            }
            // pass through original data-dir args
            let orig: Vec<String> = std::env::args().collect();
            for w in orig.windows(2) {
                if w[0] == "--data-dir" || w[0] == "-d" {
                    args.push(w[0].clone());
                    args.push(w[1].clone());
                }
            }
            server::daemon::serve(args).await;
        }
        Commands::Daemon(DaemonCmd::Start { bind, max_concurrent_tasks, allow_remote }) => {
            server::daemon::start(&bind, max_concurrent_tasks, allow_remote).await;
        }
        Commands::Daemon(DaemonCmd::Stop) => {
            server::daemon::stop().await;
        }
        Commands::Daemon(DaemonCmd::Restart { bind, max_concurrent_tasks, allow_remote }) => {
            server::daemon::restart(&bind, max_concurrent_tasks, allow_remote).await;
        }
        Commands::Daemon(DaemonCmd::Log { live }) => {
            daemon_log(daemon, live).await;
        }
        Commands::Pipeline(PipelineCmd::Apply { file, data }) => {
            exit_on_err(cli::client::pipeline_apply(daemon, file.as_deref(), data.as_deref()).await);
        }
        Commands::Pipeline(PipelineCmd::Ls) => {
            exit_on_err(cli::client::pipeline_ls(daemon).await);
        }
        Commands::Pipeline(PipelineCmd::Inspect { name }) => {
            exit_on_err(cli::client::pipeline_inspect(daemon, &name).await);
        }
        Commands::Pipeline(PipelineCmd::Delete { name }) => {
            exit_on_err(cli::client::pipeline_delete(daemon, &name).await);
        }
        Commands::Run { name, input, watch, text_output } => {
            if watch || text_output {
                exit_on_err(cli::client::run_pipeline_watch(daemon, &name, &input, text_output).await);
            } else {
                exit_on_err(cli::client::run_pipeline(daemon, &name, &input).await);
            }
        }
        Commands::Check { file } => {
            exit_on_err(check_pipeline(&file));
        }
        Commands::Task(TaskCmd::Ls) => {
            exit_on_err(cli::client::task_ls(daemon).await);
        }
        Commands::Task(TaskCmd::Snapshot(SnapshotCmd::List { task_id })) => {
            exit_on_err(cli::client::snapshot_list(daemon, &task_id).await);
        }
        Commands::Task(TaskCmd::Snapshot(SnapshotCmd::Show { task_id, seq })) => {
            exit_on_err(cli::client::snapshot_show(daemon, &task_id, seq).await);
        }
        Commands::System(SystemCmd::Prune { force, dry_run }) => {
            exit_on_err(cli::client::system_prune(daemon, force, dry_run).await);
        }
        Commands::System(SystemCmd::Operators) => {
            exit_on_err(cli::client::system_operators(daemon).await);
        }
    }
}
