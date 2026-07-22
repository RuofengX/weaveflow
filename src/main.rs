// weaveflow CLI —— 类似 Docker 的 DAG pipeline 管理命令行。
//
// 配置优先级：CLI 参数 > 环境变量（WEAVEFLOW_*）> 内置默认值。
// clap 的 env 特性完成参数/环境变量合并，cli::config::CliConfig 是统一运行配置层。
//
// 子命令：
//   weaveflow serve --bind 127.0.0.1:9928     （隐藏，daemon 内部使用）
//   weaveflow daemon start [--bind ADDR] [--max-concurrent-tasks N] [--shutdown-drain 30s]
//   weaveflow daemon stop [--timeout 35s]
//   weaveflow daemon restart [...start opts] [--stop-timeout 35s]
//   weaveflow daemon log [-f]
//   weaveflow pipeline apply -f pipe.yaml
//   weaveflow pipeline ls|list
//   weaveflow pipeline inspect <name>
//   weaveflow run <name> [-i k=v] [-i ...] [--watch|--text-output]
//   weaveflow check -f pipe.yaml
//   weaveflow task ls|list
//   weaveflow task snapshot list <task-id>
//   weaveflow task snapshot show <task-id> <seq>
//   weaveflow system prune [--force] [--dry-run]
//   weaveflow system operators
//
// 全局 flag（均可被环境变量覆盖，CLI 优先）：
//   --daemon ADDR            [WEAVEFLOW_DAEMON]          默认 127.0.0.1:9928
//   --output text|json       [WEAVEFLOW_OUTPUT]          默认 text（json = 紧凑单行，面向 Agent）
//   --http-timeout 30s       [WEAVEFLOW_HTTP_TIMEOUT]
//   --connect-timeout 5s     [WEAVEFLOW_CONNECT_TIMEOUT]
//   --ws-timeout 10s         [WEAVEFLOW_WS_TIMEOUT]
//   --prune-timeout 300s     [WEAVEFLOW_PRUNE_TIMEOUT]
//   --log-timeout 2s         [WEAVEFLOW_LOG_TIMEOUT]
//   --log-poll 500ms         [WEAVEFLOW_LOG_POLL]

use clap::{Args, Parser, Subcommand};
use std::time::Duration;

mod cli;
mod server;

use cli::config::{CliConfig, DEFAULT_BIND, OutputFormat, parse_duration};

#[derive(Parser)]
#[command(name = "weaveflow", version = env!("CARGO_PKG_VERSION"), about = "DAG batch processing engine")]
struct Cli {
    #[command(flatten)]
    global: GlobalOpts,

    #[command(subcommand)]
    command: Commands,
}

/// 全局客户端配置 —— 每个flag 均可被同名 WEAVEFLOW_* 环境变量覆盖（CLI 优先）。
#[derive(Args)]
struct GlobalOpts {
    /// Daemon address
    #[arg(long, env = "WEAVEFLOW_DAEMON", default_value = DEFAULT_BIND, global = true)]
    daemon: String,

    /// Output format: text = human-readable, json = compact single-line (for agents/jq)
    #[arg(
        long,
        env = "WEAVEFLOW_OUTPUT",
        value_enum,
        default_value = "text",
        global = true
    )]
    output: OutputFormat,

    /// Total timeout for regular HTTP requests
    #[arg(long, env = "WEAVEFLOW_HTTP_TIMEOUT", value_parser = parse_duration, default_value = "30s", global = true)]
    http_timeout: Duration,

    /// TCP connect timeout
    #[arg(long, env = "WEAVEFLOW_CONNECT_TIMEOUT", value_parser = parse_duration, default_value = "5s", global = true)]
    connect_timeout: Duration,

    /// WebSocket connect timeout (run --watch)
    #[arg(long, env = "WEAVEFLOW_WS_TIMEOUT", value_parser = parse_duration, default_value = "10s", global = true)]
    ws_timeout: Duration,

    /// Total timeout for system prune (full scan + compact can be slow)
    #[arg(long, env = "WEAVEFLOW_PRUNE_TIMEOUT", value_parser = parse_duration, default_value = "300s", global = true)]
    prune_timeout: Duration,

    /// Timeout for a single daemon log fetch
    #[arg(long, env = "WEAVEFLOW_LOG_TIMEOUT", value_parser = parse_duration, default_value = "2s", global = true)]
    log_timeout: Duration,

    /// Poll interval for daemon log -f
    #[arg(long, env = "WEAVEFLOW_LOG_POLL", value_parser = parse_duration, default_value = "500ms", global = true)]
    log_poll: Duration,
}

impl From<&GlobalOpts> for CliConfig {
    fn from(g: &GlobalOpts) -> Self {
        CliConfig {
            daemon: g.daemon.clone(),
            output: g.output,
            http_timeout: g.http_timeout,
            connect_timeout: g.connect_timeout,
            ws_timeout: g.ws_timeout,
            prune_timeout: g.prune_timeout,
            log_timeout: g.log_timeout,
            log_poll: g.log_poll,
        }
    }
}

/// daemon 生命周期（serve/start/restart）共享的配置项。
#[derive(Args)]
struct DaemonOpts {
    /// Bind address
    #[arg(long, env = "WEAVEFLOW_BIND", default_value = DEFAULT_BIND)]
    bind: String,

    /// Maximum number of concurrently running tasks (default: unlimited)
    #[arg(long, env = "WEAVEFLOW_MAX_CONCURRENT_TASKS")]
    max_concurrent_tasks: Option<usize>,

    /// Acknowledge the risk of binding to a non-loopback address without authentication
    #[arg(long)]
    allow_remote: bool,

    /// Graceful shutdown drain window (max wait for in-flight tasks)
    #[arg(long, env = "WEAVEFLOW_SHUTDOWN_DRAIN", value_parser = parse_duration, default_value = "30s")]
    shutdown_drain: Duration,
}

impl From<&DaemonOpts> for server::daemon::ServeConfig {
    fn from(o: &DaemonOpts) -> Self {
        server::daemon::ServeConfig {
            bind: o.bind.clone(),
            max_concurrent_tasks: o.max_concurrent_tasks,
            allow_remote: o.allow_remote,
            shutdown_drain: o.shutdown_drain,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Start the weaveflow HTTP server (internal, called by daemon)
    #[command(hide = true)]
    Serve {
        #[command(flatten)]
        opts: DaemonOpts,
    },

    /// Manage the weaveflow daemon process
    #[command(subcommand)]
    Daemon(DaemonCmd),

    /// Manage pipelines
    #[command(subcommand)]
    Pipeline(PipelineCmd),

    /// Manage routines (long-lived duties delegated to the daemon)
    #[command(subcommand)]
    Routine(RoutineCmd),

    /// Start an MCP server on stdio (for AI agents: Claude Code, opencode, etc.)
    Mcp,

    /// Run a pipeline
    Run {
        /// Pipeline name
        name: String,
        /// Inputs: -i key=value (value may be JSON, or @file.json)
        #[arg(short = 'i', value_parser = parse_key_val)]
        input: Vec<(String, String)>,
        /// Watch progress with TUI
        #[arg(long, conflicts_with = "text_output")]
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
        #[command(flatten)]
        opts: DaemonOpts,
    },
    /// Stop the daemon (SIGTERM, wait, then SIGKILL)
    Stop {
        /// Max wait for graceful exit before SIGKILL (should be >= --shutdown-drain)
        #[arg(long, env = "WEAVEFLOW_STOP_TIMEOUT", value_parser = parse_duration, default_value = "35s")]
        timeout: Duration,
    },
    /// Restart the daemon
    Restart {
        #[command(flatten)]
        opts: DaemonOpts,
        /// Max wait for the old daemon to exit before SIGKILL
        #[arg(long, env = "WEAVEFLOW_STOP_TIMEOUT", value_parser = parse_duration, default_value = "35s")]
        stop_timeout: Duration,
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
    Inspect { name: String },
    /// Delete a pipeline
    Delete { name: String },
}

#[derive(Subcommand)]
enum RoutineCmd {
    /// Create or update a routine from a TOML file (idempotent upsert)
    Apply {
        /// TOML file path
        #[arg(short = 'f', long = "file")]
        file: String,
    },
    /// List all routines with runtime state (alias: list)
    #[command(alias = "list")]
    Ls,
    /// Show routine definition and runtime state
    Inspect { name: String },
    /// Delete a routine and stop its worker
    Delete { name: String },
    /// Push elements into a stream routine buffer (JSON array or single value)
    Push {
        name: String,
        /// JSON string literal (array or single value)
        #[arg(short = 'd', long = "data")]
        data: String,
    },
    /// Read the routine's event inbox (incremental: pass --after <seq> to resume)
    Events {
        name: String,
        /// Only return events with seq > N (agent checkpoint resumption)
        #[arg(long, default_value = "0")]
        after: u64,
    },
}

#[derive(Subcommand)]
enum TaskCmd {
    /// List all tasks (alias: list)
    #[command(alias = "list")]
    Ls,
    /// Show task status (token-friendly summary by default; --full includes inputs + embedded output)
    Show {
        task_id: String,
        /// Full response including inputs and embedded pipeline output
        #[arg(long)]
        full: bool,
    },
    /// Manage task snapshots
    #[command(subcommand)]
    Snapshot(SnapshotCmd),
}

#[derive(Subcommand)]
enum SnapshotCmd {
    /// List snapshots for a task
    List { task_id: String },
    /// Show a specific snapshot
    Show {
        task_id: String,
        seq: u64,
        /// Print超长 base64 字段的完整内容（默认隐藏，仅显示字节长度）
        #[arg(long)]
        full: bool,
        /// 服务端截断：输出超过 N 字节时只回头部预览（省 token）
        #[arg(long)]
        max_bytes: Option<usize>,
    },
}

#[derive(Subcommand)]
enum SystemCmd {
    /// Prune old tasks and data
    Prune {
        /// 忽略各 pipeline 的 result_ttl，删除所有终态任务
        #[arg(long)]
        force: bool,
        #[arg(long)]
        dry_run: bool,
        /// 同时清空全部步骤缓存（CACHE 表 + 其独占 OBJECT）
        #[arg(long)]
        include_cache: bool,
    },
    /// List available operators
    Operators,
}

fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("invalid KEY=value: {s}"))?;
    if k.is_empty() {
        return Err(format!("empty key in KEY=value: {s}"));
    }
    Ok((k.to_string(), v.to_string()))
}

fn exit_on_err(r: Result<(), String>) {
    if let Err(e) = r {
        eprintln!("错误: {e}");
        std::process::exit(1);
    }
}

fn check_pipeline(file: &str, output: OutputFormat) -> Result<(), String> {
    let yaml = std::fs::read_to_string(file).map_err(|e| format!("读取文件 {file}: {e}"))?;
    let def = match weaveflow::dsl::parser::parse(&yaml) {
        Ok(d) => d,
        Err(e) => {
            // ParseError Display 已含"YAML 解析失败:"前缀，不再重复包裹。
            // --output json 下解析失败同样输出结构化报告（与校验失败路径一致）。
            if output.is_json() {
                let v = serde_json::json!({
                    "ok": false,
                    "errors": [{"code": "parse_error", "message": e.to_string()}],
                    "warnings": [],
                });
                println!("{}", serde_json::to_string(&v).unwrap_or_default());
                return Err("解析失败".to_string());
            }
            return Err(e.to_string());
        }
    };
    let report = weaveflow::dsl::validator::validate(&def);

    if output.is_json() {
        let v = serde_json::json!({
            "ok": report.is_ok(),
            "name": def.name,
            "steps": def.steps.len(),
            "slots": def.slots.len(),
            "errors": report.errors.iter()
                .map(|e| serde_json::json!({"code": e.code, "message": e.message}))
                .collect::<Vec<_>>(),
            "warnings": report.warnings.iter()
                .map(|w| serde_json::json!({"code": w.code, "message": w.message}))
                .collect::<Vec<_>>(),
        });
        println!("{}", serde_json::to_string(&v).unwrap_or_default());
        return if report.is_ok() {
            Ok(())
        } else {
            Err(format!("校验失败：{} 个错误", report.errors.len()))
        };
    }

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
        println!(
            "✅ {} — 校验通过（{} step{}，{} slot{}，{} 警告）",
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
    let cfg = CliConfig::from(&cli.global);
    let talks_to_daemon = matches!(
        cli.command,
        Commands::Pipeline(_)
            | Commands::Routine(_)
            | Commands::Run { .. }
            | Commands::Task(_)
            | Commands::System(_)
            | Commands::Daemon(DaemonCmd::Log { .. })
    );
    if talks_to_daemon {
        cli::client::warn_if_build_mismatch(&cfg).await;
    }
    match cli.command {
        Commands::Serve { opts } => {
            server::daemon::serve((&opts).into()).await;
        }
        Commands::Daemon(DaemonCmd::Start { opts }) => {
            server::daemon::start(&(&opts).into()).await;
        }
        Commands::Daemon(DaemonCmd::Stop { timeout }) => {
            server::daemon::stop(timeout).await;
        }
        Commands::Daemon(DaemonCmd::Restart { opts, stop_timeout }) => {
            server::daemon::restart(&(&opts).into(), stop_timeout).await;
        }
        Commands::Daemon(DaemonCmd::Log { live }) => {
            exit_on_err(cli::client::daemon_log(&cfg, live).await);
        }
        Commands::Pipeline(PipelineCmd::Apply { file, data }) => {
            exit_on_err(cli::client::pipeline_apply(&cfg, file.as_deref(), data.as_deref()).await);
        }
        Commands::Pipeline(PipelineCmd::Ls) => {
            exit_on_err(cli::client::pipeline_ls(&cfg).await);
        }
        Commands::Pipeline(PipelineCmd::Inspect { name }) => {
            exit_on_err(cli::client::pipeline_inspect(&cfg, &name).await);
        }
        Commands::Pipeline(PipelineCmd::Delete { name }) => {
            exit_on_err(cli::client::pipeline_delete(&cfg, &name).await);
        }
        Commands::Routine(RoutineCmd::Apply { file }) => {
            exit_on_err(cli::client::routine_apply(&cfg, &file).await);
        }
        Commands::Routine(RoutineCmd::Ls) => {
            exit_on_err(cli::client::routine_ls(&cfg).await);
        }
        Commands::Routine(RoutineCmd::Inspect { name }) => {
            exit_on_err(cli::client::routine_inspect(&cfg, &name).await);
        }
        Commands::Routine(RoutineCmd::Delete { name }) => {
            exit_on_err(cli::client::routine_delete(&cfg, &name).await);
        }
        Commands::Routine(RoutineCmd::Push { name, data }) => {
            exit_on_err(cli::client::routine_push(&cfg, &name, &data).await);
        }
        Commands::Routine(RoutineCmd::Events { name, after }) => {
            exit_on_err(cli::client::routine_events(&cfg, &name, after).await);
        }
        Commands::Run {
            name,
            input,
            watch,
            text_output,
        } => {
            // 非 TTY 场景（管道/CI）自动回落到 text-output 流式模式：
            // 仅提交任务就退出会让失败任务不可见且 exit code 恒为 0。
            use std::io::IsTerminal;
            let auto_text = !watch && !text_output && !std::io::stdout().is_terminal();
            if watch || text_output || auto_text {
                exit_on_err(
                    cli::client::run_pipeline_watch(&cfg, &name, &input, text_output).await,
                );
            } else {
                exit_on_err(cli::client::run_pipeline(&cfg, &name, &input).await);
            }
        }
        Commands::Check { file } => {
            exit_on_err(check_pipeline(&file, cfg.output));
        }
        Commands::Mcp => {
            exit_on_err(cli::mcp::run(cfg).await);
        }
        Commands::Task(TaskCmd::Ls) => {
            exit_on_err(cli::client::task_ls(&cfg).await);
        }
        Commands::Task(TaskCmd::Show { task_id, full }) => {
            exit_on_err(cli::client::task_show(&cfg, &task_id, full).await);
        }
        Commands::Task(TaskCmd::Snapshot(SnapshotCmd::List { task_id })) => {
            exit_on_err(cli::client::snapshot_list(&cfg, &task_id).await);
        }
        Commands::Task(TaskCmd::Snapshot(SnapshotCmd::Show {
            task_id,
            seq,
            full,
            max_bytes,
        })) => {
            exit_on_err(cli::client::snapshot_show(&cfg, &task_id, seq, full, max_bytes).await);
        }
        Commands::System(SystemCmd::Prune {
            force,
            dry_run,
            include_cache,
        }) => {
            exit_on_err(cli::client::system_prune(&cfg, force, dry_run, include_cache).await);
        }
        Commands::System(SystemCmd::Operators) => {
            exit_on_err(cli::client::system_operators(&cfg).await);
        }
    }
}

#[cfg(test)]
mod tests {
    // 所有 clap 解析测试集中在一个 #[test] 中：环境变量是进程级全局状态，
    // 拆成多个并行测试会互相干扰 env 读取。
    use super::*;

    #[test]
    fn cli_parsing_and_precedence() {
        // 1. 默认值
        let cli = Cli::try_parse_from(["weaveflow", "pipeline", "ls"]).unwrap();
        assert_eq!(cli.global.daemon, "127.0.0.1:9928");
        assert_eq!(cli.global.output, OutputFormat::Text);
        assert_eq!(cli.global.http_timeout, Duration::from_secs(30));
        assert_eq!(cli.global.connect_timeout, Duration::from_secs(5));
        assert_eq!(cli.global.ws_timeout, Duration::from_secs(10));
        assert_eq!(cli.global.prune_timeout, Duration::from_secs(300));
        assert_eq!(cli.global.log_timeout, Duration::from_secs(2));
        assert_eq!(cli.global.log_poll, Duration::from_millis(500));

        // 2. 环境变量覆盖默认值
        unsafe {
            std::env::set_var("WEAVEFLOW_DAEMON", "http://example.com:1234/");
            std::env::set_var("WEAVEFLOW_OUTPUT", "json");
            std::env::set_var("WEAVEFLOW_HTTP_TIMEOUT", "90s");
            std::env::set_var("WEAVEFLOW_SHUTDOWN_DRAIN", "45s");
        }
        let cli = Cli::try_parse_from(["weaveflow", "task", "ls"]).unwrap();
        assert_eq!(cli.global.daemon, "http://example.com:1234/");
        assert_eq!(cli.global.output, OutputFormat::Json);
        assert_eq!(cli.global.http_timeout, Duration::from_secs(90));

        // 3. CLI 参数优先于环境变量
        let cli = Cli::try_parse_from([
            "weaveflow",
            "--daemon",
            "10.0.0.1:9928",
            "--output",
            "text",
            "--http-timeout",
            "1s",
            "task",
            "ls",
        ])
        .unwrap();
        assert_eq!(cli.global.daemon, "10.0.0.1:9928");
        assert_eq!(cli.global.output, OutputFormat::Text);
        assert_eq!(cli.global.http_timeout, Duration::from_secs(1));

        // 4. daemon start：env 提供 shutdown-drain
        let cli = Cli::try_parse_from(["weaveflow", "daemon", "start"]).unwrap();
        let Commands::Daemon(DaemonCmd::Start { opts }) = &cli.command else {
            panic!("expect daemon start");
        };
        assert_eq!(opts.shutdown_drain, Duration::from_secs(45));
        assert_eq!(opts.bind, "127.0.0.1:9928");

        // 5. 全局 flag 可出现在子命令之后
        let cli = Cli::try_parse_from(["weaveflow", "task", "ls", "--daemon", "x:1"]).unwrap();
        assert_eq!(cli.global.daemon, "x:1");

        unsafe {
            std::env::remove_var("WEAVEFLOW_DAEMON");
            std::env::remove_var("WEAVEFLOW_OUTPUT");
            std::env::remove_var("WEAVEFLOW_HTTP_TIMEOUT");
            std::env::remove_var("WEAVEFLOW_SHUTDOWN_DRAIN");
        }

        // 6. daemon stop 默认 timeout / 自定义 timeout
        let cli = Cli::try_parse_from(["weaveflow", "daemon", "stop"]).unwrap();
        let Commands::Daemon(DaemonCmd::Stop { timeout }) = &cli.command else {
            panic!("expect daemon stop");
        };
        assert_eq!(*timeout, Duration::from_secs(35));

        // 7. 非法 duration 被拒绝
        assert!(Cli::try_parse_from(["weaveflow", "--http-timeout", "abc", "task", "ls"]).is_err());

        // 8. 非法 output 值被拒绝
        assert!(Cli::try_parse_from(["weaveflow", "--output", "yaml", "task", "ls"]).is_err());

        // 9. parse_key_val
        assert!(parse_key_val("a=1").is_ok());
        assert!(parse_key_val("=1").is_err());
        assert!(parse_key_val("noeq").is_err());
    }
}
