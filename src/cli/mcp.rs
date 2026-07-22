//! MCP stdio server：把 weaveflow daemon 的能力以 MCP tools 暴露给智能体。
//!
//! 薄客户端架构：与 CLI 共用同一套 HTTP client + CliConfig（WEAVEFLOW_DAEMON
//! 等环境变量），daemon 本身零改动。stdio 是 MCP 传输通道，因此本模块
//! 绝不向 stdout 写日志；错误一律通过 tool result 的 is_error 返回。
//!
//! 工具设计原则（面向智能体）：
//! - 返回结构化 JSON（structured content），紧凑无装饰；
//! - 默认省 token：任务状态走 summary 模式，大输出必须显式传 max_bytes 才全量返回；
//! - 描述文本写明典型工作流与 token 成本。

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::schemars::JsonSchema;
use rmcp::{ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use serde::Deserialize;
use serde_json::Value;

use super::client;
use super::config::CliConfig;

// ── 参数类型 ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, JsonSchema)]
pub struct NameParam {
    /// Resource name (pipeline or routine)
    pub name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ApplyPipelineParam {
    /// Full pipeline YAML text (weaveflow DSL: name/slots/steps/output)
    pub yaml: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RunPipelineParam {
    /// Pipeline name (must already be applied)
    pub name: String,
    /// Slot inputs as a JSON object (optional)
    pub inputs: Option<Value>,
    /// Wait for terminal state before returning (default true).
    /// Set false for long-running tasks: get the task_id and poll get_task_status.
    pub wait: Option<bool>,
    /// Max seconds to wait when wait=true (default 300, capped at 3600)
    pub timeout_sec: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TaskParam {
    /// Task UUID returned by run_pipeline
    pub task_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SnapshotParam {
    /// Task UUID
    pub task_id: String,
    /// Snapshot sequence number (from list_snapshots)
    pub seq: u64,
    /// Truncate output to N bytes (head preview). Strongly recommended
    /// for http/file/command step outputs to save tokens.
    pub max_bytes: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct UpsertRoutineParam {
    /// Routine definition as JSON: {name, pipeline, type: "cron"|"stream",
    /// cron?: {...}, stream?: {...}, notify?: {webhook_url?, preview_bytes?}}
    pub def: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct PushRoutineParam {
    /// Stream routine name
    pub name: String,
    /// JSON array of elements (a single value is auto-wrapped into an array)
    pub items: Value,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct RoutineEventsParam {
    /// Routine name
    pub name: String,
    /// Only return events with seq > N. Persist the max seq you have seen
    /// and pass it on the next poll for incremental catch-up. Default 0.
    pub after: Option<u64>,
    /// Max events to return (default 50)
    pub limit: Option<usize>,
}

// ── MCP server ──────────────────────────────────────────────────────────

/// MCP 要求 outputSchema 的根类型为 object；serde_json::Value 的 schema
/// 无根 type，统一包一层 result 字段。
#[derive(Debug, serde::Serialize, JsonSchema)]
pub struct ToolOutput {
    /// The tool's JSON payload (shape depends on the tool; see tool description)
    pub result: Value,
}

fn out(v: Value) -> Json<ToolOutput> {
    Json(ToolOutput { result: v })
}

#[derive(Clone)]
pub struct WeaveflowMcp {
    cfg: CliConfig,
}

#[tool_router]
impl WeaveflowMcp {
    pub fn new(cfg: CliConfig) -> Self {
        Self { cfg }
    }

    // ── pipeline ──

    #[tool(
        description = "Validate a weaveflow pipeline YAML locally (no daemon needed). \
        ALWAYS do this before apply_pipeline. Returns {ok, errors, warnings}."
    )]
    async fn validate_pipeline(
        &self,
        Parameters(p): Parameters<ApplyPipelineParam>,
    ) -> Result<Json<ToolOutput>, String> {
        let def = match weaveflow::dsl::parser::parse(&p.yaml) {
            Ok(d) => d,
            Err(e) => {
                return Ok(out(serde_json::json!({
                    "ok": false,
                    "errors": [{"code": "parse", "message": e.to_string()}],
                })));
            }
        };
        let report = weaveflow::dsl::validator::validate(&def);
        Ok(out(serde_json::json!({
            "ok": report.is_ok(),
            "name": def.name,
            "steps": def.steps.len(),
            "errors": report.errors.iter()
                .map(|e| serde_json::json!({"code": e.code, "message": e.message}))
                .collect::<Vec<_>>(),
            "warnings": report.warnings.iter()
                .map(|w| serde_json::json!({"code": w.code, "message": w.message}))
                .collect::<Vec<_>>(),
        })))
    }

    #[tool(
        description = "Create or update a pipeline on the daemon (idempotent upsert by name). \
        Validate first with validate_pipeline."
    )]
    async fn apply_pipeline(
        &self,
        Parameters(p): Parameters<ApplyPipelineParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::post_body(&self.cfg, "/pipelines", p.yaml)
            .await
            .map(out)
    }

    #[tool(description = "List all registered pipelines (id + name only, cheap).")]
    async fn list_pipelines(&self) -> Result<Json<ToolOutput>, String> {
        client::get(&self.cfg, "/pipelines").await.map(out)
    }

    #[tool(
        description = "Get the full definition of a pipeline (steps, slots, output). \
        Large-ish; prefer list_pipelines unless you need the details."
    )]
    async fn inspect_pipeline(
        &self,
        Parameters(p): Parameters<NameParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::get(
            &self.cfg,
            &format!("/pipelines/{}", client::encode_segment(&p.name)),
        )
        .await
        .map(out)
    }

    #[tool(
        description = "Delete a pipeline by name. Routines referencing it will fail on next fire."
    )]
    async fn delete_pipeline(
        &self,
        Parameters(p): Parameters<NameParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::delete(
            &self.cfg,
            &format!("/pipelines/{}", client::encode_segment(&p.name)),
        )
        .await
        .map(out)
    }

    #[tool(
        description = "List builtin step operators (type names + descriptions) for writing pipeline YAML."
    )]
    async fn list_operators(&self) -> Result<Json<ToolOutput>, String> {
        client::get(&self.cfg, "/system/operators").await.map(out)
    }

    // ── run / task ──

    #[tool(
        description = "Run a pipeline. By default waits for completion and returns a token-friendly \
        summary (no embedded output — use list_snapshots/get_snapshot for step outputs). \
        Set wait=false for long tasks, then poll get_task_status."
    )]
    async fn run_pipeline(
        &self,
        Parameters(p): Parameters<RunPipelineParam>,
    ) -> Result<Json<ToolOutput>, String> {
        let body = serde_json::json!({
            "pipeline": p.name,
            "inputs": p.inputs.unwrap_or(serde_json::json!({})),
        });
        let resp = client::post(&self.cfg, "/runs", body).await?;
        let wait = p.wait.unwrap_or(true);
        if !wait {
            return Ok(out(resp));
        }
        let task_id = resp["task_id"]
            .as_str()
            .ok_or_else(|| "response missing task_id".to_string())?;
        let timeout = std::time::Duration::from_secs(p.timeout_sec.unwrap_or(300).min(3600));
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let status = self.task_summary(task_id).await?;
            let s = status["status"].as_str().unwrap_or("");
            if s != "running" {
                return Ok(out(status));
            }
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "task {task_id} still running after {}s — poll get_task_status instead",
                    timeout.as_secs()
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    #[tool(
        description = "Get task status in token-friendly summary mode (status string + per-step \
        states/durations; no inputs, no embedded output)."
    )]
    async fn get_task_status(
        &self,
        Parameters(p): Parameters<TaskParam>,
    ) -> Result<Json<ToolOutput>, String> {
        self.task_summary(&p.task_id).await.map(out)
    }

    #[tool(description = "Get the pipeline's final output for a completed task \
        (full output — may be large; only available for ~10 min after completion and until prune). \
        For step-level data prefer list_snapshots + get_snapshot with max_bytes.")]
    async fn get_task_result(
        &self,
        Parameters(p): Parameters<TaskParam>,
    ) -> Result<Json<ToolOutput>, String> {
        let v = client::get(&self.cfg, &format!("/runs/{}", p.task_id)).await?;
        Ok(out(v))
    }

    #[tool(
        description = "List a task's step snapshots (seq + step_id only, cheap). \
        Fetch individual outputs with get_snapshot."
    )]
    async fn list_snapshots(
        &self,
        Parameters(p): Parameters<TaskParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::get(&self.cfg, &format!("/runs/{}/snapshots", p.task_id))
            .await
            .map(out)
    }

    #[tool(
        description = "Get one step's output. STRONGLY recommended to pass max_bytes \
        (e.g. 2000) for http/file/command steps — full outputs can be tens of KB."
    )]
    async fn get_snapshot(
        &self,
        Parameters(p): Parameters<SnapshotParam>,
    ) -> Result<Json<ToolOutput>, String> {
        let path = match p.max_bytes {
            Some(n) => format!("/runs/{}/snapshots/{}?max_bytes={n}", p.task_id, p.seq),
            None => format!("/runs/{}/snapshots/{}", p.task_id, p.seq),
        };
        client::get(&self.cfg, &path).await.map(out)
    }

    // ── routine（智能体值班） ──

    #[tool(
        description = "List routines (long-lived duties delegated to the daemon) with runtime state: \
        total_fired, next_fire_at, etc."
    )]
    async fn list_routines(&self) -> Result<Json<ToolOutput>, String> {
        client::get(&self.cfg, "/routines").await.map(out)
    }

    #[tool(
        description = "Get one routine's definition + runtime state (including stream buffer size)."
    )]
    async fn inspect_routine(
        &self,
        Parameters(p): Parameters<NameParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::get(
            &self.cfg,
            &format!("/routines/{}", client::encode_segment(&p.name)),
        )
        .await
        .map(out)
    }

    #[tool(
        description = "Create or update a routine (idempotent upsert; hot-reloads its worker). \
        type 'cron': {cron: {schedule|interval, inputs?, misfire?}}; \
        type 'stream': {stream: {batch_size?, flush_interval?, slot?, max_in_flight?, buffer_cap?}}. \
        Optional notify: {webhook_url, preview_bytes} — terminal events are POSTed to the URL \
        (agent-on-duty pattern: a harness receives the webhook and wakes the owning agent)."
    )]
    async fn upsert_routine(
        &self,
        Parameters(p): Parameters<UpsertRoutineParam>,
    ) -> Result<Json<ToolOutput>, String> {
        let name = p.def["name"]
            .as_str()
            .ok_or_else(|| "def.name is required".to_string())?
            .to_string();
        // 本地预校验，把配置错误挡在 daemon 之外
        let def: weaveflow::routine::RoutineDef =
            serde_json::from_value(p.def).map_err(|e| format!("invalid routine def: {e}"))?;
        let errors = weaveflow::routine::validate_routine(&def);
        if !errors.is_empty() {
            return Err(format!("routine validation failed: {}", errors.join("; ")));
        }
        let body = serde_json::to_value(&def).map_err(|e| e.to_string())?;
        client::put(
            &self.cfg,
            &format!("/routines/{}", client::encode_segment(&name)),
            body,
        )
        .await
        .map(out)
    }

    #[tool(
        description = "Delete a routine (stops its worker, flushes stream buffer, clears its event inbox)."
    )]
    async fn delete_routine(
        &self,
        Parameters(p): Parameters<NameParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::delete(
            &self.cfg,
            &format!("/routines/{}", client::encode_segment(&p.name)),
        )
        .await
        .map(out)
    }

    #[tool(
        description = "Push elements into a stream routine's buffer (micro-batched into pipeline runs)."
    )]
    async fn push_routine(
        &self,
        Parameters(p): Parameters<PushRoutineParam>,
    ) -> Result<Json<ToolOutput>, String> {
        client::post(
            &self.cfg,
            &format!("/routines/{}/push", client::encode_segment(&p.name)),
            p.items,
        )
        .await
        .map(out)
    }

    #[tool(
        description = "Read a routine's persisted event inbox (fired/task_completed/task_failed/\
        dropped/notify_failed). This is how you catch up on what your routines did while you were \
        gone: pass after=<last seq you saw> for incremental reads and persist the new max seq."
    )]
    async fn get_routine_events(
        &self,
        Parameters(p): Parameters<RoutineEventsParam>,
    ) -> Result<Json<ToolOutput>, String> {
        let after = p.after.unwrap_or(0);
        let limit = p.limit.unwrap_or(50);
        client::get(
            &self.cfg,
            &format!(
                "/routines/{}/events?after={after}&limit={limit}",
                client::encode_segment(&p.name)
            ),
        )
        .await
        .map(out)
    }

    // ── 内部 ──

    async fn task_summary(&self, task_id: &str) -> Result<Value, String> {
        client::get(&self.cfg, &format!("/runs/{task_id}?summary=1")).await
    }
}

#[tool_handler(
    name = "weaveflow",
    instructions = "weaveflow is your out-of-context data engine: declare YAML pipelines of \
    deterministic operators (http/js/filter/sort/dedup/merge/base64/file/command/llm), run them \
    on bulk data with per-step snapshots and caching, and delegate long-lived duties as routines \
    (cron schedule or stream micro-batches). Typical flow: validate_pipeline → apply_pipeline → \
    run_pipeline → list_snapshots/get_snapshot. For recurring work: upsert_routine, then poll \
    get_routine_events with a persisted seq cursor to catch up after your session ends."
)]
impl ServerHandler for WeaveflowMcp {}

/// 运行 MCP stdio server（阻塞直到客户端断开）。
pub async fn run(cfg: CliConfig) -> Result<(), String> {
    let server = WeaveflowMcp::new(cfg);
    let service = server
        .serve(rmcp::transport::io::stdio())
        .await
        .map_err(|e| format!("MCP server 初始化失败: {e}"))?;
    service
        .waiting()
        .await
        .map_err(|e| format!("MCP server 运行错误: {e}"))?;
    Ok(())
}
