use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use weave::dsl::{parser::parse, StepId};
use weave::dsl::validator::validate;
use weave::error::WeaveError;
use weave::engine::dag::Dag;
use weave::engine::runner::Runner;
use weave::store::{Database, PruneOptions};
use weave::tracker::{LayerInfo, TaskId, TaskSnapshot, TaskStatus, TaskTracker};

use super::logging::RingWriter;

type DbRef = Arc<Database>;
type TrackerRef = TaskTracker;

/// 优雅停机时等待后台任务排空的最长时间。
const SHUTDOWN_DRAIN_SECS: u64 = 30;

// ── State ────────────────────────────────────────────────────────────────

struct AppState {
    db: DbRef,
    tracker: TrackerRef,
    semaphore: Arc<tokio::sync::Semaphore>,
    log_ring: RingWriter,
    /// 停机中：/runs 拒绝新任务（503）。
    draining: Arc<AtomicBool>,
    /// 运行中的后台 pipeline 任务数。
    in_flight: Arc<AtomicUsize>,
    drain_notify: Arc<tokio::sync::Notify>,
}

#[derive(Deserialize)]
struct RunRequest {
    pipeline: String,
    inputs: Option<HashMap<String, Value>>,
}

#[derive(Serialize)]
struct TaskResponse {
    task_id: String,
    pipeline_name: String,
    inputs: Value,
    created_at: String,
    snapshot_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<Value>,
}

#[derive(Serialize)]
struct SnapshotMeta {
    seq: u64,
    step_id: StepId,
}

#[derive(Serialize)]
struct SnapshotResponse {
    seq: u64,
    step_id: StepId,
    output: Value,
}

#[derive(Deserialize)]
struct PruneRequest {
    pipeline: Option<String>,
    force: Option<bool>,
    dry_run: Option<bool>,
}

#[derive(Serialize)]
struct PruneResponse {
    tasks_removed: u64,
    snapshots_removed: u64,
    objects_removed: u64,
    cache_entries_removed: u64,
    bytes_freed: u64,
    dry_run: bool,
}

// ── Handlers ──────────────────────────────────────────────────────────────

async fn create_pipeline(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Result<Json<Value>, WeaveError> {
    tracing::info!(len = body.len(), "POST /pipelines");
    let pipeline = parse(&body)?;
    tracing::info!(name = %pipeline.name, steps = pipeline.steps.len(), "pipeline parsed");
    let report = validate(&pipeline);
    if !report.is_ok() {
        let msgs: Vec<String> = report
            .errors
            .iter()
            .map(|e| format!("[{}] {}", e.code, e.message))
            .collect();
        tracing::warn!(errors = %msgs.join("; "), "validation failed");
        return Err(WeaveError::Validation(msgs.join("; ")));
    }

    let builtins = weave::operator::builtins();
    for step in &pipeline.steps {
        let op_type = step.op.op_type();
        if op_type != "js" && !builtins.contains_key(op_type) {
            return Err(WeaveError::Validation(format!(
                "未注册的步骤类型: {}（步骤: {}）",
                op_type, step.id
            )));
        }
    }

    let pid = state.db.save_pipeline_upsert(&pipeline)?;
    tracing::info!(pipeline_id = %pid, name = %pipeline.name, "pipeline saved");
    let response = serde_json::json!({
        "id": pid.to_string(),
        "name": &*pipeline.name,
        "steps": pipeline.steps.len(),
        "slots": pipeline.slots,
    });
    Ok(Json(response))
}

async fn delete_pipeline(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, WeaveError> {
    tracing::info!(pipeline = %name, "DELETE /pipelines/:name");
    let (pid, _) = state
        .db
        .find_pipeline_by_name(&name)?
        .ok_or_else(|| WeaveError::NotFound(format!("pipeline {name} not found")))?;
    state.db.delete_pipeline(&pid)?;
    tracing::info!(pipeline_id = %pid, "pipeline deleted");
    Ok(Json(serde_json::json!({"deleted": name})))
}

async fn list_pipelines(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, WeaveError> {
    tracing::info!("GET /pipelines");
    let items = state.db.list_pipelines()?;
    let list: Vec<Value> = items
        .iter()
        .map(|(pid, def)| {
            serde_json::json!({"id": pid.to_string(), "name": &*def.name})
        })
        .collect();
    Ok(Json(serde_json::json!(list)))
}

async fn get_pipeline(
    State(state): State<Arc<AppState>>,
    Path(name_or_id): Path<String>,
) -> Result<Json<Value>, WeaveError> {
    tracing::info!(pipeline = %name_or_id, "GET /pipelines/:name");
    let pipeline = if let Some((_, p)) = state.db.find_pipeline_by_name(&name_or_id)? {
        p
    } else if let Ok(uuid) = uuid::Uuid::parse_str(&name_or_id) {
        let pid = weave::tracker::PipelineId(uuid);
        state
            .db
            .load_pipeline(&pid)?
            .ok_or_else(|| WeaveError::NotFound(format!("pipeline {name_or_id} not found")))?
    } else {
        return Err(WeaveError::NotFound(format!("pipeline {name_or_id} not found")));
    };
    let val =
        serde_json::to_value(&pipeline).map_err(|e| WeaveError::Internal(e.to_string()))?;
    Ok(Json(val))
}

async fn run_pipeline(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RunRequest>,
) -> Result<Json<Value>, WeaveError> {
    // 先占 in_flight 计数再复查 draining（反向顺序存在 TOCTOU：
    // 检查通过后信号触发、wait_for_drain 读到 0 返回，任务才被计入）。
    state.in_flight.fetch_add(1, Ordering::SeqCst);
    if state.draining.load(Ordering::SeqCst) {
        state.in_flight.fetch_sub(1, Ordering::SeqCst);
        return Err(WeaveError::Unavailable(
            "daemon 正在停机，不再接受新任务".to_string(),
        ));
    }
    tracing::info!(pipeline = %req.pipeline, "POST /runs");

    // Load pipeline
    let pipeline = match state.db.find_pipeline_by_name(&req.pipeline)? {
        Some((_pid, p)) => p,
        None => {
            return Err(WeaveError::NotFound(format!(
                "pipeline {} not found",
                req.pipeline
            )))
        }
    };

    let inputs = req.inputs.unwrap_or_default();
    tracing::info!(pipeline = %pipeline.name, input_keys = ?inputs.keys().collect::<Vec<_>>(), "run inputs resolved");

    // Build DAG layers for progress display
    let dag = Dag::from_pipeline(&pipeline)?;
    let layers = dag.topological_sort()?;
    let steps_with_timeout: Vec<(StepId, Option<f64>)> = layers
        .iter()
        .flatten()
        .map(|id| (id.clone(), dag.step(id).and_then(|s| s.timeout_sec)))
        .collect();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo {
            index: i,
            step_ids: step_ids.clone(),
        })
        .collect();

    // Create task in DB
    let task_id = state.db.create_task(
        &pipeline.name,
        serde_json::json!(inputs),
        result_ttl_secs(&pipeline),
    )?;

    // Register with tracker
    let (_rx, snapshot) = state
        .tracker
        .create(
            task_id,
            pipeline.name.to_string(),
            steps_with_timeout,
            layer_infos,
        )
        .await;

    // Spawn executor in background; permit 在后台任务内获取，HTTP 立即返回，
    // 避免并发打满时请求挂起无界。in_flight 计数由外层 watcher 无条件回收
    // （runner panic 时 unwind 会跳过内层回收，导致 drain 永远等不到归零）。
    let state_clone = state.clone();
    let pipeline_clone = pipeline.clone();
    let tid = task_id;
    let handle = tokio::spawn(async move {
        match state_clone.semaphore.clone().acquire_owned().await {
            Ok(_permit) => {
                let runner = Runner::new(
                    pipeline_clone,
                    state_clone.db.clone(),
                    state_clone.tracker.clone(),
                );

                let result = runner.run(tid, inputs).await;
                match &result {
                    Ok(_) => tracing::info!(task_id = %tid, "pipeline run completed"),
                    Err(e) => tracing::error!(task_id = %tid, error = %e, "pipeline run failed"),
                }
            }
            Err(_) => {
                state_clone
                    .tracker
                    .fail(&tid, "task semaphore closed".into())
                    .await;
                if let Err(e) = state_clone
                    .db
                    .set_task_status(&tid, weave::tracker::meta::TASK_STATUS_FAILED)
                {
                    tracing::warn!(task_id = %tid, error = %e, "set_task_status(failed) failed");
                }
            }
        }
    });

    let state_watcher = state.clone();
    tokio::spawn(async move {
        match handle.await {
            Ok(()) => {}
            Err(e) => {
                if e.is_panic() {
                    let msg = e
                        .try_into_panic()
                        .map(|p| {
                            p.downcast_ref::<String>()
                                .cloned()
                                .or_else(|| {
                                    p.downcast_ref::<&str>().map(|s| s.to_string())
                                })
                                .unwrap_or_else(|| "unknown panic".to_string())
                        })
                        .unwrap_or_else(|_| "internal panic (cancelled)".to_string());
                    tracing::error!(task_id = %tid, error = %msg, "runner panicked");
                    // panic 路径下同层 in-flight 步骤的 future 被 drop，状态会
                    // 永远停留在 Running —— 统一收口为 Failed。
                    let step_err = format!("task panicked: {msg}");
                    state_watcher
                        .tracker
                        .fail_non_terminal_steps(&tid, &step_err)
                        .await;
                    state_watcher
                        .tracker
                        .fail(&tid, format!("internal panic: {msg}"))
                        .await;
                    if let Err(db_err) = state_watcher
                        .db
                        .set_task_status(&tid, weave::tracker::meta::TASK_STATUS_FAILED)
                    {
                        tracing::warn!(task_id = %tid, error = %db_err, "set_task_status(failed) after panic failed");
                    }
                }
            }
        }
        // in_flight 无条件回收（含 runner panic 路径），保证 wait_for_drain 可归零。
        if state_watcher.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            state_watcher.drain_notify.notify_waiters();
        }
    });

    tracing::info!(task_id = %task_id, "task submitted to background runner");

    let response = serde_json::json!({
        "task_id": task_id.to_string(),
        "pipeline_name": pipeline.name,
        "status": snapshot.status,
        "layers": snapshot.layers,
    });
    Ok(Json(response))
}

/// 任务结果保留时长：pipeline 的 storage.result_ttl（下限 60s），缺省 3600s。
fn result_ttl_secs(pipeline: &weave::dsl::PipelineDef) -> i64 {
    pipeline
        .storage
        .as_ref()
        .and_then(|s| s.result_ttl)
        .map(|td| td.0.num_seconds().max(60))
        .unwrap_or(3600)
}

async fn list_tasks(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, WeaveError> {
    tracing::info!("GET /tasks");
    let tasks = state.db.list_tasks()?;
    let list: Vec<Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "task_id": t.task_id.to_string(),
                "pipeline_name": t.pipeline_name,
                "created_at": t.created_at.to_rfc3339(),
                "status": t.status,
            })
        })
        .collect();
    Ok(Json(serde_json::json!(list)))
}

async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskResponse>, WeaveError> {
    tracing::info!(task_id = %task_id, "GET /runs/:task_id");
    let tid = parse_task_id(&task_id)?;
    let task = match state.db.load_task(&tid)? {
        Some(t) => t,
        None => {
            return Err(WeaveError::NotFound(format!(
                "task {task_id} not found"
            )));
        }
    };
    let snapshot_count = state.db.count_snapshots(&tid)?;

    let progress = state
        .tracker
        .get(&tid)
        .await
        .and_then(|s| serde_json::to_value(s).ok());

    Ok(Json(TaskResponse {
        task_id,
        pipeline_name: task.pipeline_name,
        inputs: task.inputs,
        created_at: task.created_at.to_rfc3339(),
        snapshot_count,
        progress,
    }))
}

async fn list_snapshots(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<Vec<SnapshotMeta>>, WeaveError> {
    tracing::info!(task_id = %task_id, "GET /runs/:task_id/snapshots");
    let tid = parse_task_id(&task_id)?;
    let keys = state.db.list_snapshot_keys(&tid)?;
    let snaps: Vec<SnapshotMeta> = keys
        .into_iter()
        .map(|(seq, step_id)| SnapshotMeta { seq, step_id })
        .collect();
    Ok(Json(snaps))
}

async fn get_snapshot_by_seq(
    State(state): State<Arc<AppState>>,
    Path((task_id, seq)): Path<(String, u64)>,
) -> Result<Json<SnapshotResponse>, WeaveError> {
    tracing::info!(task_id = %task_id, seq = seq, "GET /runs/:task_id/snapshots/:seq");
    let tid = parse_task_id(&task_id)?;
    let snap = state.db.load_snapshot_by_seq(&tid, seq)?;
    match snap {
        Some(snap) => {
            let output: Value = match serde_json::from_slice::<Value>(&snap.output) {
                Ok(v) => {
                    if v.is_null() && !snap.output.is_empty() {
                        serde_json::json!({
                            "_anomalous_null": true,
                            "_notice": "step output is JSON null — may indicate a silent failure or unhandled error",
                            "_raw_size": snap.output.len(),
                            "_raw_hex": snap.output.iter().take(16).map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" "),
                        })
                    } else {
                        v
                    }
                }
                Err(_) => {
                    use base64::Engine;
                    let b64 = base64::engine::general_purpose::STANDARD.encode(&snap.output);
                    serde_json::json!({
                        "_binary": true,
                        "_size": snap.output.len(),
                        "_base64": b64,
                    })
                }
            };
            Ok(Json(SnapshotResponse {
                seq: snap.seq,
                step_id: snap.step_id,
                output,
            }))
        }
        None => Err(WeaveError::NotFound(format!(
            "snapshot {seq} for task {task_id} not found"
        ))),
    }
}

async fn prune_tasks(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PruneRequest>,
) -> Result<Json<PruneResponse>, WeaveError> {
    tracing::info!(pipeline = ?req.pipeline, force = req.force, dry_run = req.dry_run, "POST /prune");
    let options = PruneOptions {
        pipeline: req.pipeline,
        force: req.force.unwrap_or(false),
        dry_run: req.dry_run.unwrap_or(false),
        skip_tasks: state.tracker.running_task_ids(),
    };
    let plan = state.db.prune_scan(&options)?;
    let report = state.db.prune_execute(&plan, options.dry_run)?;
    tracing::info!(tasks = report.tasks_removed, objects = report.objects_removed, bytes = report.bytes_freed, "prune complete");
    Ok(Json(PruneResponse {
        tasks_removed: report.tasks_removed,
        snapshots_removed: report.snapshots_removed,
        objects_removed: report.objects_removed,
        cache_entries_removed: report.cache_entries_removed,
        bytes_freed: report.bytes_freed,
        dry_run: options.dry_run,
    }))
}

async fn list_operators(
) -> Result<Json<Value>, WeaveError> {
    tracing::info!("GET /system/operators");
    let builtins = weave::operator::builtins();
    let list: Vec<Value> = builtins.values().map(|op| {
            let spec = op.spec();
            serde_json::json!({
                "type_name": spec.type_name,
                "description": spec.description,
                "iterate": spec.iterate,
                "cache": spec.cache,
            })
        })
        .collect();
    Ok(Json(serde_json::json!(list)))
}

async fn get_logs(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    let offset: u64 = params.get("offset").and_then(|v| v.parse().ok()).unwrap_or(0);
    let chunk = state.log_ring.read_since(offset);
    let mut text = String::new();
    if chunk.truncated {
        text.push_str("…日志有缺失（ring buffer 已覆盖旧内容）…\n");
    }
    text.push_str(&String::from_utf8_lossy(&chunk.bytes));
    let mut resp = axum::response::Response::new(axum::body::Body::from(text));
    resp.headers_mut().insert(
        "X-Log-Offset",
        chunk.next_offset.to_string().parse().unwrap(),
    );
    if chunk.truncated {
        resp.headers_mut().insert("X-Log-Truncated", "1".parse().unwrap());
    }
    resp.headers_mut().insert(
        "content-type",
        "text/plain; charset=utf-8".parse().unwrap(),
    );
    resp
}

// ── WebSocket ─────────────────────────────────────────────────────────────

async fn ws_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, WeaveError> {
    tracing::info!(task_id = %task_id, "GET /runs/:task_id/ws");
    let tid = parse_task_id(&task_id)?;

    let (initial, rx) = match state.tracker.snapshot_and_subscribe(&tid).await {
        Some((snap, rx)) => (Some(snap), rx),
        None => {
            return Err(WeaveError::NotFound(format!(
                "task {task_id} not found"
            )));
        }
    };

    Ok(ws.on_upgrade(move |socket| handle_ws(socket, rx, initial)))
}

async fn handle_ws(
    mut socket: WebSocket,
    mut rx: tokio::sync::broadcast::Receiver<Vec<u8>>,
    initial: Option<TaskSnapshot>,
) {
    if let Some(ref snap) = initial {
        let bytes = serde_json::to_vec(snap).unwrap_or_default();
        if socket
            .send(Message::Text(String::from_utf8_lossy(&bytes).into()))
            .await
            .is_err()
        {
            return;
        }
        if matches!(
            snap.status,
            TaskStatus::Completed(_) | TaskStatus::Failed(_)
        ) {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    }

    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    Some(Ok(_)) => {}
                }
            }
            result = rx.recv() => {
                match result {
                    Ok(bytes) => {
                        if socket
                            .send(Message::Text(
                                String::from_utf8_lossy(&bytes).into(),
                            ))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        let v: Value =
                            serde_json::from_slice(&bytes).unwrap_or_default();
                        if v["status"]
                            .as_object()
                            .is_some_and(|o| {
                                o.contains_key("Completed")
                                    || o.contains_key("Failed")
                            })
                        {
                            let _ = socket.send(Message::Close(None)).await;
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        }
    }
}

// ── Operator ───────────────────────────────────────────────────────────────

fn parse_task_id(s: &str) -> Result<TaskId, WeaveError> {
    let uuid = uuid::Uuid::parse_str(s)
        .map_err(|_| WeaveError::BadRequest(format!("invalid task id: {s}")))?;
    Ok(TaskId(uuid))
}

fn build_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/pipelines", post(create_pipeline).get(list_pipelines))
        .route(
            "/pipelines/:name",
            get(get_pipeline).delete(delete_pipeline),
        )
        .route("/runs", post(run_pipeline))
        .route("/tasks", get(list_tasks))
        .route("/runs/:task_id", get(get_task))
        .route("/runs/:task_id/ws", get(ws_task))
        .route("/runs/:task_id/snapshots", get(list_snapshots))
        .route(
            "/runs/:task_id/snapshots/:seq",
            get(get_snapshot_by_seq),
        )
        .route("/prune", post(prune_tasks))
        .route("/system/operators", get(list_operators))
        .route("/system/logs", get(get_logs))
        .with_state(state)
}

// ── Serve ─────────────────────────────────────────────────────────────────

fn is_loopback_bind(addr: &str) -> bool {
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn enforce_bind_safety(bind: &str, allow_remote: bool) {
    if is_loopback_bind(bind) {
        return;
    }
    if !allow_remote {
        eprintln!(
            "error: refusing to bind to non-loopback address {bind} without --allow-remote.\n\
             The weave daemon has NO authentication: anyone who can reach this port can create\n\
             pipelines and execute arbitrary commands on this machine via the command/file operators.\n\
             Re-run with --allow-remote to acknowledge this risk, or bind to 127.0.0.1 (default)."
        );
        std::process::exit(1);
    }
    eprintln!(
        "WARNING: weave daemon has NO authentication and is binding to non-loopback address {bind}.\n\
         Anyone who can reach this port can execute arbitrary commands on this machine.\n\
         Consider binding to 127.0.0.1 instead, or placing the daemon behind an authenticated\n\
         reverse proxy / firewall."
    );
}

pub async fn serve(args: Vec<String>) {
    let log_ring = super::logging::init_logging();

    let bind = match args.iter().position(|a| a == "--bind") {
        Some(i) => match args.get(i + 1) {
            Some(v) => v.clone(),
            None => {
                eprintln!("error: --bind 缺少参数值");
                std::process::exit(1);
            }
        },
        None => "127.0.0.1:9928".to_string(),
    };

    let allow_remote = args.iter().any(|a| a == "--allow-remote");
    enforce_bind_safety(&bind, allow_remote);

    let max_concurrent = match args.iter().position(|a| a == "--max-concurrent-tasks") {
        Some(i) => match args.get(i + 1).and_then(|s| s.parse::<usize>().ok()) {
            Some(n) => Some(n),
            None => {
                eprintln!("error: --max-concurrent-tasks 需要一个非负整数参数值");
                std::process::exit(1);
            }
        },
        None => match std::env::var("WEAVE_MAX_CONCURRENT_TASKS") {
            Ok(s) => match s.parse::<usize>() {
                Ok(n) => Some(n),
                Err(_) => {
                    eprintln!("error: WEAVE_MAX_CONCURRENT_TASKS 非法值: {s:?}（需要非负整数）");
                    std::process::exit(1);
                }
            },
            Err(_) => None,
        },
    };

    let semaphore_permits = max_concurrent.unwrap_or(usize::MAX >> 3).max(1);

    let data_dir = resolve_data_dir();
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        eprintln!(
            "error: 无法创建数据目录 {}: {e}（检查权限或 WEAVE_DATA 设置）",
            data_dir.display()
        );
        std::process::exit(1);
    }
    let db_path = data_dir.join("weave.redb");
    let db = match Database::open(&db_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: 无法打开数据库 {}: {e}", db_path.display());
            std::process::exit(1);
        }
    };
    let interrupted = db.mark_interrupted_tasks().unwrap_or(0);
    if interrupted > 0 {
        tracing::warn!(interrupted, "marked tasks from previous run as interrupted");
    }
    let state = Arc::new(AppState {
        db: Arc::new(db),
        tracker: TaskTracker::new(),
        semaphore: Arc::new(tokio::sync::Semaphore::new(semaphore_permits)),
        log_ring,
        draining: Arc::new(AtomicBool::new(false)),
        in_flight: Arc::new(AtomicUsize::new(0)),
        drain_notify: Arc::new(tokio::sync::Notify::new()),
    });

    {
        let tracker = state.tracker.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let removed = tracker.cleanup_stale();
                if removed > 0 {
                    tracing::info!(removed, "tracker cleanup: removed stale tasks");
                }
            }
        });
    }

    let app = build_app(state.clone());
    tracing::info!("weave serve listening on {bind}");
    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: 无法监听 {bind}: {e}");
            std::process::exit(1);
        }
    };
    let shutdown_state = state.clone();
    let shutdown_signal = async move {
        #[cfg(unix)]
        let sigterm = async {
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(mut s) => {
                    s.recv().await;
                    tracing::info!("received SIGTERM, shutting down");
                }
                Err(e) => {
                    tracing::error!(error = %e, "SIGTERM handler 注册失败");
                    std::future::pending::<()>().await;
                }
            }
        };
        #[cfg(not(unix))]
        let sigterm = std::future::pending();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("received Ctrl-C, shutting down");
            }
            _ = sigterm => {}
        }

        // 停止接受新任务，排空运行中的后台任务（最多 SHUTDOWN_DRAIN_SECS 秒）
        shutdown_state.draining.store(true, Ordering::SeqCst);
        let remaining = wait_for_drain(
            &shutdown_state.in_flight,
            &shutdown_state.drain_notify,
            std::time::Duration::from_secs(SHUTDOWN_DRAIN_SECS),
        )
        .await;
        if remaining > 0 {
            tracing::warn!(remaining, "shutdown: 后台任务排空超时，强制退出");
        }
    };
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
    {
        eprintln!("error: HTTP server 异常退出: {e}");
        std::process::exit(1);
    }
}

/// 等待 in_flight 归零；返回剩余任务数（0 = 排空完成）。
async fn wait_for_drain(
    in_flight: &AtomicUsize,
    notify: &tokio::sync::Notify,
    timeout: std::time::Duration,
) -> usize {
    let start = std::time::Instant::now();
    loop {
        let n = in_flight.load(Ordering::SeqCst);
        if n == 0 {
            tracing::info!("shutdown: 后台任务已排空");
            return 0;
        }
        if start.elapsed() >= timeout {
            return n;
        }
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            notify.notified(),
        )
        .await;
    }
}

fn resolve_data_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("WEAVE_DATA") {
        return std::path::PathBuf::from(dir);
    }
    dirs_next::home_dir()
        .map(|h| h.join(".weave"))
        .unwrap_or_else(|| std::path::PathBuf::from(".weave"))
}

// ── Daemon ────────────────────────────────────────────────────────────────

fn pid_path() -> std::path::PathBuf {
    resolve_data_dir().join("weave.pid")
}

fn log_file_path() -> std::path::PathBuf {
    resolve_data_dir().join("weave.log")
}

fn verify_pid_binary(pid: u32, expected_exe: &std::path::Path) -> bool {
    let exe_link = format!("/proc/{pid}/exe");
    match std::fs::canonicalize(&exe_link) {
        Ok(pid_exe) => match std::fs::canonicalize(expected_exe) {
            Ok(my_exe) => pid_exe == my_exe,
            Err(_) => false,
        },
        Err(_) => false,
    }
}

fn is_daemon_running() -> bool {
    let path = pid_path();
    let pid_str = match std::fs::read_to_string(&path) {
        Ok(s) => s.trim().to_string(),
        Err(_) => return false,
    };
    let pid: u32 = match pid_str.parse() {
        Ok(p) => p,
        Err(_) => {
            let _ = std::fs::remove_file(&path);
            return false;
        }
    };
    // kill(pid, 0) 返回 EPERM 表示进程存在但属于其他用户 —— 视为存活，
    // 不得删除有效 pidfile（否则 start 会再拉起一个导致 bind 冲突/孤儿）。
    let alive = unsafe { libc::kill(pid as i32, 0) } == 0
        || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
    if !alive {
        let _ = std::fs::remove_file(&path);
        return false;
    }
    // PID 存活但二进制不匹配（PID 复用）→ 视为 stale pidfile
    let exe = std::env::current_exe().unwrap_or_default();
    if !verify_pid_binary(pid, &exe) {
        let _ = std::fs::remove_file(&path);
        return false;
    }
    true
}

pub async fn start(bind: &str, max_concurrent_tasks: Option<usize>, allow_remote: bool) {
    if is_daemon_running() {
        eprintln!(
            "daemon is already running. Use `weave daemon restart` to restart."
        );
        return;
    }
    enforce_bind_safety(bind, allow_remote);

    let data_dir = resolve_data_dir();
    if let Err(e) = std::fs::create_dir_all(&data_dir) {
        eprintln!(
            "error: 无法创建数据目录 {}: {e}（检查权限或 WEAVE_DATA 设置）",
            data_dir.display()
        );
        std::process::exit(1);
    }

    let log_path = log_file_path();
    let log_file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "error: 无法打开日志文件 {}: {e}（目录不可创建或权限不足）",
                log_path.display()
            );
            std::process::exit(1);
        }
    };

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: 无法获取当前可执行文件路径: {e}");
            std::process::exit(1);
        }
    };
    let mut cmd = tokio::process::Command::new(&exe);
    cmd.arg("serve")
        .arg("--bind")
        .arg(bind)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log_file));
    if let Some(n) = max_concurrent_tasks {
        cmd.arg("--max-concurrent-tasks").arg(n.to_string());
    }
    if allow_remote {
        cmd.arg("--allow-remote");
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: 无法启动 daemon 子进程: {e}");
            std::process::exit(1);
        }
    };
    let pid = match child.id() {
        Some(p) => p,
        None => {
            eprintln!("error: 无法获取 daemon 子进程 PID");
            std::process::exit(1);
        }
    };

    // spawn 成功后立即写 pidfile（健康检查之前），避免竞态窗口内 pidfile 缺失
    let path = pid_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    if let Err(e) = std::fs::write(&path, pid.to_string()) {
        eprintln!("error: 无法写入 PID 文件 {}: {e}", path.display());
        std::process::exit(1);
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap_or_default();
    let check_bind = if let Some(rest) = bind.strip_prefix("0.0.0.0") {
        format!("127.0.0.1{rest}")
    } else if let Some(rest) = bind.strip_prefix("[::]") {
        format!("[::1]{rest}")
    } else {
        bind.to_string()
    };
    let check_url = format!("http://{}/system/operators", check_bind);

    let healthy = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            // 子进程已退出（如 bind 冲突）时立即判失败 —— 否则健康检查可能
            // 打到端口上另一个返回 200 的进程而误报成功（pidfile 指向死 PID）。
            match child.try_wait() {
                Ok(Some(status)) => {
                    eprintln!("daemon 子进程提前退出: {status}");
                    return false;
                }
                Ok(None) => {}
                Err(e) => {
                    eprintln!("daemon 子进程状态检查失败: {e}");
                    return false;
                }
            }
            match client.get(&check_url).send().await {
                Ok(resp) if resp.status().is_success() => return true,
                _ => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
    })
    .await
    .unwrap_or(false);

    if !healthy {
        eprintln!(
            "daemon failed to start (health check timed out). Check log: {}",
            log_path.display()
        );
        kill_child_process(pid).await;
        // 仅当 pidfile 内容等于本次 spawn 的 PID 时才删除
        let ours = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            == Some(pid);
        if ours {
            let _ = std::fs::remove_file(&path);
        }
        std::process::exit(1);
    }

    println!("daemon started (PID {pid}) on {bind}");
    std::mem::forget(child);
}

/// SIGTERM → 等 2s（ESRCH 轮询）→ 必要时 SIGKILL。
async fn kill_child_process(pid: u32) {
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
    let exited = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if unsafe { libc::kill(pid as i32, 0) } != 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .is_ok();
    if !exited {
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
    }
}

pub async fn stop() {
    let path = pid_path();
    let pid_str = match std::fs::read_to_string(&path) {
        Ok(s) => s.trim().to_string(),
        Err(_) => {
            eprintln!(
                "daemon not running (no PID file at {})",
                path.display()
            );
            return;
        }
    };
    let pid: u32 = match pid_str.parse() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("stale PID file found (invalid content), removing...");
            let _ = std::fs::remove_file(&path);
            eprintln!("daemon not running");
            return;
        }
    };

    let exe = std::env::current_exe().unwrap_or_default();
    if !verify_pid_binary(pid, &exe) {
        eprintln!(
            "PID {pid} does not belong to this binary — refusing to kill (PID may have been reused).\n\
             Removing stale PID file."
        );
        let _ = std::fs::remove_file(&path);
        return;
    }

    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }

    let exited = tokio::time::timeout(
        // 必须 ≥ serve 端 drain 上限（SHUTDOWN_DRAIN_SECS），否则长任务
        // 会被 SIGKILL 强杀于执行中途，优雅停机形同虚设。
        std::time::Duration::from_secs(SHUTDOWN_DRAIN_SECS + 5),
        async {
            loop {
                if unsafe { libc::kill(pid as i32, 0) } != 0 {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        },
    )
    .await
    .unwrap_or(false);

    if !exited {
        eprintln!("daemon did not exit after SIGTERM, sending SIGKILL...");
        unsafe {
            libc::kill(pid as i32, libc::SIGKILL);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    let _ = std::fs::remove_file(&path);
    println!("daemon stopped (PID {pid})");
}

pub async fn restart(bind: &str, max_concurrent_tasks: Option<usize>, allow_remote: bool) {
    if is_daemon_running() {
        stop().await;
    }
    start(bind, max_concurrent_tasks, allow_remote).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::logging::RingWriter;
    use std::path::PathBuf;

    fn temp_db() -> (Database, PathBuf) {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "weave-server-test-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok();
        let db = Database::open(dir.join("weave.redb")).expect("open db");
        (db, dir)
    }

    fn test_state(db: Arc<Database>) -> Arc<AppState> {
        Arc::new(AppState {
            db,
            tracker: TaskTracker::new(),
            semaphore: Arc::new(tokio::sync::Semaphore::new(usize::MAX >> 3)),
            log_ring: RingWriter::new(),
            draining: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            drain_notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    #[test]
    fn loopback_bind_detection() {
        assert!(is_loopback_bind("127.0.0.1:9928"));
        assert!(is_loopback_bind("127.0.0.2:9928"));
        assert!(is_loopback_bind("localhost:9928"));
        assert!(is_loopback_bind("LOCALHOST:9928"));
        assert!(is_loopback_bind("[::1]:9928"));
        assert!(!is_loopback_bind("0.0.0.0:9928"));
        assert!(!is_loopback_bind("[::]:9928"));
        assert!(!is_loopback_bind("192.168.1.10:9928"));
        assert!(!is_loopback_bind("example.com:9928"));
    }

    #[test]
    fn verify_pid_binary_own_process() {
        let pid = std::process::id();
        let exe = std::env::current_exe().unwrap();
        assert!(verify_pid_binary(pid, &exe));
    }

    #[test]
    fn verify_pid_binary_wrong_binary() {
        let pid = std::process::id();
        let fake_exe = std::path::PathBuf::from("/usr/bin/false");
        assert!(!verify_pid_binary(pid, &fake_exe));
    }

    #[test]
    fn verify_pid_binary_nonexistent_pid() {
        assert!(!verify_pid_binary(99999999, &std::env::current_exe().unwrap()));
    }

    #[test]
    fn parse_pid_rejects_garbage() {
        assert!("abc".parse::<u32>().is_err());
        assert!("".parse::<u32>().is_err());
        assert!("123xyz".parse::<u32>().is_err());
        assert_eq!("12345".parse::<u32>().unwrap(), 12345);
    }

    #[tokio::test]
    async fn smoke_pipeline_crud() {
        let (db, _dir) = temp_db();
        let state = test_state(Arc::new(db));

        let app = build_app(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let yaml = r#"
name: test_pipeline
steps:
  - id: greet
    type: noop
output: "{greet.output}"
"#;
        let client = reqwest::Client::new();

        // POST /pipelines
        let resp = client
            .post(format!("http://{addr}/pipelines"))
            .header("content-type", "text/plain")
            .body(yaml.to_string())
            .send()
            .await
            .expect("create pipeline");
        assert_eq!(resp.status(), 200);
        let create_body: Value = resp.json().await.unwrap();
        let pipeline_id = create_body["id"].as_str().unwrap().to_string();

        // GET /pipelines
        let resp = client
            .get(format!("http://{addr}/pipelines"))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 1);

        // GET /pipelines/:id
        let resp = client
            .get(format!("http://{addr}/pipelines/{pipeline_id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // POST /runs — now returns task_id immediately (async)
        let resp = client
            .post(format!("http://{addr}/runs"))
            .json(&serde_json::json!({"pipeline": "test_pipeline"}))
            .send()
            .await
            .expect("run pipeline");
        assert_eq!(resp.status(), 200);
        let run_body: Value = resp.json().await.unwrap();
        assert!(run_body.get("task_id").is_some());

        // Wait for background task to complete
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // GET /runs/:task_id — should have progress
        let task_id = run_body["task_id"].as_str().unwrap();
        let resp = client
            .get(format!("http://{addr}/runs/{task_id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let task_body: Value = resp.json().await.unwrap();
        assert!(task_body.get("snapshot_count").is_some());

        // Verify task completed (not stuck in Running)
        let progress = task_body.get("progress").expect("progress field missing");
        let status = progress.get("status").expect("status field missing");
        let status_keys: Vec<&str> = status.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        assert!(
            status_keys.contains(&"Completed") || status_keys.contains(&"Failed"),
            "expected Completed or Failed, got: {:?}",
            status_keys
        );

        handle.abort();
        let _ = handle.await;
    }

    #[test]
    fn result_ttl_secs_reads_pipeline_storage() {
        let yaml = r#"
name: ttl_pipe
storage:
  result_ttl: "2h"
steps:
  - id: s
    type: noop
output: "{s.output}"
"#;
        let def = weave::dsl::parser::parse(yaml).expect("parse");
        assert_eq!(result_ttl_secs(&def), 7200);

        // 未配置 storage → 默认 3600
        let yaml_no_storage = r#"
name: ttl_default
steps:
  - id: s
    type: noop
output: "{s.output}"
"#;
        let def = weave::dsl::parser::parse(yaml_no_storage).expect("parse");
        assert_eq!(result_ttl_secs(&def), 3600);

        // 过小的 TTL → 下限 60s
        let yaml_tiny = r#"
name: ttl_tiny
storage:
  result_ttl: "30s"
steps:
  - id: s
    type: noop
output: "{s.output}"
"#;
        let def = weave::dsl::parser::parse(yaml_tiny).expect("parse");
        assert_eq!(result_ttl_secs(&def), 60);
    }

    #[tokio::test]
    async fn run_task_uses_pipeline_result_ttl() {
        let (db, _dir) = temp_db();
        let db = Arc::new(db);
        let state = test_state(db.clone());

        let app = build_app(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let yaml = r#"
name: ttl_run
storage:
  result_ttl: "2h"
steps:
  - id: s
    type: noop
output: "{s.output}"
"#;
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{addr}/pipelines"))
            .header("content-type", "text/plain")
            .body(yaml.to_string())
            .send()
            .await
            .expect("create pipeline");
        assert_eq!(resp.status(), 200);

        let resp = client
            .post(format!("http://{addr}/runs"))
            .json(&serde_json::json!({"pipeline": "ttl_run"}))
            .send()
            .await
            .expect("run pipeline");
        assert_eq!(resp.status(), 200);
        let run_body: Value = resp.json().await.unwrap();
        let task_id = run_body["task_id"].as_str().unwrap();
        let tid = TaskId(uuid::Uuid::parse_str(task_id).unwrap());

        let meta = db.load_task(&tid).expect("load task").expect("task exists");
        assert_eq!(meta.result_ttl_secs, 7200);

        handle.abort();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn wait_for_drain_returns_zero_when_idle() {
        let in_flight = AtomicUsize::new(0);
        let notify = tokio::sync::Notify::new();
        let remaining = wait_for_drain(
            &in_flight,
            &notify,
            std::time::Duration::from_millis(100),
        )
        .await;
        assert_eq!(remaining, 0);
    }

    #[tokio::test]
    async fn wait_for_drain_waits_for_completion() {
        let in_flight = Arc::new(AtomicUsize::new(1));
        let notify = Arc::new(tokio::sync::Notify::new());
        let c_in_flight = in_flight.clone();
        let c_notify = notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if c_in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
                c_notify.notify_waiters();
            }
        });
        let remaining = wait_for_drain(
            &in_flight,
            &notify,
            std::time::Duration::from_secs(5),
        )
        .await;
        assert_eq!(remaining, 0);
    }

    #[tokio::test]
    async fn wait_for_drain_times_out_with_remaining() {
        let in_flight = AtomicUsize::new(2);
        let notify = tokio::sync::Notify::new();
        let start = std::time::Instant::now();
        let remaining = wait_for_drain(
            &in_flight,
            &notify,
            std::time::Duration::from_millis(300),
        )
        .await;
        assert_eq!(remaining, 2);
        assert!(start.elapsed() >= std::time::Duration::from_millis(300));
        assert!(start.elapsed() < std::time::Duration::from_secs(3));
    }
}

