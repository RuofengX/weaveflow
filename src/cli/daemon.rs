use std::process::Stdio;
use std::sync::Arc;

use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::Mutex;

use weave::dsl::parser::parse;
use weave::dsl::validator::{validate, ValidateOptions};
use weave::error::WeaveError;
use weave::runtime::dag::Dag;
use weave::runtime::Executor;
use weave::store::{Database, PruneOptions};
use weave::task::{LayerInfo, TaskId, TaskTracker};

type DbRef = Arc<Mutex<Database>>;
type TrackerRef = Arc<TaskTracker>;

// ── State ────────────────────────────────────────────────────────────────

struct AppState {
    db: DbRef,
    tracker: TrackerRef,
    semaphore: Arc<tokio::sync::Semaphore>,
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
    step_id: String,
}

#[derive(Serialize)]
struct SnapshotResponse {
    seq: u64,
    step_id: String,
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
    objects_removed: u64,
    bytes_freed: u64,
    dry_run: bool,
}

// ── Handlers ──────────────────────────────────────────────────────────────

async fn create_pipeline(
    State(state): State<Arc<AppState>>,
    body: String,
) -> Result<Json<Value>, WeaveError> {
    let pipeline = parse(&body).map_err(|e| WeaveError::Parse(e.to_string()))?;
    let report = validate(&pipeline, &ValidateOptions::default());
    if !report.is_ok() {
        let msgs: Vec<String> = report
            .errors
            .iter()
            .map(|e| format!("[{}] {}", e.code, e.message))
            .collect();
        return Err(WeaveError::Validation(msgs.join("; ")));
    }

    let builtins = weave::operator::builtins();
    for step in &pipeline.steps {
        if step.r#type != "js" && !builtins.contains_key(&step.r#type) {
            return Err(WeaveError::Validation(format!(
                "未注册的步骤类型: {}（步骤: {}）",
                step.r#type, step.id
            )));
        }
    }

    let pid = state.db.lock().await.save_pipeline_upsert(&pipeline)?;
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
    let db = state.db.lock().await;
    let (pid, _) = db
        .find_pipeline_by_name(&name)?
        .ok_or_else(|| WeaveError::NotFound(format!("pipeline {name} not found")))?;
    db.delete_pipeline(&pid)?;
    Ok(Json(serde_json::json!({"deleted": name})))
}

async fn list_pipelines(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, WeaveError> {
    let items = state.db.lock().await.list_pipelines()?;
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
    let db = state.db.lock().await;
    let pipeline = if let Some((_, p)) = db.find_pipeline_by_name(&name_or_id)? {
        p
    } else if let Ok(uuid) = uuid::Uuid::parse_str(&name_or_id) {
        let pid = weave::task::PipelineId(uuid);
        db.load_pipeline(&pid)?
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
    // Load pipeline
    let pipeline = {
        let db = state.db.lock().await;
        match db.find_pipeline_by_name(&req.pipeline)? {
            Some((_pid, p)) => p,
            None => {
                return Err(WeaveError::NotFound(format!(
                    "pipeline {} not found",
                    req.pipeline
                )))
            }
        }
    };

    let inputs = req.inputs.unwrap_or_default();

    // Build DAG layers for progress display
    let dag = Dag::from_pipeline(&pipeline)
        .map_err(|e| WeaveError::Internal(format!("DAG build: {e}")))?;
    let layers = dag
        .topological_sort()
        .map_err(|e| WeaveError::Internal(format!("topo sort: {e}")))?;
    let all_step_ids: Vec<String> = layers.iter().flatten().cloned().collect();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo {
            index: i,
            step_ids: step_ids.clone(),
        })
        .collect();

    // Create task in DB
    let task_id = {
        let db = state.db.lock().await;
        db.create_task(&pipeline.name, serde_json::json!(inputs), 3600)?
    };

    // Register with tracker
    let (_rx, snapshot) = state
        .tracker
        .create(
            task_id,
            pipeline.name.to_string(),
            all_step_ids,
            layer_infos,
        )
        .await;

    // Acquire a concurrency permit. This blocks the HTTP handler until a slot is free,
    // providing natural backpressure when the concurrent task limit is reached.
    let permit = state
        .semaphore
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| WeaveError::Internal("task semaphore closed".into()))?;

    // Spawn executor in background
    let state_clone = state.clone();
    let pipeline_clone = pipeline.clone();
    tokio::spawn(async move {
        let _permit = permit; // hold the permit until this task finishes
        let executor = Executor::new(
            pipeline_clone,
            state_clone.db.clone(),
            state_clone.tracker.clone(),
        );

        let _ = executor.run(task_id, inputs, 3600).await;
    });

    let response = serde_json::json!({
        "task_id": task_id.to_string(),
        "pipeline_name": pipeline.name,
        "status": snapshot.status,
        "layers": snapshot.layers,
    });
    Ok(Json(response))
}

async fn list_tasks(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, WeaveError> {
    let tasks = state.db.lock().await.list_tasks()?;
    let list: Vec<Value> = tasks
        .iter()
        .map(|t| {
            serde_json::json!({
                "task_id": t.task_id.to_string(),
                "pipeline_name": t.pipeline_name,
                "created_at": t.created_at.to_rfc3339(),
            })
        })
        .collect();
    Ok(Json(serde_json::json!(list)))
}

async fn get_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Result<Json<TaskResponse>, WeaveError> {
    let tid = parse_task_id(&task_id)?;
    let (task, snapshot_count) = {
        let db = state.db.lock().await;
        let task = match db.load_task(&tid)? {
            Some(t) => t,
            None => {
                return Err(WeaveError::NotFound(format!(
                    "task {task_id} not found"
                )));
            }
        };
        let snaps = db.load_snapshots(&tid)?;
        (task, snaps.len() as u64)
    };

    // Check tracker for live progress
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
    let tid = parse_task_id(&task_id)?;
    let db = state.db.lock().await;
    let snapshots = db.load_snapshots(&tid)?;
    let snaps: Vec<SnapshotMeta> = snapshots
        .iter()
        .map(|(seq, snap)| SnapshotMeta {
            seq: *seq,
            step_id: snap.step_id.clone(),
        })
        .collect();
    Ok(Json(snaps))
}

async fn get_snapshot_by_seq(
    State(state): State<Arc<AppState>>,
    Path((task_id, seq)): Path<(String, u64)>,
) -> Result<Json<SnapshotResponse>, WeaveError> {
    let tid = parse_task_id(&task_id)?;
    let db = state.db.lock().await;
    let snapshots = db.load_snapshots(&tid)?;
    match snapshots.into_iter().find(|(s, _)| *s == seq) {
        Some((_, snap)) => {
            let output: Value = match serde_json::from_slice::<Value>(&snap.output) {
                Ok(v) => {
                    if v.is_null() && !snap.output.is_empty() {
                        // The stored output is literally the JSON `null` token.
                        // This is likely an anomaly — the step may have failed to produce
                        // a meaningful output but the error was not properly propagated.
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
    let options = PruneOptions {
        pipeline: req.pipeline,
        force: req.force.unwrap_or(false),
        dry_run: req.dry_run.unwrap_or(false),
    };
    let mut db = state.db.lock().await;
    let report = db.prune(&options)?;
    Ok(Json(PruneResponse {
        tasks_removed: report.tasks_removed,
        objects_removed: report.objects_removed,
        bytes_freed: report.bytes_freed,
        dry_run: options.dry_run,
    }))
}

async fn list_operators(
) -> Result<Json<Value>, WeaveError> {
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

// ── WebSocket ─────────────────────────────────────────────────────────────

async fn ws_task(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<axum::response::Response, WeaveError> {
    let tid = parse_task_id(&task_id)?;

    // Get current snapshot to send as initial state
    let initial = state.tracker.get(&tid).await;

    let rx = match state.tracker.subscribe(&tid).await {
        Some(rx) => rx,
        None => {
            return Err(WeaveError::NotFound(format!(
                "task {task_id} not running"
            )));
        }
    };

    Ok(ws.on_upgrade(move |socket| handle_ws(socket, rx, initial)))
}

async fn handle_ws(
    mut socket: WebSocket,
    mut rx: tokio::sync::broadcast::Receiver<Vec<u8>>,
    initial: Option<weave::task::TaskSnapshot>,
) {
    // Send current snapshot first so the client sees the latest state immediately,
    // even if the task completed before the WS connection was established.
    if let Some(ref snap) = initial {
        let bytes = serde_json::to_vec(snap).unwrap_or_default();
        if socket
            .send(Message::Text(String::from_utf8_lossy(&bytes).into()))
            .await
            .is_err()
        {
            return;
        }
    }

    loop {
        match rx.recv().await {
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
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
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
        .with_state(state)
}

// ── Serve ─────────────────────────────────────────────────────────────────

pub async fn serve(args: Vec<String>) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let bind = args
        .iter()
        .position(|a| a == "--bind")
        .and_then(|i| args.get(i + 1).cloned())
        .unwrap_or_else(|| "127.0.0.1:9928".to_string());

    let max_concurrent = args
        .iter()
        .position(|a| a == "--max-concurrent-tasks")
        .and_then(|i| args.get(i + 1).and_then(|s| s.parse::<usize>().ok()))
        .or_else(|| std::env::var("WEAVE_MAX_CONCURRENT_TASKS").ok().and_then(|s| s.parse().ok()));

    let semaphore_permits = max_concurrent.unwrap_or(usize::MAX >> 3).max(1);

    let data_dir = resolve_data_dir(&args);

    let db = Database::open(data_dir.join("weave.redb")).expect("open database");
    let state = Arc::new(AppState {
        db: Arc::new(Mutex::new(db)),
        tracker: Arc::new(TaskTracker::new()),
        semaphore: Arc::new(tokio::sync::Semaphore::new(semaphore_permits)),
    });

    let app = build_app(state);
    tracing::info!("weave serve listening on {bind}");
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .expect("bind");
    axum::serve(listener, app).await.expect("serve");
}

fn resolve_data_dir(args: &[String]) -> std::path::PathBuf {
    for i in 0..args.len().saturating_sub(1) {
        if args[i] == "--data-dir" || args[i] == "-d" {
            return std::path::PathBuf::from(&args[i + 1]);
        }
    }
    if let Ok(dir) = std::env::var("WEAVE_DATA") {
        return std::path::PathBuf::from(dir);
    }
    dirs_next::home_dir()
        .map(|h| h.join(".weave"))
        .unwrap_or_else(|| std::path::PathBuf::from(".weave"))
}

// ── Daemon ────────────────────────────────────────────────────────────────

fn pid_path() -> std::path::PathBuf {
    dirs_next::home_dir()
        .map(|h| h.join(".weave").join("weave.pid"))
        .unwrap_or_else(|| std::path::PathBuf::from(".weave/weave.pid"))
}

fn is_daemon_running() -> bool {
    let path = pid_path();
    let pid_str = match std::fs::read_to_string(&path) {
        Ok(s) => s.trim().to_string(),
        Err(_) => return false,
    };
    let pid: i32 = match pid_str.parse() {
        Ok(p) => p,
        Err(_) => {
            let _ = std::fs::remove_file(&path);
            return false;
        }
    };
    let alive = unsafe { libc::kill(pid, 0) == 0 };
    if !alive {
        let _ = std::fs::remove_file(&path);
    }
    alive
}

pub async fn start(bind: &str, max_concurrent_tasks: Option<usize>) {
    if is_daemon_running() {
        eprintln!(
            "daemon is already running. Use `weave daemon restart` to restart."
        );
        return;
    }
    let exe = std::env::current_exe().expect("current exe path");
    let mut cmd = tokio::process::Command::new(&exe);
    cmd.arg("serve")
        .arg("--bind")
        .arg(bind)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(n) = max_concurrent_tasks {
        cmd.arg("--max-concurrent-tasks").arg(n.to_string());
    }
    let child = cmd.spawn().expect("spawn daemon");

    let pid = child.id().expect("child PID");
    let path = pid_path();
    std::fs::create_dir_all(path.parent().unwrap()).ok();
    std::fs::write(&path, pid.to_string()).expect("write PID file");
    println!("daemon started (PID {pid}) on {bind}");
    std::mem::forget(child);
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
    let pid: u32 = pid_str.parse().expect("parse PID");
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }
    std::fs::remove_file(&path).ok();
    println!("daemon stopped (PID {pid})");
}

pub async fn restart(bind: &str, max_concurrent_tasks: Option<usize>) {
    if is_daemon_running() {
        stop().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    start(bind, max_concurrent_tasks).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_db() -> (Database, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "weave-server-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok();
        let db = Database::open(dir.join("weave.redb")).expect("open db");
        (db, dir)
    }

    #[tokio::test]
    async fn smoke_pipeline_crud() {
        let (db, _dir) = temp_db();
        let state = Arc::new(AppState {
            db: Arc::new(tokio::sync::Mutex::new(db)),
            tracker: Arc::new(TaskTracker::new()),
            semaphore: Arc::new(tokio::sync::Semaphore::new(usize::MAX >> 3)),
        });

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
}
