//! Trigger 运行时：worker 管理（cron 调度 / stream 微批缓冲）+ HTTP API。
//!
//! daemon 只接收 JSON 配置（PUT /triggers/:name）；TOML 等文件格式是 CLI 侧
//! 的本地实现细节。每个 trigger 一个后台 worker，触发 = 调用统一的
//! `submit_run` 提交路径，产生的微批全部是普通 task。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::{
    Json,
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::Value;

use weaveflow::error::WeaveflowError;
use weaveflow::trigger::{
    CronConfig, MisfirePolicy, StreamConfig, TriggerDef, TriggerRow, TriggerType, missed_fire,
    next_fire_after, validate_trigger,
};

use super::daemon::{AppState, submit_run};

// ── 事件 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct TriggerEvent {
    pub trigger: String,
    /// fired（产生 task）/ failed（提交失败）/ dropped（draining 或并发满丢弃）
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub at: String,
}

impl TriggerEvent {
    fn fired(trigger: &str, task_id: &weaveflow::tracker::TaskId) -> Self {
        Self {
            trigger: trigger.to_string(),
            kind: "fired",
            task_id: Some(task_id.to_string()),
            error: None,
            at: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn failed(trigger: &str, error: impl std::fmt::Display) -> Self {
        Self {
            trigger: trigger.to_string(),
            kind: "failed",
            task_id: None,
            error: Some(error.to_string()),
            at: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn dropped(trigger: &str, reason: impl std::fmt::Display) -> Self {
        Self {
            trigger: trigger.to_string(),
            kind: "dropped",
            task_id: None,
            error: Some(reason.to_string()),
            at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

// ── Worker 管理 ─────────────────────────────────────────────────────────

/// stream push 通道：元素批次 + 共享缓冲计数（背压用）。
#[derive(Clone)]
struct PushHandle {
    tx: tokio::sync::mpsc::UnboundedSender<Vec<Value>>,
    buffered: Arc<AtomicUsize>,
    cap: usize,
}

struct WorkerHandle {
    cancel: tokio::sync::watch::Sender<bool>,
    push: Option<PushHandle>,
}

/// Trigger worker 注册表 + 全局事件广播。
pub struct TriggerManager {
    workers: std::sync::Mutex<HashMap<String, WorkerHandle>>,
    events: tokio::sync::broadcast::Sender<Vec<u8>>,
}

impl TriggerManager {
    pub fn new() -> Self {
        let (events, _) = tokio::sync::broadcast::channel(64);
        Self {
            workers: std::sync::Mutex::new(HashMap::new()),
            events,
        }
    }

    fn emit(&self, ev: &TriggerEvent) {
        if let Ok(bytes) = serde_json::to_vec(ev) {
            let _ = self.events.send(bytes); // 无订阅者时静默丢弃
        }
    }

    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Vec<u8>> {
        self.events.subscribe()
    }

    /// 启动（或替换）一个 trigger 的后台 worker。
    pub fn start_worker(&self, state: &Arc<AppState>, def: &TriggerDef) {
        self.stop_worker(&def.name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let push = match def.trigger_type {
            TriggerType::Stream => {
                let cfg = def.stream.clone().unwrap_or_default();
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Vec<Value>>();
                let buffered = Arc::new(AtomicUsize::new(0));
                let handle = PushHandle {
                    tx,
                    buffered: buffered.clone(),
                    cap: cfg.buffer_cap,
                };
                tokio::spawn(stream_worker(
                    state.clone(),
                    def.clone(),
                    cfg,
                    rx,
                    buffered,
                    cancel_rx,
                ));
                Some(handle)
            }
            TriggerType::Cron => {
                let cfg = def.cron.clone().unwrap_or_default();
                tokio::spawn(cron_worker(state.clone(), def.clone(), cfg, cancel_rx));
                None
            }
        };
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(def.name.clone(), WorkerHandle { cancel: cancel_tx, push });
        tracing::info!(trigger = %def.name, r#type = %def.trigger_type, "trigger worker started");
    }

    /// 停止并移除 worker（幂等）。stream worker 收到取消后先 flush 剩余缓冲再退出。
    pub fn stop_worker(&self, name: &str) {
        if let Some(h) = self
            .workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name)
        {
            let _ = h.cancel.send(true);
        }
    }

    /// push 一批元素到 stream trigger 的缓冲。返回缓冲中的元素总数。
    pub fn push(&self, name: &str, items: Vec<Value>) -> Result<usize, PushError> {
        let workers = self.workers.lock().unwrap_or_else(|e| e.into_inner());
        let Some(h) = workers.get(name) else {
            return Err(PushError::NotFound);
        };
        let Some(push) = &h.push else {
            return Err(PushError::NotStream);
        };
        let n = items.len();
        let prev = push.buffered.fetch_add(n, Ordering::SeqCst);
        if prev + n > push.cap {
            push.buffered.fetch_sub(n, Ordering::SeqCst);
            return Err(PushError::Full {
                cap: push.cap,
                buffered: prev,
            });
        }
        if push.tx.send(items).is_err() {
            push.buffered.fetch_sub(n, Ordering::SeqCst);
            return Err(PushError::NotFound); // worker 已退出
        }
        Ok(prev + n)
    }
}

#[derive(Debug)]
pub enum PushError {
    NotFound,
    NotStream,
    Full { cap: usize, buffered: usize },
}

impl Default for TriggerManager {
    fn default() -> Self {
        Self::new()
    }
}

/// daemon 启动时从 redb 恢复全部 trigger worker。
pub fn start_all(state: &Arc<AppState>) {
    let rows = match state.db.list_triggers() {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "failed to load triggers on startup");
            return;
        }
    };
    for row in rows {
        state.trigger_mgr.start_worker(state, &row.def);
    }
}

// ── 提交与状态记录 ───────────────────────────────────────────────────────

async fn fire(
    state: &Arc<AppState>,
    name: &str,
    pipeline: &str,
    inputs: HashMap<String, Value>,
) {
    match submit_run(state, pipeline, inputs, &format!("trigger:{name}")).await {
        Ok(outcome) => {
            tracing::info!(
                trigger = %name,
                task_id = %outcome.task_id,
                "trigger fired"
            );
            if let Err(e) = state.db.update_trigger(name, |row| {
                row.record_fired(chrono::Utc::now(), &outcome.task_id);
            }) {
                tracing::warn!(trigger = %name, error = %e, "update_trigger(fired) failed");
            }
            state.trigger_mgr.emit(&TriggerEvent::fired(name, &outcome.task_id));
        }
        Err(WeaveflowError::Unavailable(_)) => {
            tracing::warn!(trigger = %name, "trigger dropped: daemon draining");
            state.trigger_mgr.emit(&TriggerEvent::dropped(name, "daemon draining"));
        }
        Err(e) => {
            tracing::error!(trigger = %name, error = %e, "trigger submit failed");
            if let Err(db_err) = state.db.update_trigger(name, |row| row.record_failed()) {
                tracing::warn!(trigger = %name, error = %db_err, "update_trigger(failed) failed");
            }
            state.trigger_mgr.emit(&TriggerEvent::failed(name, e));
        }
    }
}

// ── cron worker ─────────────────────────────────────────────────────────

async fn cron_worker(
    state: Arc<AppState>,
    def: TriggerDef,
    cfg: CronConfig,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) {
    let name = def.name.clone();
    // 启动时 misfire 处理：停机期间错过的触发点按策略补一次或跳过。
    if let Ok(Some(row)) = state.db.load_trigger(&name)
        && missed_fire(&row, chrono::Utc::now()).is_some()
    {
        match cfg.misfire {
            MisfirePolicy::CatchUp => {
                tracing::info!(trigger = %name, "cron misfire catch-up: firing once");
                fire(&state, &name, &def.pipeline, cfg.inputs.clone()).await;
            }
            MisfirePolicy::Skip => {
                tracing::info!(trigger = %name, "cron misfire skipped");
            }
        }
    }
    loop {
        let base = match state.db.load_trigger(&name) {
            Ok(Some(row)) => row.last_fired_at.unwrap_or(row.created_at),
            _ => {
                // trigger 已被删除但 worker 尚未收到 cancel —— 直接退出
                tracing::warn!(trigger = %name, "cron worker: row missing, exiting");
                return;
            }
        };
        let now = chrono::Utc::now();
        let Some(next) = next_fire_after(&cfg, base, now) else {
            tracing::error!(trigger = %name, "cron worker: cannot compute next fire, exiting");
            return;
        };
        if let Err(e) = state.db.update_trigger(&name, |row| {
            row.next_fire_at = Some(next);
        }) {
            tracing::warn!(trigger = %name, error = %e, "update_trigger(next_fire) failed");
        }
        let wait = (next - now).to_std().unwrap_or(std::time::Duration::ZERO);
        tokio::select! {
            _ = cancel.changed() => {
                tracing::info!(trigger = %name, "cron worker stopped");
                return;
            }
            _ = tokio::time::sleep(wait) => {
                fire(&state, &name, &def.pipeline, cfg.inputs.clone()).await;
            }
        }
    }
}

// ── stream worker ───────────────────────────────────────────────────────

async fn stream_worker(
    state: Arc<AppState>,
    def: TriggerDef,
    cfg: StreamConfig,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Vec<Value>>,
    buffered: Arc<AtomicUsize>,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) {
    let name = def.name.clone();
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.max_in_flight.max(1) as usize));
    let flush_dur = cfg
        .flush_interval_duration()
        .unwrap_or_else(|e| {
            tracing::warn!(trigger = %name, error = %e, "invalid flush_interval, disabled");
            None
        });
    let mut buf: Vec<Value> = Vec::new();

    // flush 一个批次：提交 run + 更新状态 + 发事件 + 终态后释放并发 permit
    async fn flush_batch(
        state: &Arc<AppState>,
        sem: &Arc<tokio::sync::Semaphore>,
        name: &str,
        pipeline: &str,
        slot: &str,
        batch: Vec<Value>,
        buffered: &Arc<AtomicUsize>,
    ) {
        buffered.fetch_sub(batch.len(), Ordering::SeqCst);
        let mut inputs = HashMap::new();
        inputs.insert(slot.to_string(), Value::Array(batch));
        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => return,
        };
        // 提交成功后，permit 由后台轮询在 task 到达终态时释放。
        let db = state.db.clone();
        match submit_run(state, pipeline, inputs, &format!("trigger:{name}")).await {
            Ok(outcome) => {
                tracing::info!(
                    trigger = %name,
                    task_id = %outcome.task_id,
                    "stream batch fired"
                );
                if let Err(e) = state.db.update_trigger(name, |row| {
                    row.record_fired(chrono::Utc::now(), &outcome.task_id);
                }) {
                    tracing::warn!(trigger = %name, error = %e, "update_trigger(fired) failed");
                }
                state.trigger_mgr.emit(&TriggerEvent::fired(name, &outcome.task_id));
                tokio::spawn(async move {
                    let _permit = permit;
                    wait_task_terminal(&db, &outcome.task_id).await;
                });
            }
            Err(WeaveflowError::Unavailable(_)) => {
                tracing::warn!(trigger = %name, "stream batch dropped: daemon draining");
                state.trigger_mgr.emit(&TriggerEvent::dropped(name, "daemon draining"));
                drop(permit);
            }
            Err(e) => {
                tracing::error!(trigger = %name, error = %e, "stream batch submit failed");
                if let Err(db_err) = state.db.update_trigger(name, |row| row.record_failed())
                {
                    tracing::warn!(trigger = %name, error = %db_err, "update_trigger(failed) failed");
                }
                state.trigger_mgr.emit(&TriggerEvent::failed(name, e));
                drop(permit);
            }
        }
    }

    // flush_interval 缺省时 timer 为 None，对应 select 分支永不就绪。
    let mut flush_timer: Option<tokio::time::Interval> = flush_dur
        .map(|d| tokio::time::interval_at(tokio::time::Instant::now() + d, d));

    loop {
        tokio::select! {
            _ = cancel.changed() => {
                // 关闭语义：尽力把剩余缓冲全部 flush 后退出
                while !buf.is_empty() {
                    let take = buf.len().min(cfg.batch_size);
                    let batch: Vec<Value> = buf.drain(..take).collect();
                    flush_batch(&state, &sem, &name, &def.pipeline, &cfg.slot, batch, &buffered).await;
                }
                tracing::info!(trigger = %name, "stream worker stopped");
                return;
            }
            Some(items) = rx.recv() => {
                buf.extend(items);
                while buf.len() >= cfg.batch_size {
                    let batch: Vec<Value> = buf.drain(..cfg.batch_size).collect();
                    flush_batch(&state, &sem, &name, &def.pipeline, &cfg.slot, batch, &buffered).await;
                }
            }
            _ = async {
                match &mut flush_timer {
                    Some(t) => { t.tick().await; }
                    None => std::future::pending::<()>().await,
                }
            }, if !buf.is_empty() => {
                let take = buf.len().min(cfg.batch_size);
                let batch: Vec<Value> = buf.drain(..take).collect();
                flush_batch(&state, &sem, &name, &def.pipeline, &cfg.slot, batch, &buffered).await;
            }
        }
    }
}

/// 轮询 task 状态直到终态（用于释放 trigger 级并发 permit）。
async fn wait_task_terminal(db: &Arc<weaveflow::store::Database>, task_id: &weaveflow::tracker::TaskId) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        match db.load_task(task_id) {
            Ok(Some(meta))
                if meta.status == weaveflow::tracker::meta::TASK_STATUS_RUNNING => {}
            _ => return, // 终态、行消失（prune）或 DB 错误都视为可释放
        }
    }
}

// ── HTTP handlers ───────────────────────────────────────────────────────

pub async fn upsert_trigger(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(def): Json<TriggerDef>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!(trigger = %name, "PUT /triggers/:name");
    if def.name != name {
        return Err(WeaveflowError::BadRequest(format!(
            "body 中的 name ({}) 与路径 ({name}) 不一致",
            def.name
        )));
    }
    let errors = validate_trigger(&def);
    if !errors.is_empty() {
        tracing::warn!(trigger = %name, errors = %errors.join("; "), "trigger validation failed");
        return Err(WeaveflowError::Validation(errors.join("; ")));
    }
    if state.db.find_pipeline_by_name(&def.pipeline)?.is_none() {
        return Err(WeaveflowError::BadRequest(format!(
            "pipeline {} not found（trigger 引用已注册的 pipeline，请先 apply）",
            def.pipeline
        )));
    }

    let existed = state.db.load_trigger(&name)?;
    let (row, status) = match existed {
        Some(mut old) => {
            old.def = def.clone();
            (old, "updated")
        }
        None => (TriggerRow::new(def.clone()), "created"),
    };
    state.db.save_trigger(&row)?;
    state.trigger_mgr.start_worker(&state, &def);
    tracing::info!(trigger = %name, status, "trigger saved");
    Ok(Json(serde_json::json!({"name": name, "status": status})))
}

pub async fn list_triggers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!("GET /triggers");
    let rows = state.db.list_triggers()?;
    let list: Vec<Value> = rows.iter().map(row_summary).collect();
    Ok(Json(serde_json::json!(list)))
}

pub async fn get_trigger(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!(trigger = %name, "GET /triggers/:name");
    let row = state
        .db
        .load_trigger(&name)?
        .ok_or_else(|| WeaveflowError::NotFound(format!("trigger {name} not found")))?;
    let mut v = serde_json::to_value(&row).map_err(|e| WeaveflowError::Internal(e.to_string()))?;
    if let Some(obj) = v.as_object_mut() {
        obj.insert("buffered".into(), serde_json::json!(buffered_of(&state, &name)));
    }
    Ok(Json(v))
}

pub async fn delete_trigger(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!(trigger = %name, "DELETE /triggers/:name");
    if !state.db.delete_trigger(&name)? {
        return Err(WeaveflowError::NotFound(format!("trigger {name} not found")));
    }
    state.trigger_mgr.stop_worker(&name);
    tracing::info!(trigger = %name, "trigger deleted");
    Ok(Json(serde_json::json!({"deleted": name})))
}

fn buffered_of(state: &Arc<AppState>, name: &str) -> usize {
    let workers = state
        .trigger_mgr
        .workers
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    workers
        .get(name)
        .and_then(|h| h.push.as_ref())
        .map(|p| p.buffered.load(Ordering::SeqCst))
        .unwrap_or(0)
}

fn row_summary(row: &TriggerRow) -> Value {
    serde_json::json!({
        "name": row.def.name,
        "pipeline": row.def.pipeline,
        "type": row.def.trigger_type.to_string(),
        "created_at": row.created_at.to_rfc3339(),
        "last_fired_at": row.last_fired_at.map(|t| t.to_rfc3339()),
        "next_fire_at": row.next_fire_at.map(|t| t.to_rfc3339()),
        "total_fired": row.total_fired,
        "total_failed": row.total_failed,
    })
}

pub async fn push_trigger(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    body: String,
) -> Result<Response, WeaveflowError> {
    let value: Value = serde_json::from_str(&body)
        .map_err(|e| WeaveflowError::BadRequest(format!("push body 必须是 JSON: {e}")))?;
    // 单对象/标量自动包一层数组
    let items = match value {
        Value::Array(a) => a,
        other => vec![other],
    };
    let n = items.len();
    tracing::info!(trigger = %name, items = n, "POST /triggers/:name/push");
    match state.trigger_mgr.push(&name, items) {
        Ok(buffered) => Ok(Json(serde_json::json!({
            "accepted": n,
            "buffered": buffered,
        }))
        .into_response()),
        Err(PushError::NotFound) => Err(WeaveflowError::NotFound(format!(
            "trigger {name} not found（或 worker 未运行）"
        ))),
        Err(PushError::NotStream) => Err(WeaveflowError::BadRequest(format!(
            "trigger {name} 不是 stream 类型，不能 push"
        ))),
        Err(PushError::Full { cap, buffered }) => Ok((
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "error": format!("buffer full: {buffered}/{cap} 元素在缓冲中，稍后再试"),
                "cap": cap,
                "buffered": buffered,
            })),
        )
            .into_response()),
    }
}

// ── WebSocket 事件流 ────────────────────────────────────────────────────

pub async fn ws_trigger(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, WeaveflowError> {
    tracing::info!(trigger = %name, "GET /triggers/:name/ws");
    if state.db.load_trigger(&name)?.is_none() {
        return Err(WeaveflowError::NotFound(format!("trigger {name} not found")));
    }
    let rx = state.trigger_mgr.subscribe_events();
    Ok(ws.on_upgrade(move |socket| handle_trigger_ws(socket, rx, name)))
}

async fn handle_trigger_ws(
    mut socket: WebSocket,
    mut rx: tokio::sync::broadcast::Receiver<Vec<u8>>,
    name: String,
) {
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
                        // 全局事件流按 trigger 名过滤
                        let v: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                        if v.get("trigger").and_then(|t| t.as_str()) != Some(name.as_str()) {
                            continue;
                        }
                        if socket
                            .send(Message::Text(String::from_utf8_lossy(&bytes).into()))
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
    }
}

// ── 测试 ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::daemon;
    use crate::server::logging::RingWriter;
    use std::sync::atomic::AtomicBool;
    use weaveflow::store::Database;
    use weaveflow::tracker::TaskTracker;

    fn temp_db() -> Database {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "weaveflow-trigger-test-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok();
        Database::open(dir.join("weaveflow.redb")).expect("open db")
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
            trigger_mgr: Arc::new(TriggerManager::new()),
        })
    }

    fn register_test_pipeline(db: &Database) {
        let yaml = r#"
name: trig_pipe
steps:
  - id: s
    type: noop
output: "{s.output}"
"#;
        let def = weaveflow::dsl::parser::parse(yaml).expect("parse");
        db.save_pipeline_upsert(&def).expect("save pipeline");
    }

    fn stream_def(name: &str, batch_size: usize) -> TriggerDef {
        TriggerDef {
            name: name.into(),
            pipeline: "trig_pipe".into(),
            trigger_type: TriggerType::Stream,
            stream: Some(StreamConfig {
                batch_size,
                flush_interval: None,
                max_in_flight: 4,
                slot: "items".into(),
                buffer_cap: 1000,
            }),
            cron: None,
        }
    }

    async fn serve_app(state: Arc<AppState>) -> (String, tokio::task::JoinHandle<()>) {
        let app = daemon::build_app(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn trigger_crud_roundtrip() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state).await;
        let client = reqwest::Client::new();

        // PUT 创建
        let resp = client
            .put(format!("{base}/triggers/s1"))
            .json(&stream_def("s1", 10))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "created");

        // PUT 同名更新
        let resp = client
            .put(format!("{base}/triggers/s1"))
            .json(&stream_def("s1", 20))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "updated");

        // GET 列表 + 详情
        let resp = client.get(format!("{base}/triggers")).send().await.unwrap();
        let list: Value = resp.json().await.unwrap();
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["name"], "s1");

        let resp = client
            .get(format!("{base}/triggers/s1"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let detail: Value = resp.json().await.unwrap();
        assert_eq!(detail["def"]["stream"]["batch_size"], 20);

        // 校验失败：batch_size = 0
        let mut bad = stream_def("bad", 0);
        bad.name = "bad".into();
        let resp = client
            .put(format!("{base}/triggers/bad"))
            .json(&bad)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // pipeline 不存在
        let mut orphan = stream_def("orphan", 10);
        orphan.pipeline = "no_such".into();
        let resp = client
            .put(format!("{base}/triggers/orphan"))
            .json(&orphan)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // name 不一致
        let resp = client
            .put(format!("{base}/triggers/other"))
            .json(&stream_def("s1", 10))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // DELETE
        let resp = client
            .delete(format!("{base}/triggers/s1"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .delete(format!("{base}/triggers/s1"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);

        handle.abort();
    }

    #[tokio::test]
    async fn stream_push_batches_into_tasks() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state.clone()).await;
        let client = reqwest::Client::new();

        let resp = client
            .put(format!("{base}/triggers/s1"))
            .json(&stream_def("s1", 3))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // push 5 条（数组 3 + 单对象 1 + 数组 1）→ 一批 3 条触发，余 2 条缓冲
        let resp = client
            .post(format!("{base}/triggers/s1/push"))
            .json(&serde_json::json!([1, 2, 3]))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/triggers/s1/push"))
            .json(&serde_json::json!({"x": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/triggers/s1/push"))
            .json(&serde_json::json!([5]))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["accepted"], 1);

        // 等 worker flush
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let row = db.load_trigger("s1").unwrap().unwrap();
        assert_eq!(row.total_fired, 1, "row: {row:?}");
        assert_eq!(row.recent_tasks.len(), 1);

        // task 的 inputs 应是 {items: [1,2,3]}，且来源为 trigger
        let tid = weaveflow::tracker::TaskId(
            uuid::Uuid::parse_str(&row.recent_tasks[0]).unwrap(),
        );
        let meta = db.load_task(&tid).unwrap().unwrap();
        assert_eq!(meta.inputs, serde_json::json!({"items": [1, 2, 3]}));
        assert_eq!(meta.trigger_source.as_deref(), Some("trigger:s1"));

        // 剩余 2 条仍在缓冲
        let resp = client
            .get(format!("{base}/triggers/s1"))
            .send()
            .await
            .unwrap();
        let detail: Value = resp.json().await.unwrap();
        assert_eq!(detail["buffered"], 2);

        // cron trigger 不接受 push
        let cron_def = TriggerDef {
            name: "c1".into(),
            pipeline: "trig_pipe".into(),
            trigger_type: TriggerType::Cron,
            stream: None,
            cron: Some(CronConfig {
                schedule: None,
                interval: Some("1h".into()),
                misfire: MisfirePolicy::Skip,
                inputs: HashMap::new(),
            }),
        };
        client
            .put(format!("{base}/triggers/c1"))
            .json(&cron_def)
            .send()
            .await
            .unwrap();
        let resp = client
            .post(format!("{base}/triggers/c1/push"))
            .json(&serde_json::json!([1]))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        handle.abort();
    }

    #[tokio::test]
    async fn stream_buffer_cap_returns_429() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db);
        let (base, handle) = serve_app(state).await;
        let client = reqwest::Client::new();

        let mut def = stream_def("cap", 5); // batch_size=5：push 4 条不 flush
        def.stream.as_mut().unwrap().buffer_cap = 5;
        client
            .put(format!("{base}/triggers/cap"))
            .json(&def)
            .send()
            .await
            .unwrap();

        let resp = client
            .post(format!("{base}/triggers/cap/push"))
            .json(&serde_json::json!([1, 2, 3, 4]))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/triggers/cap/push"))
            .json(&serde_json::json!([5, 6]))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 429);

        handle.abort();
    }

    #[tokio::test]
    async fn cron_interval_fires() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state).await;
        let client = reqwest::Client::new();

        let def = TriggerDef {
            name: "tick".into(),
            pipeline: "trig_pipe".into(),
            trigger_type: TriggerType::Cron,
            stream: None,
            cron: Some(CronConfig {
                schedule: None,
                interval: Some("1s".into()),
                misfire: MisfirePolicy::Skip,
                inputs: HashMap::from([("k".into(), serde_json::json!("v"))]),
            }),
        };
        let resp = client
            .put(format!("{base}/triggers/tick"))
            .json(&def)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // 等 ~2.5s 应至少触发 1 次
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        let row = db.load_trigger("tick").unwrap().unwrap();
        assert!(row.total_fired >= 1, "row: {row:?}");
        assert!(row.next_fire_at.is_some());

        handle.abort();
    }

    #[tokio::test]
    async fn delete_stops_stream_worker() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state.clone()).await;
        let client = reqwest::Client::new();

        client
            .put(format!("{base}/triggers/s1"))
            .json(&stream_def("s1", 2))
            .send()
            .await
            .unwrap();
        // push 3 条（应触发 1 批，余 1 条），删除后 worker flush 剩余
        client
            .post(format!("{base}/triggers/s1/push"))
            .json(&serde_json::json!([1, 2, 3]))
            .send()
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        client
            .delete(format!("{base}/triggers/s1"))
            .send()
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // worker 关闭语义：剩余 1 条也被 flush → 共 2 次触发
        assert!(state.db.load_trigger("s1").unwrap().is_none());
        // 无法直接查 total_fired（行已删），用 task 列表验证
        let tasks = state.db.list_tasks().unwrap();
        let trigger_tasks: Vec<_> = tasks
            .iter()
            .filter(|t| t.trigger_source.as_deref() == Some("trigger:s1"))
            .collect();
        assert_eq!(trigger_tasks.len(), 2, "tasks: {tasks:?}");

        handle.abort();
    }
}
