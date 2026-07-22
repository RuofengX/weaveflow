//! Routine 运行时：worker 管理（cron 调度 / stream 微批缓冲）+ 事件收件箱 + HTTP API。
//!
//! daemon 只接收 JSON 配置（PUT /routines/:name）；TOML 等文件格式是 CLI 侧
//! 的本地实现细节。每个 routine 一个后台 worker，触发 = 调用统一的
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
use serde_json::Value;

use weaveflow::error::WeaveflowError;
use weaveflow::routine::{
    CronConfig, MisfirePolicy, RoutineDef, RoutineEventRecord, RoutineRow, RoutineType,
    StreamConfig, missed_fire, next_fire_after, validate_routine,
};

use super::daemon::{AppState, submit_run};

// ── 事件：持久化收件箱 + 内存广播 ───────────────────────────────────────

fn new_event(routine: &str, kind: &str) -> RoutineEventRecord {
    RoutineEventRecord {
        seq: 0, // 由 append_routine_event 分配
        routine: routine.to_string(),
        kind: kind.to_string(),
        task_id: None,
        error: None,
        output_preview: None,
        at: chrono::Utc::now(),
    }
}

fn fired_event(routine: &str, task_id: &weaveflow::tracker::TaskId) -> RoutineEventRecord {
    let mut e = new_event(routine, "fired");
    e.task_id = Some(task_id.to_string());
    e
}

fn failed_event(routine: &str, error: impl std::fmt::Display) -> RoutineEventRecord {
    let mut e = new_event(routine, "failed");
    e.error = Some(error.to_string());
    e
}

fn dropped_event(routine: &str, reason: impl std::fmt::Display) -> RoutineEventRecord {
    let mut e = new_event(routine, "dropped");
    e.error = Some(reason.to_string());
    e
}

/// 持久化到收件箱（可靠通道：智能体下次会话按 seq 增量回查），
/// 同时广播给 WS 实时订阅者（尽力通道）。
fn emit(state: &Arc<AppState>, mut rec: RoutineEventRecord) {
    match state.db.append_routine_event(rec.clone()) {
        Ok(Some(seq)) => rec.seq = seq,
        Ok(None) => {
            tracing::debug!(routine = %rec.routine, kind = %rec.kind, "routine 已删除，事件仅广播不入箱");
        }
        Err(e) => {
            tracing::warn!(routine = %rec.routine, error = %e, "routine event persist failed")
        }
    }
    state.routine_mgr.emit(&rec);
}

// ── 任务终态钩子（submit_run 的统一收口点调用） ─────────────────────────

/// routine 来源的 task 到达终态时：写入 task_completed / task_failed 事件
///（附 output_preview），并按 notify 配置异步投递 webhook。
pub async fn on_task_terminal(state: &Arc<AppState>, task_id: &weaveflow::tracker::TaskId) {
    let meta = match state.db.load_task(task_id) {
        Ok(Some(m)) => m,
        _ => return,
    };
    let Some(name) = meta
        .routine_source
        .as_deref()
        .and_then(|s| s.strip_prefix("routine:"))
        .map(str::to_string)
    else {
        return; // 非 routine 来源（manual 等）
    };
    let completed = meta.status == weaveflow::tracker::meta::TASK_STATUS_COMPLETED;

    // 任务刚结束，tracker 快照必在内存中（10 分钟后才被 cleanup_stale 回收）
    let snap = state.tracker.get(task_id).await;
    let (mut error, output) = match snap.as_ref().map(|s| &s.status) {
        Some(weaveflow::tracker::TaskStatus::Completed(v)) => (None, Some(v.clone())),
        Some(weaveflow::tracker::TaskStatus::Failed(e)) => (Some(e.clone()), None),
        _ => (None, None),
    };
    if !completed && error.is_none() {
        error = Some(format!("task terminal status: {}", meta.status));
    }

    let row = state.db.load_routine(&name).ok().flatten();
    let notify = row.as_ref().and_then(|r| r.def.notify.clone());
    let preview_bytes = notify
        .as_ref()
        .map(|n| n.preview_bytes as usize)
        .unwrap_or(2048);

    let mut rec = new_event(
        &name,
        if completed {
            "task_completed"
        } else {
            "task_failed"
        },
    );
    rec.task_id = Some(task_id.to_string());
    rec.error = error;
    rec.output_preview = output.map(|v| preview_value(&v, preview_bytes));
    emit(state, rec.clone());

    // webhook 只投递终态事件；投递失败落 notify_failed 事件（不再递归投递）
    if let Some(url) = notify.and_then(|n| n.webhook_url) {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = deliver_webhook(&url, &rec).await {
                tracing::warn!(routine = %name, error = %e, "webhook delivery failed");
                let mut rec2 = new_event(&name, "notify_failed");
                rec2.task_id = rec.task_id.clone();
                rec2.error = Some(e);
                emit(&state, rec2);
            }
        });
    }
}

/// 截断为预览：紧凑序列化超出 max_bytes 时截取头部并标记总字节数。
pub(crate) fn preview_value(v: &Value, max_bytes: usize) -> Value {
    let s = serde_json::to_string(v).unwrap_or_default();
    if s.len() <= max_bytes {
        return v.clone();
    }
    let mut end = max_bytes.min(s.len());
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    Value::String(format!(
        "{}…[truncated, {} bytes total]",
        &s[..end],
        s.len()
    ))
}

/// webhook 投递：共享加固 HTTP client（SSRF 策略同 operator），
/// 最多 3 次尝试（指数退避 1s/2s），4xx 不重试。
async fn deliver_webhook(url: &str, event: &RoutineEventRecord) -> Result<(), String> {
    use weaveflow::operator::builtin::http_client;
    http_client::block_private_ips(url)
        .await
        .map_err(|e| e.to_string())?;
    let mut last_err = String::new();
    for attempt in 0..3u32 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1 << (attempt - 1))).await;
        }
        match http_client::http_client()
            .post(url)
            .json(event)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                last_err = format!("HTTP {}", resp.status());
                if resp.status().is_client_error() {
                    break; // 对端明确拒绝，重试无意义
                }
            }
            Err(e) => last_err = e.to_string(),
        }
    }
    Err(format!("webhook POST {url} 失败: {last_err}"))
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
pub struct RoutineManager {
    workers: std::sync::Mutex<HashMap<String, WorkerHandle>>,
    events: tokio::sync::broadcast::Sender<Vec<u8>>,
}

impl RoutineManager {
    pub fn new() -> Self {
        let (events, _) = tokio::sync::broadcast::channel(64);
        Self {
            workers: std::sync::Mutex::new(HashMap::new()),
            events,
        }
    }

    fn emit(&self, ev: &RoutineEventRecord) {
        if let Ok(bytes) = serde_json::to_vec(ev) {
            let _ = self.events.send(bytes); // 无订阅者时静默丢弃
        }
    }

    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Vec<u8>> {
        self.events.subscribe()
    }

    /// 启动（或替换）一个 routine 的后台 worker。
    pub fn start_worker(&self, state: &Arc<AppState>, def: &RoutineDef) {
        self.stop_worker(&def.name);
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let push = match def.routine_type {
            RoutineType::Stream => {
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
            RoutineType::Cron => {
                let cfg = def.cron.clone().unwrap_or_default();
                tokio::spawn(cron_worker(state.clone(), def.clone(), cfg, cancel_rx));
                None
            }
        };
        self.workers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                def.name.clone(),
                WorkerHandle {
                    cancel: cancel_tx,
                    push,
                },
            );
        tracing::info!(routine = %def.name, r#type = %def.routine_type, "routine worker started");
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

    /// push 一批元素到 stream routine 的缓冲。返回缓冲中的元素总数。
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

impl Default for RoutineManager {
    fn default() -> Self {
        Self::new()
    }
}

/// daemon 启动时从 redb 恢复全部 routine worker。
pub fn start_all(state: &Arc<AppState>) {
    let rows = match state.db.list_routines() {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "failed to load routines on startup");
            return;
        }
    };
    for row in rows {
        state.routine_mgr.start_worker(state, &row.def);
    }
}

// ── 提交与状态记录 ───────────────────────────────────────────────────────

async fn fire(state: &Arc<AppState>, name: &str, pipeline: &str, inputs: HashMap<String, Value>) {
    match submit_run(state, pipeline, inputs, &format!("routine:{name}")).await {
        Ok(outcome) => {
            tracing::info!(
                routine = %name,
                task_id = %outcome.task_id,
                "routine fired"
            );
            if let Err(e) = state.db.update_routine(name, |row| {
                row.record_fired(chrono::Utc::now(), &outcome.task_id);
            }) {
                tracing::warn!(routine = %name, error = %e, "update_routine(fired) failed");
            }
            emit(state, fired_event(name, &outcome.task_id));
        }
        Err(WeaveflowError::Unavailable(_)) => {
            tracing::warn!(routine = %name, "routine dropped: daemon draining");
            emit(state, dropped_event(name, "daemon draining"));
        }
        Err(e) => {
            tracing::error!(routine = %name, error = %e, "routine submit failed");
            if let Err(db_err) = state.db.update_routine(name, |row| row.record_failed()) {
                tracing::warn!(routine = %name, error = %db_err, "update_routine(failed) failed");
            }
            emit(state, failed_event(name, e));
        }
    }
}

// ── cron worker ─────────────────────────────────────────────────────────

async fn cron_worker(
    state: Arc<AppState>,
    def: RoutineDef,
    cfg: CronConfig,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) {
    let name = def.name.clone();
    // 启动时 misfire 处理：停机期间错过的触发点按策略补一次或跳过。
    if let Ok(Some(row)) = state.db.load_routine(&name)
        && missed_fire(&row, chrono::Utc::now()).is_some()
    {
        match cfg.misfire {
            MisfirePolicy::CatchUp => {
                tracing::info!(routine = %name, "cron misfire catch-up: firing once");
                fire(&state, &name, &def.pipeline, cfg.inputs.clone()).await;
            }
            MisfirePolicy::Skip => {
                tracing::info!(routine = %name, "cron misfire skipped");
            }
        }
    }
    loop {
        let base = match state.db.load_routine(&name) {
            Ok(Some(row)) => row.last_fired_at.unwrap_or(row.created_at),
            _ => {
                // routine 已被删除但 worker 尚未收到 cancel —— 直接退出
                tracing::warn!(routine = %name, "cron worker: row missing, exiting");
                return;
            }
        };
        let now = chrono::Utc::now();
        let Some(next) = next_fire_after(&cfg, base, now) else {
            tracing::error!(routine = %name, "cron worker: cannot compute next fire, exiting");
            return;
        };
        if let Err(e) = state.db.update_routine(&name, |row| {
            row.next_fire_at = Some(next);
        }) {
            tracing::warn!(routine = %name, error = %e, "update_routine(next_fire) failed");
        }
        let wait = (next - now).to_std().unwrap_or(std::time::Duration::ZERO);
        tokio::select! {
            _ = cancel.changed() => {
                tracing::info!(routine = %name, "cron worker stopped");
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
    def: RoutineDef,
    cfg: StreamConfig,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<Vec<Value>>,
    buffered: Arc<AtomicUsize>,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) {
    let name = def.name.clone();
    let sem = Arc::new(tokio::sync::Semaphore::new(
        cfg.max_in_flight.max(1) as usize
    ));
    let flush_dur = cfg.flush_interval_duration().unwrap_or_else(|e| {
        tracing::warn!(routine = %name, error = %e, "invalid flush_interval, disabled");
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
        match submit_run(state, pipeline, inputs, &format!("routine:{name}")).await {
            Ok(outcome) => {
                tracing::info!(
                    routine = %name,
                    task_id = %outcome.task_id,
                    "stream batch fired"
                );
                if let Err(e) = state.db.update_routine(name, |row| {
                    row.record_fired(chrono::Utc::now(), &outcome.task_id);
                }) {
                    tracing::warn!(routine = %name, error = %e, "update_routine(fired) failed");
                }
                emit(state, fired_event(name, &outcome.task_id));
                tokio::spawn(async move {
                    let _permit = permit;
                    wait_task_terminal(&db, &outcome.task_id).await;
                });
            }
            Err(WeaveflowError::Unavailable(_)) => {
                tracing::warn!(routine = %name, "stream batch dropped: daemon draining");
                emit(state, dropped_event(name, "daemon draining"));
                drop(permit);
            }
            Err(e) => {
                tracing::error!(routine = %name, error = %e, "stream batch submit failed");
                if let Err(db_err) = state.db.update_routine(name, |row| row.record_failed()) {
                    tracing::warn!(routine = %name, error = %db_err, "update_routine(failed) failed");
                }
                emit(state, failed_event(name, e));
                drop(permit);
            }
        }
    }

    // flush_interval 缺省时 timer 为 None，对应 select 分支永不就绪。
    let mut flush_timer: Option<tokio::time::Interval> =
        flush_dur.map(|d| tokio::time::interval_at(tokio::time::Instant::now() + d, d));

    loop {
        tokio::select! {
            _ = cancel.changed() => {
                // 关闭语义：尽力把剩余缓冲全部 flush 后退出
                while !buf.is_empty() {
                    let take = buf.len().min(cfg.batch_size);
                    let batch: Vec<Value> = buf.drain(..take).collect();
                    flush_batch(&state, &sem, &name, &def.pipeline, &cfg.slot, batch, &buffered).await;
                }
                tracing::info!(routine = %name, "stream worker stopped");
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

/// 轮询 task 状态直到终态（用于释放 routine 级并发 permit）。
async fn wait_task_terminal(
    db: &Arc<weaveflow::store::Database>,
    task_id: &weaveflow::tracker::TaskId,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        match db.load_task(task_id) {
            Ok(Some(meta)) if meta.status == weaveflow::tracker::meta::TASK_STATUS_RUNNING => {}
            _ => return, // 终态、行消失（prune）或 DB 错误都视为可释放
        }
    }
}

// ── HTTP handlers ───────────────────────────────────────────────────────

pub async fn upsert_routine(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(def): Json<RoutineDef>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!(routine = %name, "PUT /routines/:name");
    if def.name != name {
        return Err(WeaveflowError::BadRequest(format!(
            "body 中的 name ({}) 与路径 ({name}) 不一致",
            def.name
        )));
    }
    let errors = validate_routine(&def);
    if !errors.is_empty() {
        tracing::warn!(routine = %name, errors = %errors.join("; "), "routine validation failed");
        return Err(WeaveflowError::Validation(errors.join("; ")));
    }
    if state.db.find_pipeline_by_name(&def.pipeline)?.is_none() {
        return Err(WeaveflowError::BadRequest(format!(
            "pipeline {} not found（routine 引用已注册的 pipeline，请先 apply）",
            def.pipeline
        )));
    }

    let existed = state.db.load_routine(&name)?;
    let (row, status) = match existed {
        Some(mut old) => {
            old.def = def.clone();
            (old, "updated")
        }
        None => (RoutineRow::new(def.clone()), "created"),
    };
    state.db.save_routine(&row)?;
    state.routine_mgr.start_worker(&state, &def);
    tracing::info!(routine = %name, status, "routine saved");
    Ok(Json(serde_json::json!({"name": name, "status": status})))
}

pub async fn list_routines(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!("GET /routines");
    let rows = state.db.list_routines()?;
    let list: Vec<Value> = rows.iter().map(row_summary).collect();
    Ok(Json(serde_json::json!(list)))
}

pub async fn get_routine(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!(routine = %name, "GET /routines/:name");
    let row = state
        .db
        .load_routine(&name)?
        .ok_or_else(|| WeaveflowError::NotFound(format!("routine {name} not found")))?;
    let mut v = serde_json::to_value(&row).map_err(|e| WeaveflowError::Internal(e.to_string()))?;
    if let Some(obj) = v.as_object_mut() {
        obj.insert(
            "buffered".into(),
            serde_json::json!(buffered_of(&state, &name)),
        );
    }
    Ok(Json(v))
}

pub async fn delete_routine(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, WeaveflowError> {
    tracing::info!(routine = %name, "DELETE /routines/:name");
    if !state.db.delete_routine(&name)? {
        return Err(WeaveflowError::NotFound(format!(
            "routine {name} not found"
        )));
    }
    state.db.delete_routine_events(&name)?;
    state.routine_mgr.stop_worker(&name);
    tracing::info!(routine = %name, "routine deleted");
    Ok(Json(serde_json::json!({"deleted": name})))
}

/// 事件收件箱增量回查：GET /routines/:name/events?after=<seq>&limit=<n>
/// 返回 seq > after 的事件（升序）。智能体记录已读到的最大 seq，
/// 下次会话从该位置续读即可拿到跨会话的完整反馈历史。
pub async fn list_routine_events(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Result<Json<Value>, WeaveflowError> {
    let after: u64 = params
        .get("after")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let limit: usize = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    tracing::info!(routine = %name, after, limit, "GET /routines/:name/events");
    if state.db.load_routine(&name)?.is_none() {
        return Err(WeaveflowError::NotFound(format!(
            "routine {name} not found"
        )));
    }
    let events = state.db.list_routine_events(&name, after, limit)?;
    Ok(Json(serde_json::json!(events)))
}

fn buffered_of(state: &Arc<AppState>, name: &str) -> usize {
    let workers = state
        .routine_mgr
        .workers
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    workers
        .get(name)
        .and_then(|h| h.push.as_ref())
        .map(|p| p.buffered.load(Ordering::SeqCst))
        .unwrap_or(0)
}

fn row_summary(row: &RoutineRow) -> Value {
    serde_json::json!({
        "name": row.def.name,
        "pipeline": row.def.pipeline,
        "type": row.def.routine_type.to_string(),
        "created_at": row.created_at.to_rfc3339(),
        "last_fired_at": row.last_fired_at.map(|t| t.to_rfc3339()),
        "next_fire_at": row.next_fire_at.map(|t| t.to_rfc3339()),
        "total_fired": row.total_fired,
        "total_failed": row.total_failed,
    })
}

pub async fn push_routine(
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
    tracing::info!(routine = %name, items = n, "POST /routines/:name/push");
    match state.routine_mgr.push(&name, items) {
        Ok(buffered) => Ok(Json(serde_json::json!({
            "accepted": n,
            "buffered": buffered,
        }))
        .into_response()),
        Err(PushError::NotFound) => Err(WeaveflowError::NotFound(format!(
            "routine {name} not found（或 worker 未运行）"
        ))),
        Err(PushError::NotStream) => Err(WeaveflowError::BadRequest(format!(
            "routine {name} 不是 stream 类型，不能 push"
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

pub async fn ws_routine(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<Response, WeaveflowError> {
    tracing::info!(routine = %name, "GET /routines/:name/ws");
    if state.db.load_routine(&name)?.is_none() {
        return Err(WeaveflowError::NotFound(format!(
            "routine {name} not found"
        )));
    }
    let rx = state.routine_mgr.subscribe_events();
    Ok(ws.on_upgrade(move |socket| handle_routine_ws(socket, rx, name)))
}

async fn handle_routine_ws(
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
                        // 全局事件流按 routine 名过滤
                        let v: Value = serde_json::from_slice(&bytes).unwrap_or_default();
                        if v.get("routine").and_then(|t| t.as_str()) != Some(name.as_str()) {
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
        let dir =
            std::env::temp_dir().join(format!("weaveflow-routine-test-{}-{n}", std::process::id()));
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
            routine_mgr: Arc::new(RoutineManager::new()),
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

    fn stream_def(name: &str, batch_size: usize) -> RoutineDef {
        RoutineDef {
            name: name.into(),
            pipeline: "trig_pipe".into(),
            routine_type: RoutineType::Stream,
            stream: Some(StreamConfig {
                batch_size,
                flush_interval: None,
                max_in_flight: 4,
                slot: "items".into(),
                buffer_cap: 1000,
            }),
            cron: None,
            notify: None,
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
    async fn routine_crud_roundtrip() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state).await;
        let client = reqwest::Client::new();

        // PUT 创建
        let resp = client
            .put(format!("{base}/routines/s1"))
            .json(&stream_def("s1", 10))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "created");

        // PUT 同名更新
        let resp = client
            .put(format!("{base}/routines/s1"))
            .json(&stream_def("s1", 20))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "updated");

        // GET 列表 + 详情
        let resp = client.get(format!("{base}/routines")).send().await.unwrap();
        let list: Value = resp.json().await.unwrap();
        assert_eq!(list.as_array().unwrap().len(), 1);
        assert_eq!(list[0]["name"], "s1");

        let resp = client
            .get(format!("{base}/routines/s1"))
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
            .put(format!("{base}/routines/bad"))
            .json(&bad)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // pipeline 不存在
        let mut orphan = stream_def("orphan", 10);
        orphan.pipeline = "no_such".into();
        let resp = client
            .put(format!("{base}/routines/orphan"))
            .json(&orphan)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // name 不一致
        let resp = client
            .put(format!("{base}/routines/other"))
            .json(&stream_def("s1", 10))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 400);

        // DELETE
        let resp = client
            .delete(format!("{base}/routines/s1"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .delete(format!("{base}/routines/s1"))
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
            .put(format!("{base}/routines/s1"))
            .json(&stream_def("s1", 3))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // push 5 条（数组 3 + 单对象 1 + 数组 1）→ 一批 3 条触发，余 2 条缓冲
        let resp = client
            .post(format!("{base}/routines/s1/push"))
            .json(&serde_json::json!([1, 2, 3]))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/routines/s1/push"))
            .json(&serde_json::json!({"x": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/routines/s1/push"))
            .json(&serde_json::json!([5]))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["accepted"], 1);

        // 等 worker flush
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let row = db.load_routine("s1").unwrap().unwrap();
        assert_eq!(row.total_fired, 1, "row: {row:?}");
        assert_eq!(row.recent_tasks.len(), 1);

        // task 的 inputs 应是 {items: [1,2,3]}，且来源为 routine
        let tid = weaveflow::tracker::TaskId(uuid::Uuid::parse_str(&row.recent_tasks[0]).unwrap());
        let meta = db.load_task(&tid).unwrap().unwrap();
        assert_eq!(meta.inputs, serde_json::json!({"items": [1, 2, 3]}));
        assert_eq!(meta.routine_source.as_deref(), Some("routine:s1"));

        // 剩余 2 条仍在缓冲
        let resp = client
            .get(format!("{base}/routines/s1"))
            .send()
            .await
            .unwrap();
        let detail: Value = resp.json().await.unwrap();
        assert_eq!(detail["buffered"], 2);

        // cron routine 不接受 push
        let cron_def = RoutineDef {
            name: "c1".into(),
            pipeline: "trig_pipe".into(),
            routine_type: RoutineType::Cron,
            stream: None,
            cron: Some(CronConfig {
                schedule: None,
                interval: Some("1h".into()),
                misfire: MisfirePolicy::Skip,
                inputs: HashMap::new(),
            }),
            notify: None,
        };
        client
            .put(format!("{base}/routines/c1"))
            .json(&cron_def)
            .send()
            .await
            .unwrap();
        let resp = client
            .post(format!("{base}/routines/c1/push"))
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
            .put(format!("{base}/routines/cap"))
            .json(&def)
            .send()
            .await
            .unwrap();

        let resp = client
            .post(format!("{base}/routines/cap/push"))
            .json(&serde_json::json!([1, 2, 3, 4]))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            .post(format!("{base}/routines/cap/push"))
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

        let def = RoutineDef {
            name: "tick".into(),
            pipeline: "trig_pipe".into(),
            routine_type: RoutineType::Cron,
            stream: None,
            cron: Some(CronConfig {
                schedule: None,
                interval: Some("1s".into()),
                misfire: MisfirePolicy::Skip,
                inputs: HashMap::from([("k".into(), serde_json::json!("v"))]),
            }),
            notify: None,
        };
        let resp = client
            .put(format!("{base}/routines/tick"))
            .json(&def)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        // 等 ~2.5s 应至少触发 1 次
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        let row = db.load_routine("tick").unwrap().unwrap();
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
            .put(format!("{base}/routines/s1"))
            .json(&stream_def("s1", 2))
            .send()
            .await
            .unwrap();
        // push 3 条（应触发 1 批，余 1 条），删除后 worker flush 剩余
        client
            .post(format!("{base}/routines/s1/push"))
            .json(&serde_json::json!([1, 2, 3]))
            .send()
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        client
            .delete(format!("{base}/routines/s1"))
            .send()
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        // worker 关闭语义：剩余 1 条也被 flush → 共 2 次触发
        assert!(state.db.load_routine("s1").unwrap().is_none());
        // 无法直接查 total_fired（行已删），用 task 列表验证
        let tasks = state.db.list_tasks().unwrap();
        let routine_tasks: Vec<_> = tasks
            .iter()
            .filter(|t| t.routine_source.as_deref() == Some("routine:s1"))
            .collect();
        assert_eq!(routine_tasks.len(), 2, "tasks: {tasks:?}");

        handle.abort();
    }

    #[tokio::test]
    async fn inbox_records_fire_and_terminal_events() {
        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state.clone()).await;
        let client = reqwest::Client::new();

        client
            .put(format!("{base}/routines/s1"))
            .json(&stream_def("s1", 2))
            .send()
            .await
            .unwrap();
        client
            .post(format!("{base}/routines/s1/push"))
            .json(&serde_json::json!([1, 2]))
            .send()
            .await
            .unwrap();
        // 等 flush + noop task 完成
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;

        let resp = client
            .get(format!("{base}/routines/s1/events"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let events: Value = resp.json().await.unwrap();
        let events = events.as_array().unwrap();
        let kinds: Vec<&str> = events.iter().map(|e| e["kind"].as_str().unwrap()).collect();
        assert!(
            kinds.contains(&"fired") && kinds.contains(&"task_completed"),
            "events: {kinds:?}"
        );
        // seq 单调递增且从 1 开始
        let seqs: Vec<u64> = events.iter().map(|e| e["seq"].as_u64().unwrap()).collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "seqs: {seqs:?}");
        assert_eq!(seqs[0], 1);
        // task_completed 带 output_preview（noop 输出 {}）与 task_id
        let completed = events
            .iter()
            .find(|e| e["kind"] == "task_completed")
            .unwrap();
        assert!(completed.get("output_preview").is_some());
        assert!(completed["task_id"].as_str().is_some());

        // 增量回查：after = 最大 seq → 空；再 push 一批 → 只回新事件
        let max_seq = *seqs.iter().max().unwrap();
        let resp = client
            .get(format!("{base}/routines/s1/events?after={max_seq}"))
            .send()
            .await
            .unwrap();
        let events: Value = resp.json().await.unwrap();
        assert_eq!(events.as_array().unwrap().len(), 0);

        client
            .post(format!("{base}/routines/s1/push"))
            .json(&serde_json::json!([3, 4]))
            .send()
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        let resp = client
            .get(format!("{base}/routines/s1/events?after={max_seq}"))
            .send()
            .await
            .unwrap();
        let events: Value = resp.json().await.unwrap();
        let events = events.as_array().unwrap();
        assert!(!events.is_empty());
        assert!(events.iter().all(|e| e["seq"].as_u64().unwrap() > max_seq));

        // 不存在 routine 的 events → 404
        let resp = client
            .get(format!("{base}/routines/nope/events"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 404);

        // 删除 routine 后收件箱也被清空
        client
            .delete(format!("{base}/routines/s1"))
            .send()
            .await
            .unwrap();
        assert!(state.db.list_routine_events("s1", 0, 0).unwrap().is_empty());

        handle.abort();
    }

    #[tokio::test]
    async fn webhook_delivers_terminal_event() {
        // 本地 webhook 接收端：收集 POST 过来的事件 JSON
        let received = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
        let received_clone = received.clone();
        let hook_app = axum::Router::new().route(
            "/hook",
            axum::routing::post(move |axum::Json(v): axum::Json<Value>| {
                let received = received_clone.clone();
                async move {
                    received.lock().unwrap().push(v);
                    axum::http::StatusCode::OK
                }
            }),
        );
        let hook_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hook_addr = hook_listener.local_addr().unwrap();
        let hook_handle = tokio::spawn(async move {
            axum::serve(hook_listener, hook_app).await.unwrap();
        });

        let db = Arc::new(temp_db());
        register_test_pipeline(&db);
        let state = test_state(db.clone());
        let (base, handle) = serve_app(state.clone()).await;
        let client = reqwest::Client::new();

        let mut def = stream_def("wh", 2);
        def.notify = Some(weaveflow::routine::NotifyDef {
            webhook_url: Some(format!("http://{hook_addr}/hook")),
            preview_bytes: 2048,
        });
        client
            .put(format!("{base}/routines/wh"))
            .json(&def)
            .send()
            .await
            .unwrap();
        client
            .post(format!("{base}/routines/wh/push"))
            .json(&serde_json::json!([1, 2]))
            .send()
            .await
            .unwrap();

        // 等 flush + task 完成 + webhook 投递
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if received
                .lock()
                .unwrap()
                .iter()
                .any(|e| e["kind"] == "task_completed")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "webhook 未收到终态事件"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let events = received.lock().unwrap();
        let ev = events
            .iter()
            .find(|e| e["kind"] == "task_completed")
            .unwrap();
        assert_eq!(ev["routine"], "wh");
        assert!(ev["task_id"].as_str().is_some());
        assert!(ev.get("output_preview").is_some());

        handle.abort();
        hook_handle.abort();
    }

    #[test]
    fn preview_value_truncates_and_marks() {
        let small = serde_json::json!({"a": 1});
        assert_eq!(preview_value(&small, 2048), small);

        let big = serde_json::json!({"data": "x".repeat(10_000)});
        let preview = preview_value(&big, 100);
        let s = preview.as_str().unwrap();
        assert!(s.contains("truncated"), "preview: {s}");
        assert!(s.len() < 200, "preview len: {}", s.len());
        // 多字节字符边界安全
        let wide = serde_json::json!({"data": "汉".repeat(1000)});
        let preview = preview_value(&wide, 50);
        assert!(preview.as_str().unwrap().contains("truncated"));
    }
}
