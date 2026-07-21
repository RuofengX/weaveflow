//! Routine（例程）：智能体委托给 daemon 的长驻任务岗。
//!
//! 定位：智能体把「反复做/盯着做」的事注册为 routine —— 引用已注册的
//! pipeline，把「定时」与「流式微批」两类触发转化为普通的 pipeline run，
//! 任务终态后通过持久化事件收件箱（+ 可选 webhook）反馈给委托方。
//! 本模块只包含纯数据类型与校验/调度计算，不含任何运行时组件
//! （worker、HTTP handler 在 server 层）。daemon 只接收 JSON 配置；
//! TOML/YAML 等文件格式是 CLI 侧的本地实现细节。

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::tracker::TaskId;

// ── 配置类型 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoutineType {
    Stream,
    Cron,
}

impl std::fmt::Display for RoutineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutineType::Stream => write!(f, "stream"),
            RoutineType::Cron => write!(f, "cron"),
        }
    }
}

/// daemon 停机期间错过触发点后的策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MisfirePolicy {
    /// 重启后补最近一次错过的触发（更早的丢弃）。
    CatchUp,
    /// 错过的直接丢弃，从下一个触发点开始。
    #[default]
    Skip,
}

/// stream 触发器：push 进来的元素在内存中缓冲，按 batch_size / flush_interval
/// 切成微批，每批作为一次 pipeline run 提交（写入指定 slot）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConfig {
    /// 凑够多少条元素 flush 一批。
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// 距上一条元素到达超过该时长即 flush（"5s"；缺省 = 不按时间 flush）。
    #[serde(default)]
    pub flush_interval: Option<String>,
    /// 该 routine 最多同时运行多少个微批 task。
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: u32,
    /// 微批数组写入的 slot 名。
    #[serde(default = "default_slot")]
    pub slot: String,
    /// 内存缓冲元素数上限，到顶后 push 返回 429。
    #[serde(default = "default_buffer_cap")]
    pub buffer_cap: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            flush_interval: None,
            max_in_flight: default_max_in_flight(),
            slot: default_slot(),
            buffer_cap: default_buffer_cap(),
        }
    }
}

fn default_batch_size() -> usize {
    100
}
fn default_max_in_flight() -> u32 {
    4
}
fn default_slot() -> String {
    "items".to_string()
}
fn default_buffer_cap() -> usize {
    100_000
}

/// cron 触发器：按 cron 表达式或固定间隔周期性地提交 pipeline run。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CronConfig {
    /// cron 表达式（与 interval 二选一）：支持 5 段（分 时 日 月 周）
    /// 或 6/7 段（含秒），如 "0 3 * * *"。
    #[serde(default)]
    pub schedule: Option<String>,
    /// 固定间隔（"5m"、"1h"；与 schedule 二选一）。
    #[serde(default)]
    pub interval: Option<String>,
    #[serde(default)]
    pub misfire: MisfirePolicy,
    /// 每次触发时作为 slots 提交的静态输入。
    #[serde(default)]
    pub inputs: HashMap<String, Value>,
}

/// 反馈配置：routine 产生的 task 到达终态（或触发失败/丢弃）时如何通知委托方。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifyDef {
    /// 终态事件 POST 到该 URL（可选；走共享加固 HTTP client，SSRF 检查同样生效）。
    /// 面向「智能体值班」范式：harness 类软件接收 webhook 后唤醒对应智能体。
    #[serde(default)]
    pub webhook_url: Option<String>,
    /// 事件载荷 output_preview 的最大字节数。
    #[serde(default = "default_preview_bytes")]
    pub preview_bytes: u32,
}

fn default_preview_bytes() -> u32 {
    2048
}

/// 事件载荷 output_preview 字节数上限（防止把大结果塞进事件/ webhook）。
pub const MAX_PREVIEW_BYTES: u32 = 64 * 1024;

/// Routine 定义（daemon API 的 JSON 载体；CLI 侧可由 TOML 反序列化得到）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutineDef {
    pub name: String,
    /// 引用的已注册 pipeline 名（PUT 时校验存在，之后为弱引用）。
    pub pipeline: String,
    #[serde(rename = "type")]
    pub routine_type: RoutineType,
    #[serde(default)]
    pub stream: Option<StreamConfig>,
    #[serde(default)]
    pub cron: Option<CronConfig>,
    /// 终态反馈配置；缺省 = 仅写入持久化事件收件箱，不主动推送。
    #[serde(default)]
    pub notify: Option<NotifyDef>,
}

// ── 事件收件箱 ──────────────────────────────────────────────────────────

/// routine 事件（持久化到 redb routine_event 表，同时经内存 broadcast 推送 WS）。
///
/// 面向「智能体值班」范式：智能体注册 routine 后无需长连接守候，
/// 下次会话用 `GET /routines/:name/events?after=<seq>` 增量回查历史；
/// 配了 notify.webhook_url 的 routine 还会主动 POST 终态事件唤醒 harness。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineEventRecord {
    /// 每个 routine 内单调递增的序号（重启不重复，由 RoutineRow.event_seq 分配）。
    pub seq: u64,
    pub routine: String,
    /// fired（产生 task）/ failed（提交失败）/ dropped（draining 或缓冲满丢弃）/
    /// task_completed / task_failed / notify_failed（webhook 投递失败）
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// 终态事件附带的 pipeline 最终输出截断预览（字节数由 notify.preview_bytes 控制）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_preview: Option<Value>,
    pub at: DateTime<Utc>,
}

/// 每个 routine 收件箱最多保留的事件条数（超出即删最旧）。
pub const EVENT_INBOX_CAP: u64 = 100;

// ── 持久化行（配置 + 运行时状态） ────────────────────────────────────────
/// redb routine 表的一行：定义 + 跨重启保留的运行时状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutineRow {
    pub def: RoutineDef,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub last_fired_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub next_fire_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub total_fired: u64,
    #[serde(default)]
    pub total_failed: u64,
    /// 近期触发产生的 task_id（新在前，上限 RECENT_TASKS_CAP 条）。
    #[serde(default)]
    pub recent_tasks: Vec<String>,
    /// 事件收件箱的单调递增序号分配器（持久化，重启不重复）。
    #[serde(default)]
    pub event_seq: u64,
}

pub const RECENT_TASKS_CAP: usize = 20;

impl RoutineRow {
    pub fn new(def: RoutineDef) -> Self {
        Self {
            def,
            created_at: Utc::now(),
            last_fired_at: None,
            next_fire_at: None,
            total_fired: 0,
            total_failed: 0,
            recent_tasks: Vec::new(),
            event_seq: 0,
        }
    }

    pub fn record_fired(&mut self, at: DateTime<Utc>, task_id: &TaskId) {
        self.last_fired_at = Some(at);
        self.total_fired += 1;
        self.recent_tasks.insert(0, task_id.to_string());
        self.recent_tasks.truncate(RECENT_TASKS_CAP);
    }

    pub fn record_failed(&mut self) {
        self.total_failed += 1;
    }
}

// ── 时长解析（"500ms"/"30s"/"5m"/"1h"，裸数字 = 秒） ────────────────────

pub fn parse_duration_str(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let (num, mult_ms) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (s, 1_000)
    };
    let v: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid duration {s:?} (expect e.g. 500ms, 30s, 5m, 1h)"))?;
    let ms = v
        .checked_mul(mult_ms)
        .ok_or_else(|| format!("duration overflow: {s:?}"))?;
    Ok(Duration::from_millis(ms))
}

impl StreamConfig {
    pub fn flush_interval_duration(&self) -> Result<Option<Duration>, String> {
        self.flush_interval
            .as_deref()
            .map(parse_duration_str)
            .transpose()
    }
}

impl CronConfig {
    pub fn interval_duration(&self) -> Result<Option<Duration>, String> {
        self.interval.as_deref().map(parse_duration_str).transpose()
    }
}

// ── 校验 ────────────────────────────────────────────────────────────────

/// 校验 routine 定义。错误为 "[code] message" 格式字符串（与 daemon 校验风格一致）。
pub fn validate_routine(def: &RoutineDef) -> Vec<String> {
    let mut errors = Vec::new();
    if def.name.trim().is_empty() {
        errors.push("[invalid_name] routine name 不能为空".to_string());
    }
    if def.pipeline.trim().is_empty() {
        errors.push("[invalid_pipeline] pipeline 不能为空".to_string());
    }
    if let Some(n) = &def.notify {
        if let Some(url) = &n.webhook_url
            && !(url.starts_with("http://") || url.starts_with("https://"))
        {
            errors.push("[invalid_webhook_url] webhook_url 必须是 http(s) URL".to_string());
        }
        if n.preview_bytes == 0 || n.preview_bytes > MAX_PREVIEW_BYTES {
            errors.push(format!(
                "[invalid_preview_bytes] preview_bytes 必须在 1..={MAX_PREVIEW_BYTES} 之间"
            ));
        }
    }
    match def.routine_type {
        RoutineType::Stream => {
            if def.cron.is_some() {
                errors
                    .push("[section_mismatch] type 为 stream 时不允许出现 cron 配置段".to_string());
            }
            match &def.stream {
                None => errors
                    .push("[section_mismatch] type 为 stream 时缺少 stream 配置段".to_string()),
                Some(s) => {
                    if s.batch_size == 0 {
                        errors.push("[invalid_batch_size] batch_size 必须 >= 1".to_string());
                    }
                    if s.max_in_flight == 0 {
                        errors.push("[invalid_max_in_flight] max_in_flight 必须 >= 1".to_string());
                    }
                    if s.slot.trim().is_empty() {
                        errors.push("[invalid_slot] slot 不能为空".to_string());
                    }
                    if s.buffer_cap < s.batch_size {
                        errors.push(format!(
                            "[invalid_buffer_cap] buffer_cap ({}) 不能小于 batch_size ({})",
                            s.buffer_cap, s.batch_size
                        ));
                    }
                    match s.flush_interval_duration() {
                        Ok(Some(d)) if d.is_zero() => errors
                            .push("[invalid_flush_interval] flush_interval 必须 > 0".to_string()),
                        Err(e) => errors.push(format!("[invalid_flush_interval] {e}")),
                        _ => {}
                    }
                }
            }
        }
        RoutineType::Cron => {
            if def.stream.is_some() {
                errors
                    .push("[section_mismatch] type 为 cron 时不允许出现 stream 配置段".to_string());
            }
            match &def.cron {
                None => {
                    errors.push("[section_mismatch] type 为 cron 时缺少 cron 配置段".to_string())
                }
                Some(c) => match (&c.schedule, &c.interval) {
                    (Some(_), Some(_)) => errors
                        .push("[invalid_schedule] schedule 与 interval 只能二选一".to_string()),
                    (None, None) => errors.push(
                        "[invalid_schedule] cron 触发器必须提供 schedule 或 interval".to_string(),
                    ),
                    (Some(expr), None) => {
                        if parse_cron(expr).is_none() {
                            errors
                                .push(format!("[invalid_schedule] 无法解析 cron 表达式: {expr:?}"));
                        }
                    }
                    (None, Some(iv)) => match parse_duration_str(iv) {
                        Err(e) => errors.push(format!("[invalid_interval] {e}")),
                        Ok(d) if d < Duration::from_secs(1) => {
                            errors.push("[invalid_interval] interval 必须 >= 1s".to_string())
                        }
                        _ => {}
                    },
                },
            }
        }
    }
    errors
}

fn parse_cron(expr: &str) -> Option<cron::Schedule> {
    use std::str::FromStr;
    // cron crate 要求 6/7 段（含秒）；兼容常见 5 段写法（分 时 日 月 周），
    // 自动前置 "0" 秒字段。
    let owned;
    let expr = if expr.split_whitespace().count() == 5 {
        owned = format!("0 {expr}");
        &owned
    } else {
        expr
    };
    cron::Schedule::from_str(expr).ok()
}

// ── 调度计算 ────────────────────────────────────────────────────────────

/// 计算 cron 触发器在 `now` 之后的下一次触发时间。
/// interval 以 `base`（通常为 created_at）为锚点对齐，避免重启后漂移。
pub fn next_fire_after(
    c: &CronConfig,
    base: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    if let Some(expr) = &c.schedule {
        let sched = parse_cron(expr)?;
        return sched.after(&now).next();
    }
    let iv = c.interval_duration().ok().flatten()?;
    let iv_ms = iv.as_millis() as i64;
    if iv_ms <= 0 {
        return None;
    }
    let elapsed = (now - base).num_milliseconds().max(0);
    let k = elapsed / iv_ms + 1;
    base.checked_add_signed(chrono::Duration::milliseconds(k * iv_ms))
}

/// 判断启动（或重新加载）时是否存在错过的触发点。
/// 存在时返回错过的触发时间（最近一次）。
pub fn missed_fire(row: &RoutineRow, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let RoutineType::Cron = row.def.routine_type else {
        return None;
    };
    let c = row.def.cron.as_ref()?;
    let base = row.last_fired_at.unwrap_or(row.created_at);
    if let Some(expr) = &c.schedule {
        let sched = parse_cron(expr)?;
        let missed = sched.after(&base).next()?;
        if missed <= now { Some(missed) } else { None }
    } else {
        let iv = c.interval_duration().ok().flatten()?;
        let iv_ms = iv.as_millis() as i64;
        if iv_ms <= 0 {
            return None;
        }
        let missed = base.checked_add_signed(chrono::Duration::milliseconds(iv_ms))?;
        if missed <= now { Some(missed) } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_def() -> RoutineDef {
        RoutineDef {
            name: "s".into(),
            pipeline: "p".into(),
            routine_type: RoutineType::Stream,
            stream: Some(StreamConfig::default()),
            cron: None,
            notify: None,
        }
    }

    fn cron_def() -> RoutineDef {
        RoutineDef {
            name: "c".into(),
            pipeline: "p".into(),
            routine_type: RoutineType::Cron,
            stream: None,
            cron: Some(CronConfig {
                schedule: None,
                interval: Some("5m".into()),
                misfire: MisfirePolicy::Skip,
                inputs: HashMap::new(),
            }),
            notify: None,
        }
    }

    #[test]
    fn validate_notify_rules() {
        let mut d = stream_def();
        d.notify = Some(NotifyDef {
            webhook_url: Some("https://harness.local/hook".into()),
            preview_bytes: 2048,
        });
        assert!(validate_routine(&d).is_empty());

        let mut d = stream_def();
        d.notify = Some(NotifyDef {
            webhook_url: Some("ftp://x".into()),
            preview_bytes: 2048,
        });
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_webhook_url"))
        );

        let mut d = stream_def();
        d.notify = Some(NotifyDef {
            webhook_url: None,
            preview_bytes: 0,
        });
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_preview_bytes"))
        );

        let mut d = stream_def();
        d.notify = Some(NotifyDef {
            webhook_url: None,
            preview_bytes: MAX_PREVIEW_BYTES + 1,
        });
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_preview_bytes"))
        );
    }

    #[test]
    fn validate_accepts_minimal_defs() {
        assert!(validate_routine(&stream_def()).is_empty());
        assert!(validate_routine(&cron_def()).is_empty());
    }

    #[test]
    fn validate_rejects_section_mismatch() {
        let mut d = stream_def();
        d.cron = Some(CronConfig::default());
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("section_mismatch"))
        );

        let mut d = stream_def();
        d.stream = None;
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("section_mismatch"))
        );

        let mut d = cron_def();
        d.stream = Some(StreamConfig::default());
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("section_mismatch"))
        );
    }

    #[test]
    fn validate_stream_bounds() {
        let mut d = stream_def();
        d.stream.as_mut().unwrap().batch_size = 0;
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_batch_size"))
        );

        let mut d = stream_def();
        d.stream.as_mut().unwrap().max_in_flight = 0;
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_max_in_flight"))
        );

        let mut d = stream_def();
        d.stream.as_mut().unwrap().buffer_cap = 1; // < batch_size(100)
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_buffer_cap"))
        );

        let mut d = stream_def();
        d.stream.as_mut().unwrap().flush_interval = Some("0s".into());
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_flush_interval"))
        );

        let mut d = stream_def();
        d.stream.as_mut().unwrap().flush_interval = Some("abc".into());
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_flush_interval"))
        );

        let mut d = stream_def();
        d.stream.as_mut().unwrap().flush_interval = Some("5s".into());
        assert!(validate_routine(&d).is_empty());
    }

    #[test]
    fn validate_cron_schedule_rules() {
        let mut d = cron_def();
        d.cron.as_mut().unwrap().schedule = Some("0 3 * * *".into());
        d.cron.as_mut().unwrap().interval = None;
        assert!(validate_routine(&d).is_empty());

        let mut d = cron_def();
        d.cron.as_mut().unwrap().schedule = Some("0 3 * * *".into());
        // schedule + interval 同时存在
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_schedule"))
        );

        let mut d = cron_def();
        d.cron.as_mut().unwrap().interval = None;
        // 两者都缺
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_schedule"))
        );

        let mut d = cron_def();
        d.cron.as_mut().unwrap().interval = None;
        d.cron.as_mut().unwrap().schedule = Some("not a cron".into());
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_schedule"))
        );

        let mut d = cron_def();
        d.cron.as_mut().unwrap().interval = Some("500ms".into());
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_interval"))
        );
    }

    #[test]
    fn validate_rejects_empty_names() {
        let mut d = stream_def();
        d.name = "  ".into();
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_name"))
        );
        let mut d = stream_def();
        d.pipeline = String::new();
        assert!(
            validate_routine(&d)
                .iter()
                .any(|e| e.contains("invalid_pipeline"))
        );
    }

    #[test]
    fn parse_duration_units() {
        assert_eq!(
            parse_duration_str("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(parse_duration_str("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration_str("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration_str("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration_str("42").unwrap(), Duration::from_secs(42));
        for s in ["", "abc", "10x", "-3s"] {
            assert!(parse_duration_str(s).is_err(), "input {s:?}");
        }
    }

    #[test]
    fn next_fire_interval_anchored_to_base() {
        let c = CronConfig {
            schedule: None,
            interval: Some("5m".into()),
            misfire: MisfirePolicy::Skip,
            inputs: HashMap::new(),
        };
        let base = Utc::now();
        let now = base + chrono::Duration::minutes(12);
        let next = next_fire_after(&c, base, now).unwrap();
        // 12 分钟已过 → 下一次是 base + 15m
        assert_eq!(next, base + chrono::Duration::minutes(15));
    }

    #[test]
    fn next_fire_cron_schedule() {
        let c = CronConfig {
            schedule: Some("0 3 * * *".into()),
            interval: None,
            misfire: MisfirePolicy::Skip,
            inputs: HashMap::new(),
        };
        let now = Utc::now();
        let next = next_fire_after(&c, now, now).unwrap();
        assert!(next > now);
        assert_eq!(
            next.format("%M:%S").to_string(),
            "00:00",
            "cron 应落在整点分钟"
        );
    }

    #[test]
    fn missed_fire_detects_overdue_interval() {
        let mut row = RoutineRow::new(cron_def());
        // created_at 是 now，interval 5m → 此刻不应有 missed
        assert!(missed_fire(&row, Utc::now()).is_none());
        // 模拟 created_at 在 10 分钟前
        row.created_at = Utc::now() - chrono::Duration::minutes(10);
        assert!(missed_fire(&row, Utc::now()).is_some());
        // last_fired 在 1 分钟前 → 无 missed
        row.last_fired_at = Some(Utc::now() - chrono::Duration::minutes(1));
        assert!(missed_fire(&row, Utc::now()).is_none());
    }

    #[test]
    fn missed_fire_none_for_stream() {
        let row = RoutineRow::new(stream_def());
        assert!(missed_fire(&row, Utc::now()).is_none());
    }

    #[test]
    fn routine_row_recent_tasks_capped() {
        let mut row = RoutineRow::new(stream_def());
        for _ in 0..30 {
            row.record_fired(Utc::now(), &TaskId::new());
        }
        assert_eq!(row.recent_tasks.len(), RECENT_TASKS_CAP);
        assert_eq!(row.total_fired, 30);
    }

    #[test]
    fn def_json_roundtrip_and_unknown_fields_rejected() {
        let d = stream_def();
        let json = serde_json::to_string(&d).unwrap();
        let back: RoutineDef = serde_json::from_str(&json).unwrap();
        assert_eq!(d, back);

        let with_unknown = r#"{"name":"x","pipeline":"p","type":"stream","nope":1}"#;
        assert!(serde_json::from_str::<RoutineDef>(with_unknown).is_err());
    }

    #[test]
    fn def_toml_deserialization() {
        let toml_src = r#"
name = "ingest"
pipeline = "process"
type = "stream"

[stream]
batch_size = 50
flush_interval = "5s"
max_in_flight = 2
"#;
        let d: RoutineDef = toml::from_str(toml_src).unwrap();
        assert_eq!(d.name, "ingest");
        assert!(validate_routine(&d).is_empty());
        let s = d.stream.unwrap();
        assert_eq!(s.batch_size, 50);
        assert_eq!(
            s.flush_interval_duration().unwrap(),
            Some(Duration::from_secs(5))
        );

        let toml_cron = r#"
name = "nightly"
pipeline = "etl"
type = "cron"

[cron]
schedule = "0 3 * * *"
misfire = "catch_up"

[cron.inputs]
date = "latest"
"#;
        let d: RoutineDef = toml::from_str(toml_cron).unwrap();
        assert!(validate_routine(&d).is_empty());
        let c = d.cron.unwrap();
        assert_eq!(c.misfire, MisfirePolicy::CatchUp);
        assert_eq!(c.inputs.get("date").unwrap(), &serde_json::json!("latest"));
    }
}
