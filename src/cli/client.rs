use futures::StreamExt;
use serde_json::Value;
use std::io::IsTerminal;
use tokio_tungstenite::connect_async;
use tungstenite::Message;

use super::config::CliConfig;

fn encode_segment(s: &str) -> String {
    let mut r = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            r.push(b as char);
        } else {
            use std::fmt::Write;
            let _ = write!(r, "%{b:02X}");
        }
    }
    r
}

pub(crate) fn parse_daemon_addr(daemon: &str) -> (&str, &str) {
    if let Some(rest) = daemon.strip_prefix("https://") {
        ("https://", rest.trim_end_matches('/'))
    } else if let Some(rest) = daemon.strip_prefix("http://") {
        ("http://", rest.trim_end_matches('/'))
    } else {
        ("http://", daemon.trim_end_matches('/'))
    }
}

fn api_url(daemon: &str, path: &str) -> String {
    let (scheme, host) = parse_daemon_addr(daemon);
    format!("{scheme}{host}{path}")
}

fn daemon_error(url: &str, e: impl std::fmt::Display) -> String {
    format!("无法连接 daemon ({url}): {e}")
}

fn check_http_status(status: u16, body: &str) -> Result<(), String> {
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("HTTP {status}: {}", body.trim()))
    }
}

async fn parse_response(resp: reqwest::Response, url: &str) -> Result<Value, String> {
    let status = resp.status().as_u16();
    let body = resp.text().await.map_err(|e| daemon_error(url, e))?;
    check_http_status(status, &body)?;
    serde_json::from_str(&body).map_err(|e| format!("响应格式错误 ({url}): {e}"))
}

async fn get(cfg: &CliConfig, path: &str) -> Result<Value, String> {
    let url = api_url(&cfg.daemon, path);
    let resp = cfg
        .http_client()
        .get(&url)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    parse_response(resp, &url).await
}

async fn post(cfg: &CliConfig, path: &str, body: Value) -> Result<Value, String> {
    let url = api_url(&cfg.daemon, path);
    let resp = cfg
        .http_client()
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    parse_response(resp, &url).await
}

async fn post_body(cfg: &CliConfig, path: &str, body: String) -> Result<Value, String> {
    let url = api_url(&cfg.daemon, path);
    let resp = cfg
        .http_client()
        .post(&url)
        .header("content-type", "text/plain")
        .body(body)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    parse_response(resp, &url).await
}

async fn delete(cfg: &CliConfig, path: &str) -> Result<Value, String> {
    let url = api_url(&cfg.daemon, path);
    let resp = cfg
        .http_client()
        .delete(&url)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    parse_response(resp, &url).await
}

// ── Pipeline ──────────────────────────────────────────────────────────────

pub async fn pipeline_apply(
    cfg: &CliConfig,
    file: Option<&str>,
    data: Option<&str>,
) -> Result<(), String> {
    let yaml = if let Some(d) = data {
        d.to_string()
    } else if let Some(f) = file {
        std::fs::read_to_string(f).map_err(|e| format!("读取文件 {f}: {e}"))?
    } else {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| format!("读取 stdin: {e}"))?;
        buf
    };
    let result = post_body(cfg, "/pipelines", yaml).await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn pipeline_ls(cfg: &CliConfig) -> Result<(), String> {
    let result = get(cfg, "/pipelines").await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn pipeline_inspect(cfg: &CliConfig, name: &str) -> Result<(), String> {
    let result = get(cfg, &format!("/pipelines/{}", encode_segment(name))).await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn pipeline_delete(cfg: &CliConfig, name: &str) -> Result<(), String> {
    let result = delete(cfg, &format!("/pipelines/{}", encode_segment(name))).await?;
    cfg.print_json(&result);
    Ok(())
}

// ── Trigger ─────────────────────────────────────────────────────────────

async fn put(cfg: &CliConfig, path: &str, body: Value) -> Result<Value, String> {
    let url = api_url(&cfg.daemon, path);
    let resp = cfg
        .http_client()
        .put(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    parse_response(resp, &url).await
}

/// trigger apply：本地解析 TOML（业务侧载体）→ 校验 → PUT JSON 到 daemon。
/// daemon 只接收 JSON，从不接触 TOML。
pub async fn trigger_apply(cfg: &CliConfig, file: &str) -> Result<(), String> {
    let src = std::fs::read_to_string(file).map_err(|e| format!("读取文件 {file}: {e}"))?;
    let def: weaveflow::trigger::TriggerDef =
        toml::from_str(&src).map_err(|e| format!("TOML 解析失败 ({file}): {e}"))?;
    let errors = weaveflow::trigger::validate_trigger(&def);
    if !errors.is_empty() {
        return Err(format!("trigger 校验失败:\n  {}", errors.join("\n  ")));
    }
    let name = def.name.clone();
    let body = serde_json::to_value(&def).map_err(|e| format!("序列化 trigger 失败: {e}"))?;
    let result = put(cfg, &format!("/triggers/{}", encode_segment(&name)), body).await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn trigger_ls(cfg: &CliConfig) -> Result<(), String> {
    let result = get(cfg, "/triggers").await?;
    if cfg.is_json() {
        cfg.print_json(&result);
        return Ok(());
    }
    let list = result.as_array().ok_or("invalid response")?;
    for t in list {
        let name = t.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let ty = t.get("type").and_then(|v| v.as_str()).unwrap_or("?");
        let pipeline = t.get("pipeline").and_then(|v| v.as_str()).unwrap_or("?");
        let fired = t.get("total_fired").and_then(|v| v.as_u64()).unwrap_or(0);
        let failed = t.get("total_failed").and_then(|v| v.as_u64()).unwrap_or(0);
        let next = t.get("next_fire_at").and_then(|v| v.as_str()).unwrap_or("-");
        println!("{name}\t{ty}\t{pipeline}\tfired={fired} failed={failed}\tnext={next}");
    }
    Ok(())
}

pub async fn trigger_inspect(cfg: &CliConfig, name: &str) -> Result<(), String> {
    let result = get(cfg, &format!("/triggers/{}", encode_segment(name))).await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn trigger_delete(cfg: &CliConfig, name: &str) -> Result<(), String> {
    let result = delete(cfg, &format!("/triggers/{}", encode_segment(name))).await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn trigger_push(cfg: &CliConfig, name: &str, data: &str) -> Result<(), String> {
    let value: Value = serde_json::from_str(data).map_err(|e| format!("解析 JSON 失败: {e}"))?;
    let result = post(
        cfg,
        &format!("/triggers/{}/push", encode_segment(name)),
        value,
    )
    .await?;
    cfg.print_json(&result);
    Ok(())
}

// ── 运行 ──────────────────────────────────────────────────────────────────

fn build_run_body(name: &str, inputs: &[(String, String)]) -> Result<Value, String> {
    let mut inputs_map = serde_json::Map::new();
    for (k, v) in inputs {
        let val = resolve_input_value(v)?;
        inputs_map.insert(k.clone(), val);
    }
    Ok(serde_json::json!({
        "pipeline": name,
        "inputs": inputs_map,
    }))
}

pub async fn run_pipeline(
    cfg: &CliConfig,
    name: &str,
    inputs: &[(String, String)],
) -> Result<(), String> {
    let result = post(cfg, "/runs", build_run_body(name, inputs)?).await?;
    cfg.print_json(&result);
    Ok(())
}

fn resolve_input_value(v: &str) -> Result<Value, String> {
    if let Some(path) = v.strip_prefix('@') {
        let content = std::fs::read_to_string(path).map_err(|e| format!("读取 {path}: {e}"))?;
        serde_json::from_str(&content).map_err(|e| format!("解析 JSON 文件 {path} 失败: {e}"))
    } else {
        Ok(serde_json::from_str(v).unwrap_or(Value::String(v.to_string())))
    }
}

// ── 任务 ─────────────────────────────────────────────────────────────────

pub async fn task_ls(cfg: &CliConfig) -> Result<(), String> {
    let result = get(cfg, "/tasks").await?;
    cfg.print_json(&result);
    Ok(())
}

pub async fn snapshot_list(cfg: &CliConfig, task_id: &str) -> Result<(), String> {
    let result = get(cfg, &format!("/runs/{task_id}/snapshots")).await?;
    cfg.print_json(&result);
    Ok(())
}

/// 超过该长度且字符全部落在 base64 字母表内的字符串视为内联二进制数据。
const BASE64_MASK_MIN_LEN: usize = 512;

fn looks_like_base64(s: &str) -> bool {
    s.len() >= BASE64_MASK_MIN_LEN
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'=')
}

fn mask_base64_strings(v: &mut Value) {
    match v {
        Value::String(s) => {
            if looks_like_base64(s) {
                let b64_len = s.len();
                let padding = s.bytes().rev().take_while(|&b| b == b'=').count();
                let decoded_bytes = b64_len / 4 * 3 - padding.min(2);
                *s = format!("<base64 hidden: {decoded_bytes} bytes — use --full to show>");
            }
        }
        Value::Array(arr) => arr.iter_mut().for_each(mask_base64_strings),
        Value::Object(map) => map.values_mut().for_each(mask_base64_strings),
        _ => {}
    }
}

/// Best-effort 构建码核对：daemon 与 CLI 不一致（如改名前旧版 weave 残留）时告警。
/// 连接失败等情况静默——真正的请求会给出正式错误。
pub async fn warn_if_build_mismatch(cfg: &CliConfig) {
    let url = api_url(&cfg.daemon, "/system/version");
    let Ok(resp) = cfg.http_client().get(&url).send().await else {
        return;
    };
    if resp.status().as_u16() == 404 {
        eprintln!(
            "警告: daemon 不支持 /system/version，可能是旧版本 daemon（如改名前的 weave）残留，\
             建议 `weaveflow daemon restart`"
        );
        return;
    }
    let Ok(v) = resp.json::<Value>().await else {
        return;
    };
    let Some(daemon_code) = v.get("build_code").and_then(|x| x.as_str()) else {
        return;
    };
    if weaveflow::build_code_mismatch(weaveflow::BUILD_CODE, daemon_code) {
        eprintln!(
            "警告: daemon 构建码 ({daemon_code}) 与 CLI ({}) 不一致，可能是旧版本 daemon 残留，\
             建议 `weaveflow daemon restart`",
            weaveflow::BUILD_CODE
        );
    }
}

pub async fn snapshot_show(
    cfg: &CliConfig,
    task_id: &str,
    seq: u64,
    full: bool,
) -> Result<(), String> {
    let mut result = get(cfg, &format!("/runs/{task_id}/snapshots/{seq}")).await?;
    if !full {
        mask_base64_strings(&mut result);
    }
    cfg.print_json(&result);
    Ok(())
}

// ── 系统 ────────────────────────────────────────────────────────────────

pub async fn system_operators(cfg: &CliConfig) -> Result<(), String> {
    let result = get(cfg, "/system/operators").await?;
    if cfg.is_json() {
        cfg.print_json(&result);
        return Ok(());
    }
    let list = result.as_array().ok_or("invalid response")?;
    for op in list {
        let name = op.get("type_name").and_then(|v| v.as_str()).unwrap_or("?");
        let desc = op.get("description").and_then(|v| v.as_str()).unwrap_or("");
        let iter = op.get("iterate").and_then(|v| v.as_bool()).unwrap_or(false);
        let cache = op.get("cache").and_then(|v| v.as_bool()).unwrap_or(true);
        let iter_flag = if iter { " [iterate]" } else { "" };
        let cache_flag = if !cache { " [no-cache]" } else { "" };
        println!("{name}: {desc}{iter_flag}{cache_flag}");
    }
    Ok(())
}

pub async fn system_prune(cfg: &CliConfig, force: bool, dry_run: bool) -> Result<(), String> {
    let body = serde_json::json!({
        "force": force,
        "dry_run": dry_run,
    });
    // prune 可能全表扫描 + compact，使用独立的放宽超时（--prune-timeout）
    let url = api_url(&cfg.daemon, "/prune");
    let resp = cfg
        .prune_client()?
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    let result = parse_response(resp, &url).await?;
    if cfg.is_json() {
        cfg.print_json(&result);
        return Ok(());
    }
    let tasks_removed = result["tasks_removed"].as_u64().unwrap_or(0);
    let snapshots_removed = result["snapshots_removed"].as_u64().unwrap_or(0);
    let objects_removed = result["objects_removed"].as_u64().unwrap_or(0);
    let bytes_freed = result["bytes_freed"].as_u64().unwrap_or(0);
    if dry_run {
        println!(
            "Would remove: {tasks_removed} tasks, {snapshots_removed} snapshots, {objects_removed} objects ({bytes_freed} bytes)"
        );
    } else {
        println!(
            "Removed: {tasks_removed} tasks, {snapshots_removed} snapshots, {objects_removed} objects ({bytes_freed} bytes)"
        );
    }
    Ok(())
}

// ── Daemon 日志 ────────────────────────────────────────────────────────────

pub async fn daemon_log(cfg: &CliConfig, live: bool) -> Result<(), String> {
    use std::io::Write;

    let client = reqwest::Client::builder()
        .timeout(cfg.log_timeout)
        .connect_timeout(cfg.connect_timeout)
        .build()
        .map_err(|e| format!("构建 HTTP client 失败: {e}"))?;
    let mut offset = 0u64;
    let (scheme, host) = parse_daemon_addr(&cfg.daemon);

    loop {
        let url = format!("{scheme}{host}/system/logs?offset={offset}");
        match client.get(&url).send().await {
            Ok(resp) => {
                let new_offset_hdr = resp
                    .headers()
                    .get("X-Log-Offset")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());
                let truncated = resp
                    .headers()
                    .get("X-Log-Truncated")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s == "true" || s == "1")
                    .unwrap_or(false);
                if truncated {
                    eprintln!("[weaveflow] 日志缓冲已绕回，offset 之前的日志有缺口");
                }
                match resp.text().await {
                    Ok(body) => {
                        if !body.is_empty() {
                            print!("{body}");
                            let _ = std::io::stdout().flush();
                        }
                        if let Some(n) = new_offset_hdr {
                            offset = n;
                        }
                    }
                    Err(e) => return Err(format!("daemon log: 读取响应失败: {e}")),
                }
                if !live {
                    return Ok(());
                }
            }
            Err(e) => {
                return Err(format!("daemon log: 连接失败: {e}"));
            }
        }
        tokio::time::sleep(cfg.log_poll).await;
    }
}

// ── Watch (WS + TUI) ──────────────────────────────────────────────────────

pub async fn run_pipeline_watch(
    cfg: &CliConfig,
    name: &str,
    inputs: &[(String, String)],
    text_mode: bool,
) -> Result<(), String> {
    // 1. POST /runs → 获取 task_id + pipeline_name
    let run_resp = post(cfg, "/runs", build_run_body(name, inputs)?).await?;
    let task_id = run_resp["task_id"]
        .as_str()
        .ok_or_else(|| "响应中缺少 task_id".to_string())?
        .to_string();
    let pipeline_name = run_resp["pipeline_name"]
        .as_str()
        .unwrap_or(name)
        .to_string();

    // 2. 连接 WS
    let (http_scheme, host) = parse_daemon_addr(&cfg.daemon);
    let ws_scheme = if http_scheme == "https://" {
        "wss://"
    } else {
        "ws://"
    };
    let ws_url = format!("{ws_scheme}{host}/runs/{task_id}/ws");
    let (ws_stream, _) = tokio::time::timeout(cfg.ws_timeout, connect_async(&ws_url))
        .await
        .map_err(|_| format!("WebSocket 连接超时 ({ws_url})"))?
        .map_err(|e| format!("WebSocket 连接失败 ({ws_url}): {e}"))?;

    let (_, mut read) = ws_stream.split();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Value>();

    // 启动 reader 任务
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(val) = serde_json::from_str::<Value>(&text) {
                        let done = val
                            .get("status")
                            .and_then(|s| s.as_object())
                            .map(|o| {
                                let k = o.keys().next().map(|k| k.as_str()).unwrap_or("");
                                k == "Completed" || k == "Failed"
                            })
                            .unwrap_or(false);
                        let _ = tx.send(val);
                        if done {
                            break;
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(_) => break,
                _ => {}
            }
        }
    });

    // 3. Render：--output json 时逐条推送原始 TaskSnapshot JSON 行（面向 Agent）；
    //    否则 stdout 非 TTY 时自动回落 text 模式。
    if cfg.is_json() {
        crate::cli::watch::run_json_stream(&mut rx).await?;
        return Ok(());
    }
    let text_mode = text_mode || !std::io::stdout().is_terminal();
    if text_mode {
        crate::cli::watch::run_text(&mut rx).await?;
    } else {
        // run_tui 是同步阻塞事件循环 —— 放进 spawn_blocking，
        // 否则单核 tokio runtime 下 WS reader 永远得不到调度。
        let tid = task_id.clone();
        let pname = pipeline_name.clone();
        tokio::task::spawn_blocking(move || crate::cli::watch::run_tui(&mut rx, &tid, &pname))
            .await
            .map_err(|e| format!("TUI 任务 panic: {e}"))?
            .map_err(|e| format!("TUI 渲染失败: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_segment_ascii_passthrough() {
        assert_eq!(encode_segment("abc-DEF_123.txt~"), "abc-DEF_123.txt~");
    }

    #[test]
    fn encode_segment_chinese() {
        // "中文" 的 UTF-8: E4 B8 AD E6 96 87
        assert_eq!(encode_segment("中文"), "%E4%B8%AD%E6%96%87");
    }

    #[test]
    fn encode_segment_space() {
        assert_eq!(encode_segment("a b"), "a%20b");
    }

    #[test]
    fn encode_segment_slash() {
        assert_eq!(encode_segment("a/b"), "a%2Fb");
    }

    #[test]
    fn encode_segment_reserved_chars() {
        assert_eq!(encode_segment("?&=#%:+@"), "%3F%26%3D%23%25%3A%2B%40");
    }

    #[test]
    fn encode_segment_mixed() {
        assert_eq!(
            encode_segment("管道 v1/测试"),
            "%E7%AE%A1%E9%81%93%20v1%2F%E6%B5%8B%E8%AF%95"
        );
    }

    #[test]
    fn check_http_status_accepts_2xx() {
        for status in [200, 201, 204, 299] {
            assert!(check_http_status(status, "{}").is_ok(), "status {status}");
        }
    }

    #[test]
    fn check_http_status_rejects_error_status() {
        let body = r#"{"error":"pipeline foo not found"}"#;
        for status in [400, 404, 500] {
            let err = check_http_status(status, body).unwrap_err();
            assert!(err.starts_with(&format!("HTTP {status}:")), "err: {err}");
            assert!(err.contains("pipeline foo not found"), "err: {err}");
        }
    }

    #[test]
    fn check_http_status_trims_body() {
        let err = check_http_status(404, "  not found\n").unwrap_err();
        assert_eq!(err, "HTTP 404: not found");
    }

    #[test]
    fn resolve_input_value_rejects_broken_json_at_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.json");
        std::fs::write(&path, b"not json").expect("write");
        let arg = format!("@{}", path.display());
        let err = resolve_input_value(&arg).unwrap_err();
        assert!(err.contains("解析 JSON"), "err: {err}");
    }

    #[test]
    fn resolve_input_value_accepts_valid_json_at_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("good.json");
        std::fs::write(&path, b"{\"k\": 1}").expect("write");
        let arg = format!("@{}", path.display());
        let val = resolve_input_value(&arg).unwrap();
        assert_eq!(val, serde_json::json!({"k": 1}));
    }

    #[test]
    fn mask_base64_strings_hides_long_base64_with_byte_length() {
        // 766 个 base64 字符（含 2 个 padding）= 571 字节原始数据
        let b64 = format!("{}==", "QUJD".repeat(191));
        let mut v = serde_json::json!({"output": {"content": b64, "size": 571}});
        mask_base64_strings(&mut v);
        let masked = v["output"]["content"].as_str().unwrap();
        assert!(masked.contains("571 bytes"), "masked: {masked}");
        assert!(masked.contains("--full"), "masked: {masked}");
        assert_eq!(v["output"]["size"], serde_json::json!(571));
    }

    #[test]
    fn mask_base64_strings_recurses_into_arrays() {
        let b64 = "QUJD".repeat(200);
        let mut v = serde_json::json!([{"items": [b64]}]);
        mask_base64_strings(&mut v);
        let masked = v[0]["items"][0].as_str().unwrap();
        assert!(masked.starts_with("<base64 hidden:"), "masked: {masked}");
    }

    #[test]
    fn mask_base64_strings_keeps_short_and_non_base64_strings() {
        let long_text = "这是一段普通中文文本。".repeat(100);
        let short_b64 = "QUJD".repeat(50); // 200 chars < 512 阈值
        let mut v = serde_json::json!({"text": long_text, "short": short_b64});
        mask_base64_strings(&mut v);
        assert!(v["text"].as_str().unwrap().starts_with("这是一段"));
        assert_eq!(v["short"].as_str().unwrap().len(), 200);
    }
}
