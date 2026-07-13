use futures::StreamExt;
use serde_json::Value;
use tokio_tungstenite::connect_async;
use tungstenite::Message;

fn api_url(daemon: &str, path: &str) -> String {
    format!("http://{daemon}{path}")
}

fn daemon_error(url: &str, e: impl std::fmt::Display) -> String {
    format!("无法连接 daemon ({url}): {e}")
}

async fn get(daemon: &str, path: &str) -> Result<Value, String> {
    let url = api_url(daemon, path);
    let resp = reqwest::get(&url)
        .await
        .map_err(|e| daemon_error(&url, e))?;
    resp.json().await.map_err(|e| daemon_error(&url, e))
}

async fn post(daemon: &str, path: &str, body: Value) -> Result<Value, String> {
    let url = api_url(daemon, path);
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    resp.json().await.map_err(|e| daemon_error(&url, e))
}

async fn post_body(daemon: &str, path: &str, body: String) -> Result<Value, String> {
    let url = api_url(daemon, path);
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("content-type", "text/plain")
        .body(body)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    resp.json().await.map_err(|e| daemon_error(&url, e))
}

// ── Pipeline ──────────────────────────────────────────────────────────────

pub async fn pipeline_apply(daemon: &str, file: Option<&str>, data: Option<&str>) -> Result<(), String> {
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
    let result = post_body(daemon, "/pipelines", yaml).await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

pub async fn pipeline_ls(daemon: &str) -> Result<(), String> {
    let result = get(daemon, "/pipelines").await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

pub async fn pipeline_inspect(daemon: &str, name: &str) -> Result<(), String> {
    let result = get(daemon, &format!("/pipelines/{name}")).await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

pub async fn pipeline_delete(daemon: &str, name: &str) -> Result<(), String> {
    let result = delete(daemon, &format!("/pipelines/{name}")).await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

// ── Run ──────────────────────────────────────────────────────────────────

pub async fn run_pipeline(daemon: &str, name: &str, inputs: &[(String, String)]) -> Result<(), String> {
    let mut inputs_map = serde_json::Map::new();
    for (k, v) in inputs {
        let val = resolve_input_value(v)?;
        inputs_map.insert(k.clone(), val);
    }
    let body = serde_json::json!({
        "pipeline": name,
        "inputs": inputs_map,
    });
    let result = post(daemon, "/runs", body).await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

fn resolve_input_value(v: &str) -> Result<Value, String> {
    if let Some(path) = v.strip_prefix('@') {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("读取 {path}: {e}"))?;
        Ok(serde_json::from_str(&content)
            .unwrap_or(Value::String(content)))
    } else {
        Ok(serde_json::from_str(v).unwrap_or(Value::String(v.to_string())))
    }
}

// ── Task ─────────────────────────────────────────────────────────────────

pub async fn task_ls(daemon: &str) -> Result<(), String> {
    let result = get(daemon, "/tasks").await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

pub async fn snapshot_list(daemon: &str, task_id: &str) -> Result<(), String> {
    let result = get(daemon, &format!("/runs/{task_id}/snapshots")).await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

pub async fn snapshot_show(daemon: &str, task_id: &str, seq: u64) -> Result<(), String> {
    let result = get(daemon, &format!("/runs/{task_id}/snapshots/{seq}")).await?;
    println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
    Ok(())
}

// ── System ────────────────────────────────────────────────────────────────

pub async fn system_operators(daemon: &str) -> Result<(), String> {
    let result = get(daemon, "/system/operators").await?;
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

pub async fn system_prune(daemon: &str, force: bool, dry_run: bool) -> Result<(), String> {
    let body = serde_json::json!({
        "force": force,
        "dry_run": dry_run,
    });
    let result = post(daemon, "/prune", body).await?;
    let tasks_removed = result["tasks_removed"].as_u64().unwrap_or(0);
    let objects_removed = result["objects_removed"].as_u64().unwrap_or(0);
    let bytes_freed = result["bytes_freed"].as_u64().unwrap_or(0);
    if dry_run {
        println!("Would remove: {tasks_removed} tasks, {objects_removed} objects ({bytes_freed} bytes)");
    } else {
        println!("Removed: {tasks_removed} tasks, {objects_removed} objects ({bytes_freed} bytes)");
    }
    Ok(())
}

async fn delete(daemon: &str, path: &str) -> Result<Value, String> {
    let url = format!("http://{daemon}{path}");
    let resp = reqwest::Client::new()
        .delete(&url)
        .send()
        .await
        .map_err(|e| daemon_error(&url, e))?;
    resp.json().await.map_err(|e| daemon_error(&url, e))
}

// ── Watch (WS + TUI) ──────────────────────────────────────────────────────

pub async fn run_pipeline_watch(
    daemon: &str,
    name: &str,
    inputs: &[(String, String)],
    text_mode: bool,
) -> Result<(), String> {
    // 1. POST /runs → get task_id + pipeline_name
    let mut inputs_map = serde_json::Map::new();
    for (k, v) in inputs {
        let val = resolve_input_value(v)?;
        inputs_map.insert(k.clone(), val);
    }
    let body = serde_json::json!({
        "pipeline": name,
        "inputs": inputs_map,
    });
    let run_resp = post(daemon, "/runs", body).await?;
    let task_id = run_resp["task_id"]
        .as_str()
        .ok_or_else(|| "响应中缺少 task_id".to_string())?
        .to_string();
    let pipeline_name = run_resp["pipeline_name"]
        .as_str()
        .unwrap_or(name)
        .to_string();

    // 2. Connect WS
    let ws_url = format!(
        "ws://{}/runs/{task_id}/ws",
        daemon.trim_start_matches("http://")
    );
    let (ws_stream, _) = connect_async(&ws_url)
        .await
        .map_err(|e| format!("WebSocket 连接失败 ({ws_url}): {e}"))?;

    let (_, mut read) = ws_stream.split();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Value>();

    // Spawn reader
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

    // 3. Render
    if text_mode {
        crate::cli::watch::run_text(&mut rx).await;
    } else {
        crate::cli::watch::run_tui(&mut rx, &task_id, &pipeline_name)
            .map_err(|e| format!("TUI 渲染失败: {e}"))?;
    }
    Ok(())
}
