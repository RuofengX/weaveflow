use base64::Engine;
use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct FileOperator;

fn extension_to_mimetype(ext: &str) -> &'static str {
    match ext.to_lowercase().as_str() {
        "txt" => "text/plain",
        "md" => "text/markdown",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" => "application/javascript",
        "json" => "application/json",
        "yaml" | "yml" => "application/yaml",
        "xml" => "application/xml",
        "csv" => "text/csv",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "tar" => "application/x-tar",
        "mp3" => "audio/mpeg",
        "mp4" => "video/mp4",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "webm" => "video/webm",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        _ => "application/octet-stream",
    }
}

fn detect_mimetype(path: &str) -> &'static str {
    if let Some(ext) = std::path::Path::new(path).extension().and_then(|e| e.to_str()) {
        extension_to_mimetype(ext)
    } else {
        "application/octet-stream"
    }
}

#[async_trait]
impl Operator for FileOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("file", "读取本地文件或远程 URL，产出 JSON 对象").with_cache(false)
    }

    async fn run(
        &self,
        inputs: &Value,
    ) -> Result<Value, OperatorError> {
        // 本地路径优先
        debug!("file operator");
        if let Some(path) = inputs.get("path").and_then(|v| v.as_str()) {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|e| OperatorError::Runtime(format!("读取文件 {path}: {e}")))?;
            let mimetype = detect_mimetype(path);
            let size = bytes.len();
            let content = base64::engine::general_purpose::STANDARD.encode(&bytes);
            return Ok(serde_json::json!({
                "content": content,
                "mimetype": mimetype,
                "size": size,
            }));
        }

        // 远程 URL
        if let Some(url) = inputs.get("url").and_then(|v| v.as_str()) {
            let resp = reqwest::get(url)
                .await
                .map_err(|e| OperatorError::Runtime(format!("HTTP GET {url}: {e}")))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(OperatorError::Runtime(format!(
                    "HTTP GET {url} → {status}"
                )));
            }
            let mimetype = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| OperatorError::Runtime(format!("读取响应体 {url}: {e}")))?;
            let size = bytes.len();
            let content = base64::engine::general_purpose::STANDARD.encode(&bytes);
            return Ok(serde_json::json!({
                "content": content,
                "mimetype": mimetype,
                "size": size,
            }));
        }

        Err(OperatorError::Config(
            "file 算子需要 `path`（本地路径）或 `url`（远程地址）".into(),
        ))
    }
}
