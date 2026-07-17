use base64::Engine;
use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use super::http_client;
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

fn is_allowed_path_with_roots(canonical: &std::path::Path, roots: &str) -> bool {
    if roots.is_empty() {
        return true;
    }
    for root in roots.split(':') {
        if canonical.starts_with(root) {
            return true;
        }
    }
    false
}

fn is_allowed_path(canonical: &std::path::Path) -> bool {
    let roots = std::env::var("WEAVE_FILE_ALLOW_ROOTS").unwrap_or_default();
    is_allowed_path_with_roots(canonical, &roots)
}

#[async_trait]
impl Operator for FileOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("file", "读取本地文件或远程 URL，产出 JSON 对象").with_cache(false)
    }

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        debug!("file operator");
        if let Some(path) = inputs.get("path").and_then(|v| v.as_str()) {
            let canonical = tokio::fs::canonicalize(path)
                .await
                .map_err(|e| OperatorError::Runtime(format!("canonicalize {path}: {e}")))?;

            let metadata = tokio::fs::metadata(&canonical)
                .await
                .map_err(|e| OperatorError::Runtime(format!("stat {path}: {e}")))?;

            if !metadata.is_file() {
                return Err(OperatorError::Runtime(format!(
                    "{path} is not a regular file"
                )));
            }

            if metadata.len() > 64 * 1024 * 1024 {
                return Err(OperatorError::Runtime(format!(
                    "file {path} exceeds 64MB limit ({} bytes)",
                    metadata.len()
                )));
            }

            if !is_allowed_path(&canonical) {
                return Err(OperatorError::Runtime(format!(
                    "file {path} is outside allowed roots"
                )));
            }

            let bytes = tokio::fs::read(&canonical)
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

        if let Some(url) = inputs.get("url").and_then(|v| v.as_str()) {
            http_client::block_private_ips(url).await?;
            let resp = http_client::http_client()
                .get(url)
                .send()
                .await
                .map_err(|e| OperatorError::Runtime(format!("HTTP GET {url}: {e}")))?;

            let status = resp.status();
            if !status.is_success() {
                return Err(OperatorError::Runtime(format!(
                    "HTTP GET {url} → {status}"
                )));
            }

            http_client::check_content_length(resp.content_length()).ok_or_else(|| {
                OperatorError::Runtime("response body exceeds 64MB limit".into())
            })?;

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
            http_client::check_body_size(bytes.len())?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_allowed_path_with_roots_empty_allows_all() {
        assert!(is_allowed_path_with_roots(
            std::path::Path::new("/etc/passwd"),
            ""
        ));
    }

    #[test]
    fn is_allowed_path_with_roots_filters() {
        assert!(is_allowed_path_with_roots(
            std::path::Path::new("/tmp/file.txt"),
            "/tmp:/var/data"
        ));
        assert!(is_allowed_path_with_roots(
            std::path::Path::new("/var/data/file.txt"),
            "/tmp:/var/data"
        ));
        assert!(!is_allowed_path_with_roots(
            std::path::Path::new("/etc/passwd"),
            "/tmp:/var/data"
        ));
    }

    #[test]
    fn detect_mimetype_works() {
        assert_eq!(detect_mimetype("test.txt"), "text/plain");
        assert_eq!(detect_mimetype("test.json"), "application/json");
        assert_eq!(detect_mimetype("test.unknown"), "application/octet-stream");
        assert_eq!(detect_mimetype("noext"), "application/octet-stream");
    }
}
