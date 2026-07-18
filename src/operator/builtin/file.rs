use base64::Engine;
use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, warn};

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

/// 分段解析 roots：trim 后丢弃空段，返回 (有效 roots, 丢弃的空段数)。
fn split_roots(raw: &str) -> (Vec<&str>, usize) {
    let total = raw.split(':').count();
    let kept: Vec<&str> = raw
        .split(':')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let dropped = total - kept.len();
    (kept, dropped)
}

#[cfg(test)]
fn is_allowed_path_with_roots(canonical: &std::path::Path, roots: &str) -> bool {
    if roots.is_empty() {
        return true;
    }
    let (kept, _) = split_roots(roots);
    if kept.is_empty() {
        return false;
    }
    kept.iter().any(|root| canonical.starts_with(root))
}

fn is_allowed_path(canonical: &std::path::Path) -> bool {
    static WARN_NO_ROOTS: std::sync::Once = std::sync::Once::new();
    let raw = std::env::var("WEAVE_FILE_ALLOW_ROOTS").unwrap_or_default();
    if raw.is_empty() {
        WARN_NO_ROOTS.call_once(|| {
            warn!(
                "WEAVE_FILE_ALLOW_ROOTS 未配置，file 算子允许读取所有本地路径；生产环境建议配置白名单根目录"
            );
        });
        return true;
    }
    let (kept, dropped) = split_roots(&raw);
    if dropped > 0 {
        warn!(dropped, "WEAVE_FILE_ALLOW_ROOTS 含空段，已忽略");
    }
    if kept.is_empty() {
        warn!("WEAVE_FILE_ALLOW_ROOTS 已设置但过滤空段后为空（配置疑似有误），所有路径将被拒绝");
        return false;
    }
    kept.iter().any(|root| canonical.starts_with(root))
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

            if !is_allowed_path(&canonical) {
                return Err(OperatorError::Runtime(format!(
                    "file {path} is outside allowed roots"
                )));
            }

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
            let bytes = http_client::read_body_limited(resp).await?;
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
    fn is_allowed_path_with_roots_ignores_empty_segments() {
        assert!(is_allowed_path_with_roots(
            std::path::Path::new("/tmp/file.txt"),
            "/tmp::/var/data:"
        ));
        assert!(!is_allowed_path_with_roots(
            std::path::Path::new("/etc/passwd"),
            ":::"
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
