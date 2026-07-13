use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// 文件读取算子。读本地文件或远程 URL，产出原始二进制 bytes。
///
/// 与 `http` 算子的区分：
/// - `http`：REST API 交互，返回 `{status, body}` JSON
/// - `file`：纯粹二进制获取，返回原始 bytes
pub struct FileOperator;

#[async_trait]
impl Operator for FileOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("file", "读取本地文件或远程 URL，产出原始 bytes").with_cache(false)
    }

    async fn run<'a>(
        &self,
        _data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        // 本地路径优先
        if let Some(path) = config.get("path").and_then(|v| v.as_str()) {
            let bytes = tokio::fs::read(path)
                .await
                .map_err(|e| OperatorError::Runtime(format!("读取文件 {path}: {e}")))?;
            return Ok(Cow::Owned(bytes));
        }

        // 远程 URL
        if let Some(url) = config.get("url").and_then(|v| v.as_str()) {
            let resp = reqwest::get(url)
                .await
                .map_err(|e| OperatorError::Runtime(format!("HTTP GET {url}: {e}")))?;
            let status = resp.status();
            if !status.is_success() {
                return Err(OperatorError::Runtime(format!(
                    "HTTP GET {url} → {status}"
                )));
            }
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| OperatorError::Runtime(format!("读取响应体 {url}: {e}")))?;
            return Ok(Cow::Owned(bytes.to_vec()));
        }

        Err(OperatorError::Config(
            "file 算子需要 `path`（本地路径）或 `url`（远程地址）".into(),
        ))
    }
}
