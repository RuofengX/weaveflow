use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, warn};

use super::http_client;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct HttpOperator;

#[async_trait]
impl Operator for HttpOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("http", "HTTP 请求").with_cache(false)
    }

    async fn run(&self, inputs: Value) -> Result<Value, OperatorError> {
        let url = inputs
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("缺少 url".into()))?;
        let method = inputs
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("GET");
        debug!(url = %url, method, "http request");

        let client = http_client::http_client();

        let body_data = inputs.get("body");
        let body_bytes = match body_data {
            Some(Value::String(s)) => s.clone().into_bytes(),
            Some(v) if !v.is_null() => serde_json::to_vec(v).unwrap_or_default(),
            _ => Vec::new(),
        };

        http_client::block_private_ips(url).await?;

        let method_upper = method.to_uppercase();
        if matches!(method_upper.as_str(), "GET" | "DELETE") && !body_bytes.is_empty() {
            warn!(method = %method_upper, "http 算子 GET/DELETE 请求的 body 被忽略");
        }

        let mut req_builder = match method_upper.as_str() {
            "GET" => client.get(url),
            "POST" => client.post(url).body(body_bytes),
            "PUT" => client.put(url).body(body_bytes),
            "DELETE" => client.delete(url),
            _ => return Err(OperatorError::Config(format!("不支持 HTTP 方法: {method}"))),
        };

        if let Some(headers) = inputs.get("headers").and_then(|v| v.as_object()) {
            for (k, v) in headers {
                match v.as_str() {
                    Some(val) => req_builder = req_builder.header(k.as_str(), val),
                    None => warn!(header = %k, "http 算子 header 值非字符串，已跳过"),
                }
            }
        }

        let resp = req_builder
            .send()
            .await
            .map_err(|e| OperatorError::Runtime(format!("HTTP: {e}")))?;

        http_client::check_content_length(resp.content_length())
            .ok_or_else(|| OperatorError::Runtime("response body exceeds 64MB limit".into()))?;

        let status = resp.status().as_u16();
        let body_bytes = http_client::read_body_limited(resp).await?;
        let body = String::from_utf8_lossy(&body_bytes).into_owned();

        Ok(serde_json::json!({
            "status": status,
            "body": body,
        }))
    }
}
