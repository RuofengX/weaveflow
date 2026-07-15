use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct HttpOperator;

#[async_trait]
impl Operator for HttpOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("http", "HTTP 请求").with_cache(false)
    }

    async fn run(
        &self,
        data: &Value,
        config: &Value,
    ) -> Result<Value, OperatorError> {
        let url = config.get("url").and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("缺少 url".into()))?;
        let method = config.get("method").and_then(|v| v.as_str()).unwrap_or("GET");

        let client = reqwest::Client::new();
        let body_bytes = if !data.is_null() {
            serde_json::to_vec(data).unwrap_or_default()
        } else {
            Vec::new()
        };

        let mut req_builder = match method.to_uppercase().as_str() {
            "GET" => client.get(url),
            "POST" => client.post(url).body(body_bytes),
            "PUT" => client.put(url).body(body_bytes),
            "DELETE" => client.delete(url),
            _ => return Err(OperatorError::Config(format!("不支持 HTTP 方法: {method}"))),
        };

        if let Some(headers) = config.get("headers").and_then(|v| v.as_object()) {
            for (k, v) in headers {
                if let Some(val) = v.as_str() {
                    req_builder = req_builder.header(k.as_str(), val);
                }
            }
        }

        let resp = req_builder.send().await
            .map_err(|e| OperatorError::Runtime(format!("HTTP: {e}")))?;

        let status = resp.status().as_u16();
        let body = resp.text().await
            .map_err(|e| OperatorError::Runtime(format!("HTTP read body: {e}")))?;

        Ok(serde_json::json!({
            "status": status,
            "body": body,
        }))
    }
}
