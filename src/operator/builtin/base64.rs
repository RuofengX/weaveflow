use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use base64::Engine;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct Base64Operator;

#[async_trait]
impl Operator for Base64Operator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("base64", "Base64 编解码")
    }

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        let mode = inputs.get("mode").and_then(|v| v.as_str()).unwrap_or("encode");
        debug!(mode, "base64 operator");
        let data = inputs.get("data").unwrap_or(&Value::Null);
        let input = match data {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        match mode {
            "encode" => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(input.as_bytes());
                Ok(Value::String(encoded))
            }
            "decode" => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(input.as_bytes())
                    .map_err(|e| OperatorError::Config(format!("base64 decode: {e}")))?;
                let len = decoded.len();
                let text = String::from_utf8(decoded).map_err(|_| {
                    OperatorError::Runtime(format!(
                        "base64 decode: {len} 字节不是合法 UTF-8，无法用字符串表示"
                    ))
                })?;
                Ok(Value::String(text))
            }
            _ => Err(OperatorError::Config("mode 必须是 encode 或 decode".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn decode_invalid_utf8_returns_error() {
        let op = Base64Operator;
        let inputs = json!({ "data": "/w==", "mode": "decode" });
        let err = op.run(inputs).await.expect_err("must fail");
        let msg = err.to_string();
        assert!(msg.contains("UTF-8"), "unexpected error: {msg}");
        assert!(msg.contains('1'), "error should carry byte length: {msg}");
    }

    #[tokio::test]
    async fn decode_valid_utf8_roundtrip() {
        let op = Base64Operator;
        let inputs = json!({ "data": "aGVsbG8=", "mode": "decode" });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out, json!("hello"));
    }
}
