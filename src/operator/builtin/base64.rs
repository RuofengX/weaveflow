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
        data: &Value,
        config: &Value,
    ) -> Result<Value, OperatorError> {
        let mode = config.get("mode").and_then(|v| v.as_str()).unwrap_or("encode");
        debug!(mode, "base64 operator");
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
                let text = String::from_utf8(decoded).unwrap_or_default();
                Ok(Value::String(text))
            }
            _ => Err(OperatorError::Config("mode 必须是 encode 或 decode".into())),
        }
    }
}
