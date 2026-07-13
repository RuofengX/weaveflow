use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct Base64Operator;

#[async_trait]
impl Operator for Base64Operator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("base64", "Base64 编解码")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        use base64::Engine;

        // Extract string value from data (may be JSON-wrapped)
        let input = str_from_bytes(data);

        let mode = config.get("mode").and_then(|v| v.as_str()).unwrap_or("encode");
        match mode {
            "encode" => {
                let encoded = base64::engine::general_purpose::STANDARD.encode(input.as_bytes());
                let result = serde_json::to_vec(&Value::String(encoded))
                    .map_err(|e| OperatorError::Runtime(format!("base64 encode: {e}")))?;
                Ok(Cow::Owned(result))
            }
            "decode" => {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(input.as_bytes())
                    .map_err(|e| OperatorError::Config(format!("base64 decode: {e}")))?;
                let text = String::from_utf8(decoded).unwrap_or_default();
                let result = serde_json::to_vec(&Value::String(text))
                    .map_err(|e| OperatorError::Runtime(format!("base64 serialize: {e}")))?;
                Ok(Cow::Owned(result))
            }
            _ => Err(OperatorError::Config("mode 必须是 encode 或 decode".into())),
        }
    }
}

/// Try to extract a plain string from bytes that may be JSON-serialized.
fn str_from_bytes(data: &[u8]) -> String {
    if data.is_empty() {
        return String::new();
    }
    // Try parsing as JSON: if it's a JSON string, unwrap it
    if let Ok(val) = serde_json::from_slice::<Value>(data)
        && let Some(s) = val.as_str() {
            return s.to_string();
        }
    // Fallback: treat as raw string bytes
    String::from_utf8(data.to_vec()).unwrap_or_default()
}
