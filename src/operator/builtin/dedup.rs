use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::builtin::resolve_nested;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct DedupOperator;

#[async_trait]
impl Operator for DedupOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("dedup", "按字段去重数组")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let field = config.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let value: Value = serde_json::from_slice(data)
            .map_err(|e| OperatorError::Config(format!("dedup parse: {e}")))?;
        let is_array = value.is_array();
        let arr: Vec<Value> = match value {
            Value::Array(a) => a,
            other => vec![other],
        };

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for item in arr {
            let key = if field.is_empty() {
                serde_json::to_string(&item).unwrap_or_default()
            } else {
                serde_json::to_string(resolve_nested(&item, field)).unwrap_or_default()
            };
            if seen.insert(key) {
                result.push(item);
            }
        }

        let output = if is_array {
            Value::Array(result)
        } else {
            result.into_iter().next().unwrap_or(Value::Null)
        };
        let bytes = serde_json::to_vec(&output)
            .map_err(|e| OperatorError::Runtime(format!("dedup serialize: {e}")))?;
        Ok(Cow::Owned(bytes))
    }
}
