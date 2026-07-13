use std::borrow::Cow;

use async_trait::async_trait;
use rayon::prelude::*;
use serde_json::Value;

use crate::operator::builtin::resolve_nested;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct SortOperator;

#[async_trait]
impl Operator for SortOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("sort", "按字段排序数组")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let field = config.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let order = config.get("order").and_then(|v| v.as_str()).unwrap_or("asc");

        let value: Value = serde_json::from_slice(data)
            .map_err(|e| OperatorError::Config(format!("sort parse: {e}")))?;
        let is_array = value.is_array();
        let mut arr: Vec<Value> = match value {
            Value::Array(a) => a,
            other => vec![other],
        };

        arr.par_sort_by(|a, b| {
            let va = resolve_nested(a, field);
            let vb = resolve_nested(b, field);
            let cmp = compare_values(va, vb);
            if order == "desc" { cmp.reverse() } else { cmp }
        });

        let output = if is_array {
            Value::Array(arr)
        } else {
            arr.into_iter().next().unwrap_or(Value::Null)
        };
        let bytes = serde_json::to_vec(&output)
            .map_err(|e| OperatorError::Runtime(format!("sort serialize: {e}")))?;
        Ok(Cow::Owned(bytes))
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().partial_cmp(&y.as_f64()).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}
