use async_trait::async_trait;
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

use crate::operator::builtin::resolve_nested;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct SortOperator;

#[async_trait]
impl Operator for SortOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("sort", "按字段排序数组")
    }

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        let field = inputs.get("field").and_then(|v| v.as_str()).unwrap_or("").to_string();
        debug!(field, "sort operator");
        let order = inputs.get("order").and_then(|v| v.as_str()).unwrap_or("asc").to_string();
        let data = if let Value::Object(mut m) = inputs {
            m.remove("data").unwrap_or(Value::Null)
        } else {
            inputs
        };

        let is_array = data.is_array();
        let mut arr: Vec<Value> = match data {
            Value::Array(a) => a,
            other => vec![other],
        };

        arr.par_sort_by(|a, b| {
            let va = resolve_nested(a, &field);
            let vb = resolve_nested(b, &field);
            let cmp = compare_values(va, vb);
            if order == "desc" { cmp.reverse() } else { cmp }
        });

        let output = if is_array {
            Value::Array(arr)
        } else {
            arr.into_iter().next().unwrap_or(Value::Null)
        };
        Ok(output)
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let ra = type_rank(a);
    let rb = type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().partial_cmp(&y.as_f64()).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(_), Value::Array(_)) | (Value::Object(_), Value::Object(_)) => {
            let sa = serde_json::to_string(a).unwrap_or_default();
            let sb = serde_json::to_string(b).unwrap_or_default();
            sa.cmp(&sb)
        }
        _ => Ordering::Equal,
    }
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sort(data: Value) -> Value {
        let op = SortOperator;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(op.run(json!({ "data": data }))).expect("run")
    }

    #[test]
    fn mixed_type_sort_is_total_and_deterministic() {
        let data = json!([
            "s", 3, null, true, [2], { "k": 1 }, false, -1, "a", [], {}, 2.5
        ]);
        let first = sort(data.clone());
        let second = sort(data);
        assert_eq!(first, second);
        let arr = first.as_array().expect("array");
        assert_eq!(arr.len(), 12);
        assert_eq!(
            first,
            json!([null, false, true, -1, 2.5, 3, "a", "s", [2], [], { "k": 1 }, {}])
        );
    }

    #[test]
    fn same_type_ordering_preserved() {
        let data = json!([3, 1, 2]);
        assert_eq!(sort(data), json!([1, 2, 3]));
        let data = json!(["b", "a"]);
        assert_eq!(sort(data), json!(["a", "b"]));
        let data = json!([true, false, true]);
        assert_eq!(sort(data), json!([false, true, true]));
    }
}
