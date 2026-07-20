use async_trait::async_trait;
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

use crate::operator::builtin::{compare_json_numbers, resolve_nested};
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct SortOperator;

#[async_trait]
impl Operator for SortOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("sort", "按字段排序数组")
    }

    async fn run(&self, inputs: Value) -> Result<Value, OperatorError> {
        let field = inputs
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        debug!(field, "sort operator");
        let order = inputs
            .get("order")
            .and_then(|v| v.as_str())
            .unwrap_or("asc")
            .to_string();
        if !matches!(order.as_str(), "asc" | "desc") {
            return Err(OperatorError::Config(format!(
                "sort 不支持 order: {order}（可选: asc/desc）"
            )));
        }
        let data = if let Value::Object(mut m) = inputs {
            match m.remove("data") {
                Some(v) if !v.is_null() => v,
                _ => {
                    return Err(OperatorError::Config(
                        "sort 算子 inputs.data 缺失或为 null".into(),
                    ));
                }
            }
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
        (Value::Number(_), Value::Number(_)) => {
            compare_json_numbers(a, b).unwrap_or(Ordering::Equal)
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

    async fn sort(data: Value) -> Value {
        let op = SortOperator;
        op.run(json!({ "data": data })).await.expect("run")
    }

    #[tokio::test]
    async fn mixed_type_sort_is_total_and_deterministic() {
        let data = json!([
            "s", 3, null, true, [2], { "k": 1 }, false, -1, "a", [], {}, 2.5
        ]);
        let first = sort(data.clone()).await;
        let second = sort(data).await;
        assert_eq!(first, second);
        let arr = first.as_array().expect("array");
        assert_eq!(arr.len(), 12);
        assert_eq!(
            first,
            json!([null, false, true, -1, 2.5, 3, "a", "s", [2], [], { "k": 1 }, {}])
        );
    }

    #[tokio::test]
    async fn same_type_ordering_preserved() {
        let data = json!([3, 1, 2]);
        assert_eq!(sort(data).await, json!([1, 2, 3]));
        let data = json!(["b", "a"]);
        assert_eq!(sort(data).await, json!(["a", "b"]));
        let data = json!([true, false, true]);
        assert_eq!(sort(data).await, json!([false, true, true]));
    }

    #[tokio::test]
    async fn big_integer_sort_is_exact() {
        let data = json!([
            9007199254740993_i64,
            9007199254740992_i64,
            9007199254740994_i64
        ]);
        assert_eq!(
            sort(data).await,
            json!([
                9007199254740992_i64,
                9007199254740993_i64,
                9007199254740994_i64
            ])
        );
    }

    #[tokio::test]
    async fn unknown_order_returns_config_error() {
        let op = SortOperator;
        let err = op
            .run(json!({ "data": [1, 2], "order": "ascending" }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
    }

    #[tokio::test]
    async fn missing_data_returns_config_error() {
        let op = SortOperator;
        let err = op
            .run(json!({ "field": "x" }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
        let err = op
            .run(json!({ "data": null }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
    }
}
