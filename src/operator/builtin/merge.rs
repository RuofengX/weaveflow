use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct MergeOperator;

#[async_trait]
impl Operator for MergeOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("merge", "合并两个对象")
    }

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        debug!("merge operator");
        let a = inputs.get("a").cloned()
            .or_else(|| inputs.get("data").cloned());
        let a = match a {
            Some(v) if !v.is_null() => v,
            _ => Value::Null,
        };

        let b = inputs.get("b").cloned()
            .ok_or_else(|| OperatorError::Config("缺少 b 字段".into()))?;
        let deep = inputs.get("deep").and_then(|v| v.as_bool()).unwrap_or(false);

        match (a.as_object(), b.as_object()) {
            (Some(oa), Some(ob)) => {
                let mut merged = oa.clone();
                for (k, v) in ob {
                    if deep {
                        match merged.get_mut(k) {
                            Some(existing) => deep_merge(existing, v),
                            None => {
                                merged.insert(k.clone(), v.clone());
                            }
                        }
                    } else {
                        merged.insert(k.clone(), v.clone());
                    }
                }
                Ok(Value::Object(merged))
            }
            _ => Err(OperatorError::Config("a 和 b 必须是对象".into())),
        }
    }
}

fn deep_merge(a: &mut Value, b: &Value) {
    if let (Value::Object(ma), Value::Object(mb)) = (&mut *a, b) {
        for (k, vb) in mb {
            match ma.get_mut(k) {
                Some(va) => deep_merge(va, vb),
                None => {
                    ma.insert(k.clone(), vb.clone());
                }
            }
        }
    } else {
        *a = b.clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn shallow_merge_overwrites_nested_by_default() {
        let op = MergeOperator;
        let inputs = json!({
            "a": { "cfg": { "x": 1, "y": 2 } },
            "b": { "cfg": { "y": 3 } }
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out["cfg"], json!({ "y": 3 }));
    }

    #[tokio::test]
    async fn deep_merge_recurses_into_objects() {
        let op = MergeOperator;
        let inputs = json!({
            "a": { "cfg": { "x": 1, "y": 2 }, "name": "weave" },
            "b": { "cfg": { "y": 3, "z": 4 } },
            "deep": true
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out["cfg"], json!({ "x": 1, "y": 3, "z": 4 }));
        assert_eq!(out["name"], json!("weave"));
    }

    #[tokio::test]
    async fn deep_merge_b_overwrites_arrays_and_scalars() {
        let op = MergeOperator;
        let inputs = json!({
            "a": { "list": [1, 2, 3], "n": 1 },
            "b": { "list": [9], "n": 2 },
            "deep": true
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out["list"], json!([9]));
        assert_eq!(out["n"], json!(2));
    }
}
