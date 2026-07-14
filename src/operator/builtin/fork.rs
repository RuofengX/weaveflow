use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct ForkOperator;

#[async_trait]
impl Operator for ForkOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("fork", "并行多路分发，聚合结果").with_cache(false)
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let branches = config
            .get("branches")
            .and_then(|v| v.as_array())
            .ok_or_else(|| OperatorError::Config("fork 需要 branches 数组".into()))?;

        let join_mode = config
            .get("join")
            .and_then(|v| v.as_str())
            .unwrap_or("object");

        let mut futures = Vec::new();

        for (i, branch) in branches.iter().enumerate() {
            let branch_id = branch
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("branch_{i}"));

            let op_type = branch
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OperatorError::Config("branch 缺少 type".into()))?;

            let branch_inputs = branch.get("inputs").cloned().unwrap_or(Value::Null);

            let op = super::get_builtin(op_type)
                .ok_or_else(|| OperatorError::Config(format!("fork branch 未注册: {op_type}")))?;

            let data_owned = data.to_vec();

            futures.push(async move {
                let output = op
                    .run(&data_owned, &branch_inputs)
                    .await?
                    .into_owned();
                Ok::<_, OperatorError>((branch_id, output))
            });
        }

        let results = futures::future::join_all(futures).await;

        let mut collected = Vec::new();
        for result in results {
            let (branch_id, output_bytes) = result?;
            let val: Value =
                serde_json::from_slice(&output_bytes).unwrap_or_else(|_| {
                    use base64::Engine;
                    let b64 =
                        base64::engine::general_purpose::STANDARD.encode(&output_bytes);
                    serde_json::json!({
                        "_branch_binary": true,
                        "_branch_size": output_bytes.len(),
                        "_branch_base64": b64,
                    })
                });
            match join_mode {
                "array" => collected.push(val),
                _ => {
                    let mut map = serde_json::Map::new();
                    map.insert(branch_id.clone(), val);
                    collected.push(serde_json::Value::Object(map));
                }
            }
        }

        if join_mode == "array" {
            let bytes = serde_json::to_vec(&collected)
                .map_err(|e| OperatorError::Runtime(format!("fork serialize: {e}")))?;
            Ok(Cow::Owned(bytes))
        } else {
            let merged: Value = collected
                .into_iter()
                .fold(serde_json::json!({}), |mut acc, item| {
                    if let Value::Object(m) = item {
                        if let Some(obj) = acc.as_object_mut() {
                            obj.extend(m);
                        }
                    }
                    acc
                });
            let bytes = serde_json::to_vec(&merged)
                .map_err(|e| OperatorError::Runtime(format!("fork serialize: {e}")))?;
            Ok(Cow::Owned(bytes))
        }
    }
}
