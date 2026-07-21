use chrono::Utc;
use std::sync::Arc;

use serde_json::Value;
use tracing::info;

use crate::dsl::{IterateConfig, StepDef};
use crate::engine::step::{resolve_operator, retry_with_op};
use crate::error::WeaveflowError;
use crate::operator::Operator;
use crate::tracker::{IterateProgress, StepState, TaskId, TaskTracker};
use crate::vm::Scope;
use crate::vm::resolver::{resolve_ref, resolve_value_tree};

pub fn effective_max_workers(cfg: &IterateConfig) -> usize {
    cfg.max_workers
        .map(|n| (n as usize).max(1))
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
}

pub async fn execute_iterate(
    scope: &mut Scope,
    step: &StepDef,
    cfg: &IterateConfig,
    task_id: &TaskId,
    tracker: &TaskTracker,
) -> Result<(Value, u32), (WeaveflowError, u32)> {
    if let Some(ref batch) = cfg.batch
        && batch.size == 0
    {
        return Err((
            WeaveflowError::BadRequest(format!("步骤 {} 的 iterate.batch.size 不能为 0", step.id)),
            0,
        ));
    }

    // over 只解析一次并持有，分派时按 chunk 取元素/切片
    let over_ref = resolve_ref(scope, &cfg.over).map_err(|e| (e, 0))?;

    let total_items = match &*over_ref {
        Value::Array(arr) => arr.len(),
        _ => 1,
    };
    let batched = cfg.batch.is_some();
    let batch_size = cfg.batch.as_ref().map(|b| b.size as usize);

    let total_chunks: usize = if let Some(bs) = batch_size {
        (total_items + bs - 1) / bs.max(1)
    } else {
        total_items
    };

    let max_workers = effective_max_workers(cfg);

    info!(
        step = %step.id,
        items = total_items,
        batched,
        max_workers,
        total_chunks,
        "iterate started"
    );

    let started_at = Utc::now();
    tracker
        .update_step(
            task_id,
            &step.id,
            StepState::Iterating {
                started_at,
                progress: IterateProgress {
                    total: total_chunks as u64,
                    done: 0,
                },
            },
        )
        .await;

    // operator 与 op 的 JSON 序列化移到循环外，每个 step 只做一次
    let op: Arc<dyn Operator> = Arc::from(resolve_operator(step).map_err(|e| (e, 0))?);
    let op_value = serde_json::to_value(&step.op).map_err(|e| {
        (
            WeaveflowError::Internal(format!("step op serialize: {e}")),
            0,
        )
    })?;

    let mut results: Vec<Value> = vec![Value::Null; total_chunks];
    let mut remaining: Vec<usize> = (0..total_chunks).collect();
    let mut done_count: u64 = 0;
    let mut max_attempts: u32 = 1;

    while !remaining.is_empty() {
        let batch: Vec<usize> = remaining
            .drain(..max_workers.min(remaining.len()))
            .collect();
        let mut batch_futures = Vec::new();

        for &idx in &batch {
            // 切片访问 over 数组，不克隆整个数组
            let element = if batched {
                let bs = batch_size.unwrap();
                let start = idx * bs;
                let end = (start + bs).min(total_items);
                let arr: Vec<Value> = match &*over_ref {
                    Value::Array(a) => a[start..end].to_vec(),
                    _ => vec![(*over_ref).clone()],
                };
                Value::Array(arr)
            } else {
                match &*over_ref {
                    Value::Array(a) => a[idx].clone(),
                    _ => (*over_ref).clone(),
                }
            };

            // 逐 chunk 解析：as_name 绑定当前元素，{as_name...} 引用在任意
            // 算子字段真实解析（等同于注入 op 级 scope 根）。
            let item_inputs = resolve_value_tree(
                scope,
                &op_value,
                Some(cfg.as_name.as_str()),
                Some((cfg.as_name.as_str(), &element)),
                true,
                false,
            )
            .map_err(|e| (e, 0))?;

            let op = op.clone();
            batch_futures.push(async move {
                let (output, attempts) = retry_with_op(op.as_ref(), item_inputs, step)
                    .await
                    .map_err(|(e, a)| (WeaveflowError::from(e), a))?;
                Ok::<_, (WeaveflowError, u32)>((idx, output, attempts))
            });
        }

        let batch_results = futures::future::join_all(batch_futures).await;
        for r in batch_results {
            let (idx, val, attempts) = r?;
            results[idx] = val;
            max_attempts = max_attempts.max(attempts);
            done_count += 1;
            if done_count.is_multiple_of(10) || done_count == total_chunks as u64 {
                tracker
                    .update_iterate(task_id, &step.id, done_count, total_chunks as u64)
                    .await;
            }
        }
    }

    let final_result: Value = if batched {
        let mut merged = Vec::new();
        for (idx, v) in results.into_iter().enumerate() {
            match v {
                Value::Array(arr) => merged.extend(arr),
                other => {
                    return Err((
                        WeaveflowError::Internal(format!(
                            "步骤 {} 的第 {idx} 个 batch chunk 返回了非数组结果: {}",
                            step.id,
                            serde_json::to_string(&other).unwrap_or_default()
                        )),
                        max_attempts,
                    ));
                }
            }
        }
        Value::Array(merged)
    } else {
        Value::Array(results)
    };

    scope.set_output(&step.id, final_result.clone());

    let completed_at = Utc::now();
    let duration_ms = (completed_at - started_at).num_milliseconds().max(0) as u64;
    info!(
        step = %step.id,
        items = total_items,
        duration_ms,
        "iterate completed"
    );

    Ok((final_result, max_attempts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::step_op::StepOp;
    use crate::dsl::{BatchConfig, StepId, VariablePath};
    use std::collections::HashMap;

    fn iterate_cfg(max_workers: Option<u32>) -> IterateConfig {
        IterateConfig {
            over: VariablePath::parse("{slots.items}").unwrap(),
            as_name: "item".into(),
            max_workers,
            batch: None,
        }
    }

    #[test]
    fn effective_max_workers_none_uses_available_parallelism() {
        let cfg = iterate_cfg(None);
        let expected = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let n = effective_max_workers(&cfg);
        assert_eq!(n, expected);
        assert!(n > 1, "test machine should have >1 core");
    }

    #[test]
    fn effective_max_workers_zero_is_defensive_one() {
        let cfg = iterate_cfg(Some(0));
        assert_eq!(effective_max_workers(&cfg), 1);
    }

    #[test]
    fn effective_max_workers_explicit_value() {
        let cfg = iterate_cfg(Some(3));
        assert_eq!(effective_max_workers(&cfg), 3);
    }

    #[tokio::test]
    async fn batch_size_zero_returns_error_not_panic() {
        let mut slots = HashMap::new();
        slots.insert("items".to_string(), serde_json::json!([1, 2, 3]));
        let mut scope = Scope::new(slots);
        let tracker = TaskTracker::new();
        let step = StepDef {
            id: StepId::from("s"),
            after: None,
            iterate: Some(IterateConfig {
                over: VariablePath::parse("{slots.items}").unwrap(),
                as_name: "item".into(),
                max_workers: None,
                batch: Some(BatchConfig { size: 0 }),
            }),
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Noop,
        };
        let cfg = step.iterate.clone().unwrap();
        let result = execute_iterate(
            &mut scope,
            &step,
            &cfg,
            &TaskId(uuid::Uuid::new_v4()),
            &tracker,
        )
        .await;
        let (err, _attempts) = result.expect_err("batch.size 0 must be rejected");
        assert!(err.to_string().contains("batch.size"), "err: {err}");
    }

    #[tokio::test]
    async fn batched_non_array_chunk_result_returns_error() {
        let mut slots = HashMap::new();
        slots.insert("items".to_string(), serde_json::json!([1, 2]));
        let mut scope = Scope::new(slots);
        let tracker = TaskTracker::new();
        let step = StepDef {
            id: StepId::from("s"),
            after: None,
            iterate: Some(IterateConfig {
                over: VariablePath::parse("{slots.items}").unwrap(),
                as_name: "item".into(),
                max_workers: Some(1),
                batch: Some(BatchConfig { size: 1 }),
            }),
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Var(crate::dsl::step_op::VarInputs { value: None }),
        };
        let cfg = step.iterate.clone().unwrap();
        let result = execute_iterate(
            &mut scope,
            &step,
            &cfg,
            &TaskId(uuid::Uuid::new_v4()),
            &tracker,
        )
        .await;
        let (err, _attempts) = result.expect_err("non-array chunk result must be rejected");
        assert!(err.to_string().contains("非数组"), "err: {err}");
    }
}
