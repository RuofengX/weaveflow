use chrono::Utc;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use tracing::info;

use crate::dsl::{IterateConfig, StepDef};
use crate::engine::step::{resolve_operator, retry_with_op};
use crate::error::{WeaveError, WeaveResult};
use crate::operator::Operator;
use crate::store::Database;
use crate::tracker::{IterateProgress, StepState, TaskId, TaskTracker};
use crate::vm::resolver::resolve_ref;
use crate::vm::Scope;

pub async fn execute_iterate(
    _db: Arc<Mutex<Database>>,
    scope: &mut Scope,
    step: &StepDef,
    inputs: &Value,
    cfg: &IterateConfig,
    task_id: &TaskId,
    tracker: &TaskTracker,
) -> WeaveResult<Value> {
    if let Some(ref batch) = cfg.batch
        && batch.size == 0
    {
        return Err(WeaveError::BadRequest(format!(
            "步骤 {} 的 iterate.batch.size 不能为 0",
            step.id
        )));
    }

    let over_ref = resolve_ref(scope, &cfg.over)?;

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

    let max_workers = cfg.max_workers.map(|n| n as usize).unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

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
                    errors: 0,
                    skip: 0,
                },
            },
        )
        .await;

    // 将 operator 解析移到循环外，避免重复分配
    let op: Arc<dyn Operator> = Arc::from(resolve_operator(step)?);

    let mut results: Vec<Value> = vec![Value::Null; total_chunks];
    let mut remaining: Vec<usize> = (0..total_chunks).collect();
    let mut done_count: u64 = 0;

    while !remaining.is_empty() {
        let batch: Vec<usize> = remaining
            .drain(..max_workers.min(remaining.len()))
            .collect();
        let mut batch_futures = Vec::new();

        for &idx in &batch {
            // 切片访问 over 数组，不克隆整个数组
            let data = if batched {
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

            let op = op.clone();
            let mut item_inputs = inputs.clone();
            if let Value::Object(ref mut map) = item_inputs {
                map.insert("data".to_string(), data);
            }

            batch_futures.push(async move {
                let output = retry_with_op(op.as_ref(), item_inputs, step).await?;
                Ok::<_, WeaveError>((idx, output))
            });
        }

        let batch_results = futures::future::join_all(batch_futures).await;
        for r in batch_results {
            let (idx, val) = r?;
            results[idx] = val;
            done_count += 1;
            if done_count.is_multiple_of(10) || done_count == total_chunks as u64 {
                tracker
                    .update_iterate(task_id, &step.id, done_count, total_chunks as u64)
                    .await;
            }
        }
    }

    let final_result: Value = if batched {
        Value::Array(
            results
                .into_iter()
                .flat_map(|v| v.as_array().cloned().unwrap_or_default())
                .collect()
        )
    } else {
        Value::Array(results)
    };

    scope.set_output(&step.id, final_result.clone());

    let completed_at = Utc::now();
    let duration_ms = (completed_at - started_at).num_milliseconds() as u64;
    info!(
        step = %step.id,
        items = total_items,
        duration_ms,
        "iterate completed"
    );
    tracker
        .update_step(
            task_id,
            &step.id,
            StepState::Completed {
                started_at,
                completed_at,
                attempts: 1,
                cached: false,
                duration_ms,
            },
        )
        .await;

    Ok(final_result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::step_op::StepOp;
    use crate::dsl::{BatchConfig, StepId, VariablePath};
    use std::collections::HashMap;

    #[tokio::test]
    async fn batch_size_zero_returns_error_not_panic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(Mutex::new(
            Database::open(dir.path().join("weave.redb")).expect("open db"),
        ));
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
            db,
            &mut scope,
            &step,
            &Value::Null,
            &cfg,
            &TaskId(uuid::Uuid::new_v4()),
            &tracker,
        )
        .await;
        let err = result.expect_err("batch.size 0 must be rejected");
        assert!(err.to_string().contains("batch.size"), "err: {err}");
    }
}
