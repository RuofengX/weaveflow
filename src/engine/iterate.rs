use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use tracing::info;

use crate::dsl::{IterateConfig, StepDef};
use crate::engine::step::resolve_operator;
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
    config: Value,
    cfg: &IterateConfig,
    task_id: &TaskId,
    tracker: &TaskTracker,
) -> WeaveResult<Value> {
    let over_ref = resolve_ref(scope, &cfg.over)?;

    let items: Vec<Value> = match &*over_ref {
        Value::Array(arr) => arr.clone(),
        other => vec![other.clone()],
    };

    let total_items = items.len();
    let batched = cfg.batch.is_some();
    let batch_size = cfg.batch.as_ref().map(|b| b.size as usize);

    let chunks: Vec<Vec<Value>> = if let Some(bs) = batch_size {
        items.chunks(bs).map(|c| c.to_vec()).collect()
    } else {
        items.into_iter().map(|item| vec![item]).collect()
    };

    let total_chunks = chunks.len() as u64;
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

    let started_at = chrono::Utc::now().timestamp_millis();
    tracker
        .update_step(
            task_id,
            &step.id,
            StepState::Iterating {
                started_at,
                progress: IterateProgress {
                    total: total_chunks,
                    done: 0,
                    errors: 0,
                    skip: 0,
                },
            },
        )
        .await;

    let mut results: Vec<Value> = vec![Value::Null; total_chunks as usize];
    let mut remaining: Vec<_> = chunks.into_iter().enumerate().collect();
    let mut done_count: u64 = 0;

    while !remaining.is_empty() {
        let batch: Vec<_> = remaining
            .drain(..max_workers.min(remaining.len()))
            .collect();
        let mut batch_futures = Vec::new();

        for (idx, chunk) in batch {
            let data = if batched {
                Value::Array(chunk)
            } else {
                chunk.into_iter().next().unwrap_or(Value::Null)
            };
            let op: Box<dyn Operator> = resolve_operator(step, scope)?;
            let config = config.clone();

            batch_futures.push(async move {
                let output = op
                    .run(&data, &config)
                    .await?;
                Ok::<_, WeaveError>((idx, output))
            });
        }

        let batch_results = futures::future::join_all(batch_futures).await;
        for r in batch_results {
            let (idx, val) = r?;
            results[idx] = val;
            done_count += 1;
            if done_count.is_multiple_of(10) || done_count == total_chunks {
                tracker
                    .update_iterate(task_id, &step.id, done_count, total_chunks)
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

    let completed_at = chrono::Utc::now().timestamp_millis();
    let duration_ms = (completed_at - started_at) as u64;
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
                duration_ms: (completed_at - started_at) as u64,
            },
        )
        .await;

    Ok(final_result)
}
