use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;

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
) -> WeaveResult<Vec<u8>> {
    let over_bytes = resolve_ref(scope, &cfg.over)?;

    let items: Vec<Value> = serde_json::from_slice(&over_bytes)
        .map_err(|e| WeaveError::Internal(format!("iterate parse array: {e}")))?;

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
            let data_bytes = if batched {
                serde_json::to_vec(&chunk)
            } else {
                serde_json::to_vec(&chunk[0])
            };
            let data_bytes = data_bytes
                .map_err(|e| WeaveError::Internal(format!("iterate data serialize: {e}")))?;
            let op: Box<dyn Operator> = resolve_operator(step, scope)?;
            let config = config.clone();

            batch_futures.push(async move {
                let output = op
                    .run(&data_bytes, &config)
                    .await?;
                let owned = output.into_owned();

                let v: Value = serde_json::from_slice(&owned)
                    .map_err(|e| WeaveError::Internal(format!("iterate output parse: {e}")))?;
                Ok::<_, WeaveError>((idx, v))
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

    let final_result: Vec<Value> = if batched {
        results
            .into_iter()
            .flat_map(|v| v.as_array().cloned().unwrap_or_default())
            .collect()
    } else {
        results
    };

    let final_bytes = serde_json::to_vec(&final_result)
        .map_err(|e| WeaveError::Internal(format!("iterate final serialize: {e}")))?;
    scope.set_output(&step.id, &final_bytes);

    let completed_at = chrono::Utc::now().timestamp_millis();
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

    Ok(final_bytes)
}
