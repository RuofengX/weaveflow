use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, error, info, warn};

use crate::dsl::{PipelineDef, StepId};
use crate::engine::dag::Dag;
use crate::engine::step::execute_step;
use crate::error::{WeaveflowError, WeaveflowResult};
use crate::store::Database;
use crate::tracker::{Snapshot, StepState, TaskId, TaskTracker};
use crate::vm::{Scope, redact_env_values, resolve_value_tree};

pub struct Runner {
    pub pipeline: PipelineDef,
    pub db: Arc<Database>,
    pub tracker: TaskTracker,
}

impl Runner {
    pub fn new(pipeline: PipelineDef, db: Arc<Database>, tracker: TaskTracker) -> Self {
        Runner {
            pipeline,
            db,
            tracker,
        }
    }

    pub async fn run(
        &self,
        task_id: TaskId,
        slots: HashMap<String, Value>,
    ) -> WeaveflowResult<Vec<u8>> {
        let result = run_inner(
            &self.pipeline,
            self.db.clone(),
            &self.tracker,
            task_id,
            slots,
        )
        .await;
        if let Err(ref e) = result {
            self.tracker.fail(&task_id, e.to_string()).await;
            if let Err(db_err) = self
                .db
                .set_task_status(&task_id, crate::tracker::meta::TASK_STATUS_FAILED)
            {
                warn!(task_id = %task_id, error = %db_err, "set_task_status(failed) failed");
            }
        }
        result
    }
}

pub async fn run_inner(
    pipeline: &PipelineDef,
    db: Arc<Database>,
    tracker: &TaskTracker,
    task_id: TaskId,
    slots: HashMap<String, Value>,
) -> WeaveflowResult<Vec<u8>> {
    let mut slots = slots;

    info!(
        pipeline = %pipeline.name,
        task_id = %task_id,
        steps = pipeline.steps.len(),
        slots_count = pipeline.slots.len(),
        "pipeline run started"
    );

    for slot_def in &pipeline.slots {
        if !slots.contains_key(&slot_def.name) {
            match slot_def.schema.get("default") {
                Some(default_val) => {
                    debug!(
                        "slot '{}' missing, applying default: {}",
                        slot_def.name,
                        serde_json::to_string(default_val).unwrap_or_default()
                    );
                    slots.insert(slot_def.name.clone(), default_val.clone());
                }
                None => {
                    warn!(
                        "slot '{}' has no value and no default in schema",
                        slot_def.name
                    );
                }
            }
        } else {
            debug!("slot '{}' has user-provided value", slot_def.name);
        }
    }

    for slot_def in &pipeline.slots {
        if let Some(val) = slots.get(&slot_def.name) {
            match jsonschema::compile(&slot_def.schema) {
                Ok(schema) => {
                    if let Err(errors) = schema.validate(val) {
                        let msgs: Vec<String> = errors.map(|e| e.to_string()).collect();
                        return Err(WeaveflowError::BadRequest(format!(
                            "slot '{}' validation failed: {}",
                            slot_def.name,
                            msgs.join("; ")
                        )));
                    }
                }
                Err(e) => {
                    return Err(WeaveflowError::Internal(format!(
                        "slot '{}' schema compile error: {}",
                        slot_def.name, e
                    )));
                }
            }
        }
    }

    let mut scope = Scope::new(slots);

    let dag = Dag::from_pipeline(pipeline)?;
    let layers = dag.topological_sort()?;

    for step_id in layers.iter().flatten() {
        tracker
            .update_step(&task_id, step_id, StepState::Pending)
            .await;
    }

    let last_step_id = single_output_ref(&pipeline.output)
        .filter(|id| dag.step(id).is_some())
        .or_else(|| layers.last().and_then(|l| l.last()).cloned());

    for (layer_idx, layer) in layers.iter().enumerate() {
        let mut futures = Vec::new();
        debug!(layer_steps = ?layer, "executing parallel layer");
        for step_id in layer {
            let sd = dag
                .step(step_id)
                .ok_or_else(|| {
                    WeaveflowError::Internal(format!("step {step_id} not found in DAG"))
                })?
                .clone();
            let mut sc = scope.clone();
            let db_clone = db.clone();
            let tracker_clone = tracker.clone();
            let tid = task_id;
            let sid = step_id.clone();

            futures.push(async move {
                let started_at = Utc::now();
                tracker_clone
                    .update_step(
                        &tid,
                        &sid,
                        StepState::Running {
                            started_at,
                            attempts: 1,
                        },
                    )
                    .await;

                let out = execute_step(db_clone, &mut sc, &sd, &tid, &tracker_clone).await;
                (sd.id.clone(), started_at, out)
            });
        }

        // 这里等待层中所有步骤完成后再处理错误，提高可观测性
        type StepOutcome = (
            StepId,
            DateTime<Utc>,
            Result<(Value, u32, bool), (WeaveflowError, u32)>,
        );
        let results: Vec<StepOutcome> = futures::future::join_all(futures).await;

        let mut layer_failed = false;
        let mut first_error = String::new();

        for (step_id, started_at, result) in results {
            match result {
                Ok((output, attempts_used, cache_hit)) => {
                    let completed_at = Utc::now();
                    let duration_ms = (completed_at - started_at).num_milliseconds().max(0) as u64;
                    debug!(step = %step_id, duration_ms, "parallel step completed");
                    scope.set_output(&step_id, output.clone());
                    save_step_snapshot(
                        &db,
                        &task_id,
                        &step_id,
                        &output,
                        Some(&step_id) == last_step_id.as_ref(),
                        &scope,
                    );
                    tracker
                        .update_step(
                            &task_id,
                            &step_id,
                            StepState::Completed {
                                started_at,
                                completed_at,
                                attempts: attempts_used,
                                cached: cache_hit,
                                duration_ms,
                            },
                        )
                        .await;
                }
                Err((e, attempts)) => {
                    error!(step = %step_id, error = %e, "parallel step failed");
                    if !layer_failed {
                        first_error = format!("step {step_id} failed: {e}");
                        layer_failed = true;
                    }
                    let now = Utc::now();
                    tracker
                        .update_step(
                            &task_id,
                            &step_id,
                            StepState::Failed {
                                started_at: Some(started_at),
                                completed_at: now,
                                error: e.to_string(),
                                attempts,
                            },
                        )
                        .await;
                }
            }
        }

        if layer_failed {
            for remaining_layer in layers.iter().skip(layer_idx + 1) {
                for step_id in remaining_layer {
                    tracker
                        .update_step(&task_id, step_id, StepState::Skipped)
                        .await;
                }
            }
            return Err(WeaveflowError::Internal(first_error));
        }
    }

    let final_output;
    let output_val: Value;
    {
        // output 是任意 JSON + 内联 Ref 标签；in_literal=true 避免把用户数据里的
        // 单键 "Literal" 对象误当 RefValue serde 标签拆包（该约定只属于算子字段位）。
        output_val = resolve_value_tree(&scope, &pipeline.output, None, false, true)?;
        final_output = serde_json::to_vec(&output_val)
            .map_err(|e| WeaveflowError::Internal(format!("output serialize: {e}")))?;
    }

    info!(task_id = %task_id, pipeline = %pipeline.name, "pipeline run completed");
    tracker.complete(&task_id, output_val).await;
    if let Err(db_err) = db.set_task_status(&task_id, crate::tracker::meta::TASK_STATUS_COMPLETED) {
        warn!(task_id = %task_id, error = %db_err, "set_task_status(completed) failed");
    }

    Ok(final_output)
}

/// output 恰好是单个内联 Ref 标签时，取其首段作为“末步”候选（用于 durable 快照判定）。
fn single_output_ref(output: &Value) -> Option<StepId> {
    let map = output.as_object()?;
    if map.len() != 1 || !map.contains_key("Ref") {
        return None;
    }
    let path: crate::dsl::VariablePath = serde_json::from_value(map.get("Ref")?.clone()).ok()?;
    path.parts.first().map(|p| StepId::from(p.clone()))
}

fn save_step_snapshot(
    db: &Database,
    task_id: &TaskId,
    step_id: &StepId,
    output: &Value,
    is_last: bool,
    scope: &Scope,
) {
    let secrets = scope.env_values();
    let serialized = if secrets.is_empty() {
        serde_json::to_vec(output)
    } else {
        let mut redacted = output.clone();
        redact_env_values(&mut redacted, &secrets);
        serde_json::to_vec(&redacted)
    };
    let bytes = match serialized {
        Ok(b) => b,
        Err(e) => {
            warn!(task_id = %task_id, step = %step_id, error = %e, "snapshot serialize failed; skipping save");
            return;
        }
    };
    let snap = Snapshot {
        seq: 0,
        step_id: step_id.clone(),
        output: bytes,
    };
    let result = if is_last {
        db.save_snapshot_durable(task_id, snap)
    } else {
        db.save_snapshot(task_id, snap)
    };
    if let Err(e) = result {
        warn!(task_id = %task_id, step = %step_id, error = %e, "snapshot save failed");
    }
}
