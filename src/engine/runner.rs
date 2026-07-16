use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::dsl::{PipelineDef, RefValue, StepId};
use crate::engine::dag::Dag;
use crate::engine::step::execute_step;
use crate::error::{WeaveError, WeaveResult};
use crate::store::Database;
use crate::tracker::{Snapshot, StepState, TaskId, TaskTracker};
use crate::vm::Scope;

pub struct Runner {
    pub pipeline: PipelineDef,
    pub db: Arc<Mutex<Database>>,
    pub tracker: TaskTracker,
}

impl Runner {
    pub fn new(pipeline: PipelineDef, db: Arc<Mutex<Database>>, tracker: TaskTracker) -> Self {
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
    ) -> WeaveResult<Vec<u8>> {
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
        }
        result
    }
}

pub async fn run_inner(
    pipeline: &PipelineDef,
    db: Arc<Mutex<Database>>,
    tracker: &TaskTracker,
    task_id: TaskId,
    slots: HashMap<String, Value>,
) -> WeaveResult<Vec<u8>> {
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
                        let msgs: Vec<String> = errors
                            .map(|e| e.to_string())
                            .collect();
                        return Err(WeaveError::BadRequest(format!(
                            "slot '{}' validation failed: {}",
                            slot_def.name,
                            msgs.join("; ")
                        )));
                    }
                }
                Err(e) => {
                    return Err(WeaveError::Internal(format!(
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

    for step_id in layers.iter().flatten(){
        tracker
            .update_step(&task_id, step_id, StepState::Pending)
            .await;
    }

    let last_step_id = layers.last().and_then(|l| l.last()).cloned();

    for layer in layers.iter() {
        let mut futures = Vec::new();
        debug!(layer_steps = ?layer, "executing parallel layer");
        for step_id in layer {
            let sd = dag
                .step(step_id)
                .ok_or_else(|| {
                    WeaveError::Internal(format!("step {step_id} not found in DAG"))
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
        let results: Vec<(StepId, DateTime<Utc>, WeaveResult<Value>)> =
            futures::future::join_all(futures).await;

        let mut layer_failed = false;
        let mut first_error = String::new();

        for (step_id, started_at, result) in results {
            match result {
                Ok(output) => {
                    let completed_at = Utc::now();
                    let duration_ms = (completed_at - started_at).num_milliseconds() as u64;
                    debug!(step = %step_id, duration_ms, "parallel step completed");
                    scope.set_output(&step_id, output.clone());
                    {
                        let db_lock = db.lock().await;
                        save_step_snapshot(
                            &db_lock,
                            &task_id,
                            &step_id,
                            &output,
                            Some(&step_id) == last_step_id.as_ref(),
                        );
                    }
                    let completed_at = Utc::now();
                    let duration_ms = (completed_at - started_at).num_milliseconds() as u64;
                    tracker
                        .update_step(
                            &task_id,
                            &step_id,
                            StepState::Completed {
                                started_at,
                                completed_at,
                                attempts: 1,
                                cached: false,
                                duration_ms,
                            },
                        )
                        .await;
                }
                Err(e) => {
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
                                attempts: 1,
                            },
                        )
                        .await;
                }
            }
        }

        if layer_failed {
            return Err(WeaveError::Internal(first_error));
        }
    }

    let final_output;
    let output_val: Value;
    {
        match &pipeline.output {
            RefValue::Literal(v) => {
                output_val = v.clone();
                final_output = serde_json::to_vec(&output_val)
                    .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?;
            }
            RefValue::Ref(path) => {
                if path.parts.is_empty() {
                    return Err(WeaveError::Internal("empty output ref".into()));
                }
                let step_id = StepId::from(path.parts[0].clone());
                let value = scope.get_output(&step_id).ok_or_else(|| {
                    WeaveError::Internal(format!("output step {step_id} not found"))
                })?;

                if path.parts.len() <= 2 {
                    output_val = (*value).clone();
                    final_output = serde_json::to_vec(&output_val)
                        .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?;
                } else {
                    let mut current = &*value;
                    let start = if path.parts.len() >= 2 && path.parts[1] == "output" {
                        2
                    } else {
                        1
                    };
                    for part in &path.parts[start..] {
                        current = current.get(part).unwrap_or(&Value::Null);
                    }
                    output_val = current.clone();
                    final_output = serde_json::to_vec(&output_val)
                        .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?;
                }
            }
        }
    }

    info!(task_id = %task_id, pipeline = %pipeline.name, "pipeline run completed");
    tracker.complete(&task_id, output_val).await;

    Ok(final_output)
}

fn save_step_snapshot(
    db: &Database,
    task_id: &TaskId,
    step_id: &StepId,
    output: &Value,
    is_last: bool,
) {
    let bytes = serde_json::to_vec(output).unwrap_or_default();
    let snap = Snapshot {
        seq: 0,
        step_id: step_id.clone(),
        output: bytes,
    };
    let _ = if is_last {
        db.save_snapshot_durable(task_id, snap)
    } else {
        db.save_snapshot(task_id, snap)
    };
}
