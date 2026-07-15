use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::dsl::{PipelineDef, RefValue};
use crate::engine::dag::Dag;
use crate::engine::step::{execute_step, execute_step_static};
use crate::error::{WeaveError, WeaveResult};
use crate::store::Database;
use crate::tracker::{Snapshot, StepState, TaskId, TaskTracker};
use crate::vm::Scope;

pub struct Runner {
    pub pipeline: PipelineDef,
    pub db: Arc<Mutex<Database>>,
    pub tracker: Arc<TaskTracker>,
}

impl Runner {
    pub fn new(pipeline: PipelineDef, db: Arc<Mutex<Database>>, tracker: Arc<TaskTracker>) -> Self {
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
    tracker: &Arc<TaskTracker>,
    task_id: TaskId,
    slots: HashMap<String, Value>,
) -> WeaveResult<Vec<u8>> {
    let mut slots = slots;
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
                        return Err(WeaveError::Internal(format!(
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

    // Rules validation
    for rule in &pipeline.rules {
        let op = crate::engine::step::resolve_rule_operator(rule, None)?;
        let rule_config = rule
            .inputs
            .as_ref()
            .map(|m| {
                let mut obj = serde_json::Map::new();
                for (k, v) in m {
                    obj.insert(k.clone(), v.to_value());
                }
                Value::Object(obj)
            })
            .unwrap_or(Value::Null);
        let result = op
            .run(&Value::Null, &rule_config)
            .await
            .map_err(|e| WeaveError::Internal(format!("rule '{}' execution error: {e}", rule.id)))?;
        if let Some(obj) = result.as_object()
            && !obj.get("valid").and_then(|v| v.as_bool()).unwrap_or(true)
        {
            let err_msg = obj.get("error").and_then(|v| v.as_str()).unwrap_or("validation failed");
            let hint = obj.get("hint").and_then(|v| v.as_str()).unwrap_or("");
            return Err(WeaveError::Internal(format!(
                "rule '{}' failed: {err_msg}{}", if hint.is_empty() { "" } else { ": " },
                if hint.is_empty() { "" } else { hint }
            )));
        }
    }

    let mut scope = Scope::new(slots);

    let dag = Dag::from_pipeline(pipeline)?;
    let layers = dag.topological_sort()?;

    let all_step_ids: Vec<String> = layers.iter().flatten().cloned().collect();

    for step_id in &all_step_ids {
        tracker
            .update_step(&task_id, step_id, StepState::Pending)
            .await;
    }

    let last_step_id = layers.last().and_then(|l| l.last()).cloned();

    for layer in layers.iter() {
        if layer.len() == 1 {
            let step_id = &layer[0];
            let step_def = dag.step(step_id).ok_or_else(|| {
                WeaveError::Internal(format!("step {step_id} not found in DAG"))
            })?;

            let started_at = chrono::Utc::now().timestamp_millis();
            tracker
                .update_step(
                    &task_id,
                    step_id,
                    StepState::Running {
                        started_at,
                        attempts: 1,
                    },
                )
                .await;

            match execute_step(
                db.clone(),
                &mut scope,
                step_def,
                &task_id,
                tracker.as_ref(),
            )
            .await
            {
                Ok(output) => {
                    scope.set_output(step_id, output.clone());
                    {
                        let db_lock = db.lock().await;
                        save_step_snapshot(
                            &db_lock,
                            &task_id,
                            step_id,
                            &output,
                            Some(step_id.as_str()) == last_step_id.as_deref(),
                        );
                    }
                    let completed_at = chrono::Utc::now().timestamp_millis();
                    tracker
                        .update_step(
                            &task_id,
                            step_id,
                            StepState::Completed {
                                started_at,
                                completed_at,
                                attempts: 1,
                                cached: false,
                                duration_ms: (completed_at - started_at) as u64,
                            },
                        )
                        .await;
                }
                Err(e) => {
                    let now = chrono::Utc::now().timestamp_millis();
                    tracker
                        .update_step(
                            &task_id,
                            step_id,
                            StepState::Failed {
                                started_at: Some(started_at),
                                completed_at: now,
                                error: e.to_string(),
                                attempts: 1,
                            },
                        )
                        .await;
                    return Err(WeaveError::Internal(format!("step {step_id} failed: {e}")));
                }
            }
            continue;
        }

        let mut futures = Vec::new();
        for step_id in layer {
            let sd = dag
                .step(step_id)
                .ok_or_else(|| {
                    WeaveError::Internal(format!("step {step_id} not found in DAG"))
                })?
                .clone();
            let mut sc = scope.clone();
            let db_clone = db.clone();
            let tracker_clone = Arc::clone(tracker);
            let tid = task_id;
            let sid = step_id.clone();

            futures.push(async move {
                let started_at = chrono::Utc::now().timestamp_millis();
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

                let out = execute_step_static(db_clone, &mut sc, &sd).await;
                (sd.id.clone(), started_at, out)
            });
        }

        let results: Vec<(String, i64, WeaveResult<Value>)> =
            futures::future::join_all(futures).await;

        let mut layer_failed = false;
        let mut first_error = String::new();

        for (step_id, started_at, result) in results {
            match result {
                Ok(output) => {
                    scope.set_output(&step_id, output.clone());
                    {
                        let db_lock = db.lock().await;
                        save_step_snapshot(
                            &db_lock,
                            &task_id,
                            &step_id,
                            &output,
                            Some(step_id.as_str()) == last_step_id.as_deref(),
                        );
                    }
                    let completed_at = chrono::Utc::now().timestamp_millis();
                    tracker
                        .update_step(
                            &task_id,
                            &step_id,
                            StepState::Completed {
                                started_at,
                                completed_at,
                                attempts: 1,
                                cached: false,
                                duration_ms: (completed_at - started_at) as u64,
                            },
                        )
                        .await;
                }
                Err(e) => {
                    if !layer_failed {
                        first_error = format!("step {step_id} failed: {e}");
                        layer_failed = true;
                    }
                    let now = chrono::Utc::now().timestamp_millis();
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

    let final_output = {
        match &pipeline.output {
            RefValue::Literal(v) => serde_json::to_vec(v)
                .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?,
            RefValue::Ref(path) => {
                if path.parts.is_empty() {
                    return Err(WeaveError::Internal("empty output ref".into()));
                }
                let step_id = &path.parts[0];
                let value = scope.get_output(step_id).ok_or_else(|| {
                    WeaveError::Internal(format!("output step {step_id} not found"))
                })?;

                if path.parts.len() <= 2 {
                    serde_json::to_vec(&*value)
                        .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?
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
                    serde_json::to_vec(current)
                        .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?
                }
            }
        }
    };

    let output_val: Value = match serde_json::from_slice(&final_output) {
        Ok(v) => v,
        Err(_) => {
            serde_json::json!(final_output)
        }
    };
    tracker.complete(&task_id, output_val).await;

    Ok(final_output)
}

fn save_step_snapshot(
    db: &Database,
    task_id: &TaskId,
    step_id: &str,
    output: &Value,
    is_last: bool,
) {
    let bytes = serde_json::to_vec(output).unwrap_or_default();
    let snap = Snapshot {
        seq: 0,
        step_id: step_id.to_string(),
        output: bytes,
    };
    let _ = if is_last {
        db.save_snapshot_durable(task_id, snap)
    } else {
        db.save_snapshot(task_id, snap)
    };
}
