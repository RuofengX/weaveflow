use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::dsl::schema::{PipelineDef, RefValue, parse_template};
use crate::error::{WeaveError, WeaveResult};
use crate::operator::builtin::js::JsOperator;
use crate::operator::{Operator, get_builtin};
use crate::runtime::dag::Dag;
use crate::store::Database;
use crate::task::progress::{IterateProgress, StepState};
use crate::task::scope::Scope;
use crate::task::snapshot::Snapshot;
use crate::task::{TaskId, TaskTracker};
use tracing::{debug, warn};

pub struct Executor {
    pipeline: PipelineDef,
    db: Arc<Mutex<Database>>,
    tracker: Arc<TaskTracker>,
}

impl Executor {
    pub fn new(pipeline: PipelineDef, db: Arc<Mutex<Database>>, tracker: Arc<TaskTracker>) -> Self {
        Executor {
            pipeline,
            db,
            tracker,
        }
    }

    /// Run the pipeline. `task_id` is pre-created by caller and registered with tracker.
    ///
    /// All errors are reported to the tracker via `fail()` so the caller (HTTP handler)
    /// and WebSocket subscribers see the failure reason.
    pub async fn run(
        &self,
        task_id: TaskId,
        slots: HashMap<String, Value>,
        _ttl_secs: i64,
    ) -> WeaveResult<Vec<u8>> {
        let result = self.run_inner(task_id, slots).await;
        if let Err(ref e) = result {
            self.tracker.fail(&task_id, e.to_string()).await;
        }
        result
    }

    /// Inner execution — separated so the outer `run` can catch all errors for the tracker.
    async fn run_inner(
        &self,
        task_id: TaskId,
        slots: HashMap<String, Value>,
    ) -> WeaveResult<Vec<u8>> {
        // 1. Apply slot defaults from pipeline schema, then create scope
        let mut slots = slots;
        for slot_def in &self.pipeline.slots {
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

        // 1b. Validate slot values against their JSON Schema
        for slot_def in &self.pipeline.slots {
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

        let slots_bytes = serde_json::to_vec(&slots)
            .map_err(|e| WeaveError::Internal(format!("slots serialize: {e}")))?;
        let mut scope = Scope::new(&slots_bytes);

        // 2. DAG topology
        let dag = Dag::from_pipeline(&self.pipeline)
            .map_err(|e| WeaveError::Internal(format!("DAG build: {e}")))?;
        let layers = dag
            .topological_sort()
            .map_err(|e| WeaveError::Internal(format!("topo sort: {e}")))?;

        let all_step_ids: Vec<String> = layers.iter().flatten().cloned().collect();

        // 3. Track: set all steps to Pending initially
        for step_id in &all_step_ids {
            self.tracker
                .update_step(&task_id, step_id, StepState::Pending)
                .await;
        }

        let last_step_id = layers.last().and_then(|l| l.last()).cloned();

        // 4. Execute layers
        for layer in layers.iter() {
            if layer.len() == 1 {
                // 一共只有一层的额外操作
                let step_id = &layer[0];
                let step_def = dag.step(step_id).ok_or_else(|| {
                    WeaveError::Internal(format!("step {step_id} not found in DAG"))
                })?;

                let started_at = chrono::Utc::now().timestamp_millis();
                self.tracker
                    .update_step(
                        &task_id,
                        step_id,
                        StepState::Running {
                            started_at,
                            attempts: 1,
                        },
                    )
                    .await;

                match self.execute_step(step_def, &mut scope, &task_id).await {
                    Ok(output) => {
                        scope.set_output(step_id, &output);
                        {
                            let db = self.db.lock().await;
                            self.save_step_snapshot(
                                &db,
                                &task_id,
                                step_id,
                                output.clone(),
                                Some(step_id.as_str()) == last_step_id.as_deref(),
                            );
                        }
                        let completed_at = chrono::Utc::now().timestamp_millis();
                        self.tracker
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
                        self.tracker
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

            // Multistep layer — join_all
            let mut futures = Vec::new();
            for step_id in layer {
                let sd = dag
                    .step(step_id)
                    .ok_or_else(|| {
                        WeaveError::Internal(format!("step {step_id} not found in DAG"))
                    })?
                    .clone();
                let mut sc = scope.clone();
                let db = self.db.clone();
                let tracker = self.tracker.clone();
                let tid = task_id;
                let sid = step_id.clone();

                futures.push(async move {
                    let started_at = chrono::Utc::now().timestamp_millis();
                    tracker
                        .update_step(
                            &tid,
                            &sid,
                            StepState::Running {
                                started_at,
                                attempts: 1,
                            },
                        )
                        .await;

                    let out = Self::execute_step_static(db, &sd, &mut sc).await;
                    (sd.id.clone(), started_at, out)
                });
            }

            let results: Vec<(String, i64, WeaveResult<Vec<u8>>)> =
                futures::future::join_all(futures).await;

            let mut layer_failed = false;
            let mut first_error = String::new();

            for (step_id, started_at, result) in results {
                match result {
                    Ok(output) => {
                        scope.set_output(&step_id, &output);
                        {
                            let db = self.db.lock().await;
                            self.save_step_snapshot(
                                &db,
                                &task_id,
                                &step_id,
                                output,
                                Some(step_id.as_str()) == last_step_id.as_deref(),
                            );
                        }
                        let completed_at = chrono::Utc::now().timestamp_millis();
                        self.tracker
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
                        self.tracker
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

        // 5. Return final output
        let final_output = {
            let output_ref = parse_template(&self.pipeline.output);
            match &output_ref {
                RefValue::Literal(v) => serde_json::to_vec(v)
                    .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?,
                RefValue::Ref(var) => {
                    if var.parts.is_empty() {
                        return Err(WeaveError::Internal("empty output ref".into()));
                    }
                    let step_id = &var.parts[0];
                    let output_bytes = scope.get_output(step_id).ok_or_else(|| {
                        WeaveError::Internal(format!("output step {step_id} not found"))
                    })?;

                    if var.parts.len() <= 2 {
                        output_bytes
                    } else {
                        let v: Value = serde_json::from_slice(&output_bytes)
                            .map_err(|e| WeaveError::Internal(format!("output parse: {e}")))?;
                        let mut current = &v;
                        let start = if var.parts.len() >= 2 && var.parts[1] == "output" {
                            2
                        } else {
                            1
                        };
                        for part in &var.parts[start..] {
                            current = current.get(part).unwrap_or(&Value::Null);
                        }
                        serde_json::to_vec(current)
                            .map_err(|e| WeaveError::Internal(format!("output serialize: {e}")))?
                    }
                }
            }
        };

        let output_val: Value = serde_json::from_slice(&final_output).unwrap_or(Value::Null);
        self.tracker.complete(&task_id, output_val).await;

        Ok(final_output)
    }

    // ── step execution ──

    async fn execute_step(
        &self,
        step: &crate::dsl::schema::StepDef,
        scope: &mut Scope,
        task_id: &TaskId,
    ) -> WeaveResult<Vec<u8>> {
        let (data, config) = resolve_inputs(scope, step)?;
        let op: Box<dyn Operator> = resolve_operator(step, scope)?;

        if let Some(ref cfg) = step.iterate {
            // Step-level cache: include data + over content in key
            let over_ref = parse_template(&cfg.over);
            let over_bytes = match &over_ref {
                RefValue::Ref(var) if !var.parts.is_empty() => resolve_ref(scope, var)?,
                _ => Vec::new(),
            };
            // Build cache key: op_type + data + config + over_bytes
            let mut hasher = Sha256::new();
            hasher.update(step.r#type.as_bytes());
            hasher.update(b":");
            hasher.update(&data);
            hasher.update(b":");
            hasher.update(serde_json::to_vec(&config).unwrap_or_default());
            hasher.update(b":iterate:");
            hasher.update(&over_bytes);
            let cache_key = hasher.finalize().to_vec();

            {
                let db = self.db.lock().await;
                if let Some(cached) = db.check_cache_bytes(&cache_key)? {
                    scope.set_output(&step.id, &cached);
                    return Ok(cached);
                }
            }

            let result = self
                .execute_iterate(step, data, config, cfg, scope, task_id)
                .await?;

            // Step-level cache: after iterate (write final result)
            {
                let db = self.db.lock().await;
                db.set_cache_bytes(&cache_key, &result)?;
            }
            return Ok(result);
        }

        let cache_key = compute_cache_key(&step.r#type, &data, &config);
        {
            let db = self.db.lock().await;
            if let Some(cached) = db.check_cache_bytes(&cache_key)? {
                scope.set_output(&step.id, &cached);
                return Ok(cached);
            }
        }

        self.execute_with_retry(op.as_ref(), &data, &config, &cache_key, step)
            .await
    }

    /// Static version for parallel execution.
    async fn execute_step_static(
        db: Arc<Mutex<Database>>,
        step: &crate::dsl::schema::StepDef,
        scope: &mut Scope,
    ) -> WeaveResult<Vec<u8>> {
        let (data, config) = resolve_inputs(scope, step)?;
        let op: Box<dyn Operator> = resolve_operator(step, scope)?;

        if step.iterate.is_some() {
            return Err(WeaveError::Internal(
                "iterate not supported in parallel layer".into(),
            ));
        }

        let cache_key = compute_cache_key(&step.r#type, &data, &config);
        {
            let db_guard = db.lock().await;
            if let Some(cached) = db_guard.check_cache_bytes(&cache_key)? {
                scope.set_output(&step.id, &cached);
                return Ok(cached);
            }
        }

        let output = op
            .run(&data, &config)
            .await
            .map_err(|e| WeaveError::Operator(e.to_string()))?;
        let owned = output.into_owned();
        scope.set_output(&step.id, &owned);
        {
            let db_guard = db.lock().await;
            db_guard.set_cache_bytes(&cache_key, &owned)?;
        }
        Ok(owned)
    }

    async fn execute_iterate(
        &self,
        step: &crate::dsl::schema::StepDef,
        _data: Vec<u8>,
        config: Value,
        cfg: &crate::dsl::schema::IterateConfig,
        scope: &mut Scope,
        task_id: &TaskId,
    ) -> WeaveResult<Vec<u8>> {
        let over_ref = parse_template(&cfg.over);
        let over_bytes = match &over_ref {
            RefValue::Ref(var) if !var.parts.is_empty() => resolve_ref(scope, var)?,
            _ => {
                return Err(WeaveError::Internal(
                    "iterate.over must be a reference".into(),
                ));
            }
        };

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
        self.tracker
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
                        .await
                        .map_err(|e| WeaveError::Operator(e.to_string()))?;
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
                self.tracker
                    .update_iterate(task_id, &step.id, done_count, total_chunks)
                    .await;
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
        self.tracker
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

    async fn execute_with_retry(
        &self,
        op: &dyn Operator,
        data: &[u8],
        config: &Value,
        cache_key: &[u8],
        step: &crate::dsl::schema::StepDef,
    ) -> WeaveResult<Vec<u8>> {
        let max_attempts = step.retry.as_ref().map(|r| r.max_attempts).unwrap_or(1);
        let delay_ms = step.retry.as_ref().map(|r| r.delay_ms).unwrap_or(1000);

        for _attempt in 0..max_attempts {
            match op.run(data, config).await {
                Ok(output) => {
                    let owned = output.into_owned();
                    {
                        let db = self.db.lock().await;
                        db.set_cache_bytes(cache_key, &owned)?;
                    }
                    return Ok(owned);
                }
                Err(_) if _attempt + 1 < max_attempts => {
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                }
                Err(e) => return Err(WeaveError::Operator(e.to_string())),
            }
        }
        Err(WeaveError::Operator("retry exhausted".into()))
    }

    // ── snapshot ──

    fn save_step_snapshot(
        &self,
        db: &Database,
        task_id: &TaskId,
        step_id: &str,
        output: Vec<u8>,
        is_last: bool,
    ) {
        let snap = Snapshot {
            seq: 0,
            step_id: step_id.to_string(),
            output,
        };
        let _ = if is_last {
            db.save_snapshot_durable(task_id, snap)
        } else {
            db.save_snapshot(task_id, snap)
        };
    }
}

// ── Operator Resolution ────────────────────────────────────────────────────

/// Resolve operator for a step. Builtin by name, or inline JS.
/// `code` 中的 `{{step_id.output}}` 双花括号引用会从 scope 解析并内联。
fn resolve_operator(
    step: &crate::dsl::schema::StepDef,
    scope: &Scope,
) -> WeaveResult<Box<dyn Operator>> {
    let op_type = &step.r#type;

    // Inline JS: type == "js" with code field
    if op_type == "js" {
        let code = step.code.as_deref().unwrap_or("").to_string();

        // 解析 code 中的 {{...}} 模板引用
        let code = resolve_code_templates(&code, scope)?;

        return Ok(Box::new(JsOperator {
            name: step.id.clone(),
            source: code,
        }));
    }

    // Builtin lookup
    get_builtin(op_type).ok_or_else(|| WeaveError::Internal(format!("未注册: {op_type}")))
}

/// 解析 code 中的 `{{step_id.output}}` 双花括号模板引用。
/// 单花括号 `{...}` 在 JS 中常见（代码块、对象字面量），不处理。
fn resolve_code_templates(code: &str, scope: &Scope) -> WeaveResult<String> {
    // 匹配 {{identifier.output.path}}
    let re = regex::Regex::new(r"\{\{([a-zA-Z_][\w.]*)\}\}")
        .map_err(|e| WeaveError::Internal(format!("code template regex: {e}")))?;

    let mut result = code.to_string();
    for cap in re.captures_iter(code) {
        let ref_expr = &cap[1]; // e.g. "load_dep.output" or "load_dep.output.body"
        let parts: Vec<&str> = ref_expr.split('.').collect();
        if parts.is_empty() || parts[0].is_empty() {
            continue;
        }
        let step_id = parts[0];
        let bytes = scope.get_output(step_id).ok_or_else(|| {
            WeaveError::Internal(format!(
                "code 模板 {{}} 引用了不存在的步骤: {step_id}"
            ))
        })?;

        // 有嵌套路径时：JSON 解析 → 取字段
        let resolved = if parts.len() <= 1 || (parts.len() == 2 && parts[1] == "output") {
            String::from_utf8(bytes).map_err(|e| {
                WeaveError::Internal(format!("code 模板 {step_id}.output 不是 UTF-8: {e}"))
            })?
        } else {
            let v: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                WeaveError::Internal(format!("code 模板 {step_id} JSON 解析: {e}"))
            })?;
            let mut current = &v;
            let start = if parts[1] == "output" { 2 } else { 1 };
            for part in &parts[start..] {
                current = current.get(part).unwrap_or(&serde_json::Value::Null);
            }
            match current {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        };

        result = result.replace(&cap[0], &resolved);
    }

    Ok(result)
}

// ── Input Resolution ───────────────────────────────────────────────────────

fn resolve_inputs(
    scope: &Scope,
    step: &crate::dsl::schema::StepDef,
) -> WeaveResult<(Vec<u8>, Value)> {
    let as_name = step.iterate.as_ref().map(|c| c.as_name.as_str());
    let Some(inputs) = &step.inputs else {
        return Ok((Vec::new(), Value::Object(Default::default())));
    };

    match inputs {
        Value::Object(map) => {
            let mut data: Vec<u8> = Vec::new();
            let mut config_map = serde_json::Map::new();

            for (k, v) in map {
                match v {
                    Value::String(s) => match parse_template(s) {
                        RefValue::Ref(ref_var) => {
                            if ref_var.parts.first().map(|p| p.as_str()) == as_name {
                                continue;
                            }
                            let resolved = resolve_ref(scope, &ref_var)?;
                            if k == "data" {
                                data = resolved;
                            } else {
                                let val: Value = {
                                    let resolved_len = resolved.len();
                                    let ref_str = ref_var.parts.join(".");

                                    match serde_json::from_slice(&resolved) {
                                        Ok(v) => v,
                                        Err(e_json) => {
                                            match String::from_utf8(resolved) {
                                                Ok(s) => Value::String(s),
                                                Err(e_utf8) => {
                                                    let preview: String = e_utf8
                                                        .as_bytes()
                                                        .iter()
                                                        .take(40)
                                                        .map(|b| format!("{b:02x}"))
                                                        .collect::<Vec<_>>()
                                                        .join(" ");
                                                    warn!(
                                                        step_id = %ref_str,
                                                        key = %k,
                                                        bytes_len = resolved_len,
                                                        hex_preview = %preview,
                                                        "ref value for key '{k}' is neither valid JSON ({e_json}) nor UTF-8 ({e_utf8}) — using empty string"
                                                    );
                                                    Value::String(String::new())
                                                }
                                            }
                                        }
                                    }
                                };
                                config_map.insert(k.clone(), val);
                            }
                        }
                        RefValue::Literal(lit) => {
                            if k == "data" {
                                data = s.as_bytes().to_vec();
                            } else {
                                config_map.insert(k.clone(), lit.clone());
                            }
                        }
                    },
                    other => {
                        if k == "data" {
                            data = serde_json::to_vec(other).map_err(|e| {
                                WeaveError::Internal(format!("data serialize: {e}"))
                            })?;
                        } else {
                            config_map.insert(k.clone(), other.clone());
                        }
                    }
                }
            }

            Ok((data, Value::Object(config_map)))
        }
        other => {
            let bytes = serde_json::to_vec(other)
                .map_err(|e| WeaveError::Internal(format!("input serialize: {e}")))?;
            Ok((bytes, Value::Object(Default::default())))
        }
    }
}

fn resolve_ref(scope: &Scope, var: &crate::dsl::schema::VariableRef) -> WeaveResult<Vec<u8>> {
    if var.parts.is_empty() {
        return Ok(Vec::new());
    }

    match var.parts[0].as_str() {
        "slots" => {
            let slots_bytes = scope.slots().unwrap_or_default();
            if slots_bytes.is_empty() {
                warn!("scope has no slots bytes, resolving ref {}", var.parts.join("."));
                return Ok(Vec::new());
            }
            let v: Value = serde_json::from_slice(&slots_bytes)
                .map_err(|e| WeaveError::Internal(format!("slots parse: {e}")))?;
            let mut current = &v;
            for part in &var.parts[1..] {
                let next = current.get(part);
                if next.is_none() {
                    warn!(
                        ref_path = %var.parts.join("."),
                        missing_part = %part,
                        available = %serde_json::to_string(current).unwrap_or_default(),
                        "slot ref path not found, using Null"
                    );
                }
                current = next.unwrap_or(&Value::Null);
            }
            debug!(
                ref_path = %var.parts.join("."),
                resolved = %serde_json::to_string(current).unwrap_or_default(),
                "resolved slot ref"
            );
            serde_json::to_vec(current)
                .map_err(|e| WeaveError::Internal(format!("ref serialize: {e}")))
        }
        "env" => {
            let val = if var.parts.len() >= 2 {
                std::env::var(&var.parts[1]).unwrap_or_default()
            } else {
                String::new()
            };
            Ok(val.into_bytes())
        }
        _ => {
            let step_id = &var.parts[0];
            let bytes = scope.get_output(step_id).ok_or_else(|| {
                WeaveError::Internal(format!("step {step_id} not found in scope"))
            })?;

            if var.parts.len() == 1 || (var.parts.len() == 2 && var.parts[1] == "output") {
                Ok(bytes)
            } else {
                let v: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| WeaveError::Internal(format!("step output parse: {e}")))?;
                let mut current = &v;
                let start = if var.parts.len() >= 2 && var.parts[1] == "output" {
                    2
                } else {
                    1
                };
                for part in &var.parts[start..] {
                    current = current.get(part).unwrap_or(&Value::Null);
                }
                serde_json::to_vec(current)
                    .map_err(|e| WeaveError::Internal(format!("field serialize: {e}")))
            }
        }
    }
}

// ── Cache ──────────────────────────────────────────────────────────────────

fn compute_cache_key(op_type: &str, data: &[u8], config: &Value) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(op_type.as_bytes());
    hasher.update(b":");
    hasher.update(data);
    hasher.update(b":");
    hasher.update(serde_json::to_vec(config).unwrap_or_default());
    hasher.finalize().to_vec()
}
