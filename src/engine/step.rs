use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::dsl::pipeline::{parse_template, RefValue, StepDef};
use crate::engine::cache::compute_cache_key;
use crate::engine::iterate::execute_iterate;
use crate::error::{WeaveError, WeaveResult};
use crate::operator::builtin::js::JsOperator;
use crate::operator::{get_builtin, Operator};
use crate::store::Database;
use crate::tracker::{TaskId, TaskTracker};
use crate::vm::resolver::{resolve_code_templates, resolve_inputs, resolve_ref};
use crate::vm::Scope;

pub async fn execute_step(
    db: Arc<Mutex<Database>>,
    scope: &mut Scope,
    step: &StepDef,
    task_id: &TaskId,
    tracker: &TaskTracker,
) -> WeaveResult<Vec<u8>> {
    let (data, config) = resolve_inputs(scope, step)?;
    let op: Box<dyn Operator> = resolve_operator(step, scope)?;

    if let Some(ref cfg) = step.iterate {
        let over_ref = parse_template(&cfg.over);
        let over_bytes = match &over_ref {
            RefValue::Ref(var) if !var.parts.is_empty() => resolve_ref(scope, var)?,
            _ => Vec::new(),
        };
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
            let db_lock = db.lock().await;
            if let Some(cached) = db_lock.check_cache_bytes(&cache_key)? {
                scope.set_output(&step.id, &cached);
                return Ok(cached);
            }
        }

        let result = execute_iterate(
            db.clone(),
            scope,
            step,
            config,
            cfg,
            task_id,
            tracker,
        )
        .await?;

        {
            let db_lock = db.lock().await;
            db_lock.set_cache_bytes(&cache_key, &result)?;
        }
        return Ok(result);
    }

    let cache_key = compute_cache_key(&step.r#type, &data, &config);
    {
        let db_lock = db.lock().await;
        if let Some(cached) = db_lock.check_cache_bytes(&cache_key)? {
            scope.set_output(&step.id, &cached);
            return Ok(cached);
        }
    }

    execute_with_retry(db, op.as_ref(), &data, &config, &cache_key, step).await
}

pub async fn execute_step_static(
    db: Arc<Mutex<Database>>,
    scope: &mut Scope,
    step: &StepDef,
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

pub async fn execute_with_retry(
    db: Arc<Mutex<Database>>,
    op: &dyn Operator,
    data: &[u8],
    config: &Value,
    cache_key: &[u8],
    step: &StepDef,
) -> WeaveResult<Vec<u8>> {
    let max_attempts = step.retry.as_ref().map(|r| r.max_attempts).unwrap_or(1);
    let delay_ms = step.retry.as_ref().map(|r| r.delay_ms).unwrap_or(1000);

    for _attempt in 0..max_attempts {
        match op.run(data, config).await {
            Ok(output) => {
                let owned = output.into_owned();
                {
                    let db_lock = db.lock().await;
                    db_lock.set_cache_bytes(cache_key, &owned)?;
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

pub fn resolve_operator(
    step: &StepDef,
    scope: &Scope,
) -> WeaveResult<Box<dyn Operator>> {
    let op_type = &step.r#type;

    if op_type == "js" {
        let code = step.code.as_deref().unwrap_or("").to_string();
        let code = resolve_code_templates(&code, scope)?;
        return Ok(Box::new(JsOperator {
            name: step.id.clone(),
            source: code,
        }));
    }

    get_builtin(op_type).ok_or_else(|| WeaveError::Internal(format!("未注册: {op_type}")))
}

pub fn resolve_rule_operator(
    rule: &crate::dsl::pipeline::RuleDef,
    scope: Option<&Scope>,
) -> WeaveResult<Box<dyn Operator>> {
    let op_type = &rule.r#type;

    if op_type == "js" {
        let mut code = rule.code.as_deref().unwrap_or("").to_string();
        if let Some(s) = scope {
            code = resolve_code_templates(&code, s)?;
        }
        return Ok(Box::new(JsOperator {
            name: rule.id.clone(),
            source: code,
        }));
    }

    get_builtin(op_type).ok_or_else(|| WeaveError::Internal(format!("未注册: {op_type}")))
}
