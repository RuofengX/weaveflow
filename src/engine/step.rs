use std::sync::Arc;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::dsl::StepDef;
use crate::dsl::StepOp;
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
) -> WeaveResult<Value> {
    let (data, config) = resolve_inputs(scope, step)?;
    let op: Box<dyn Operator> = resolve_operator(step, scope)?;

    if let Some(cfg) = step.iterate.as_ref() {
        let over_ref = resolve_ref(scope, &cfg.over)?;
        let mut hasher = Sha256::new();
        hasher.update(step.op.op_type().as_bytes());
        hasher.update(b":");
        hasher.update(serde_json::to_vec(&*data).unwrap_or_default());
        hasher.update(b":");
        hasher.update(serde_json::to_vec(&config).unwrap_or_default());
        hasher.update(b":iterate:");
        hasher.update(serde_json::to_vec(&*over_ref).unwrap_or_default());
        let cache_key = hasher.finalize().to_vec();

        {
            let db_lock = db.lock().await;
            if let Some(v) = db_lock.check_cache_bytes(&cache_key)? {
                debug!(step = %step.id, "iterate cache hit");
                scope.set_output(&step.id, v.clone());
                return Ok(v);
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

    let cache_key = compute_cache_key(step.op.op_type(), &data, &config);
    {
        let db_lock = db.lock().await;
        if let Some(v) = db_lock.check_cache_bytes(&cache_key)? {
            debug!(step = %step.id, op = %step.op.op_type(), "cache hit");
            scope.set_output(&step.id, v.clone());
            return Ok(v);
        }
    }

    execute_with_retry(db, op.as_ref(), &data, &config, &cache_key, step).await
}
pub async fn execute_with_retry(
    db: Arc<Mutex<Database>>,
    op: &dyn Operator,
    data: &Value,
    config: &Value,
    cache_key: &[u8],
    step: &StepDef,
) -> WeaveResult<Value> {
    let max_attempts = step.retry.as_ref().map(|r| r.max_attempts).unwrap_or(1);
    let delay_ms = step.retry.as_ref().map(|r| r.delay_ms).unwrap_or(1000);

    for _attempt in 0..max_attempts {
        debug!(step = %step.id, attempt = _attempt + 1, max_attempts, "executing operator");
        match op.run(data, config).await {
            Ok(output) => {
                if _attempt > 0 {
                    info!(step = %step.id, attempt = _attempt + 1, "retry succeeded");
                }
                {
                    let db_lock = db.lock().await;
                    db_lock.set_cache_bytes(cache_key, &output)?;
                }
                return Ok(output);
            }
            Err(_) if _attempt + 1 < max_attempts => {
                warn!(step = %step.id, attempt = _attempt + 1, max_attempts, delay_ms, "retrying");
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err(WeaveError::Operator("retry exhausted".into()))
}

pub fn resolve_operator(
    step: &StepDef,
    scope: &Scope,
) -> WeaveResult<Box<dyn Operator>> {
    if let StepOp::Js(ref inputs) = step.op {
        let code = resolve_code_templates(&inputs.code, scope)?;
        return Ok(Box::new(JsOperator {
            name: step.id.0.clone(),
            source: code,
        }));
    }

    let op_type = step.op.op_type();
    get_builtin(op_type).ok_or_else(|| WeaveError::Internal(format!("未注册: {op_type}")))
}


