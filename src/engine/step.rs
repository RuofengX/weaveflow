//! Step 执行：输入解析 → 缓存查找 → iterate 分发 → retry 循环。
//!
//! 单个 step 的执行流程：
//!   1. 解析输入（slots、环境变量、上游输出）
//!   2. 解析算子，确定缓存策略（step.cache 覆盖算子 spec().cache）
//!   3. 基于 (op_type, inputs) 计算内容寻址缓存 key（iterate 步骤混入 over 数组）
//!   4. 缓存开启时查缓存 → 命中 → 直接返回
//!   5. 如果配置了 iterate → 展开数组分批执行（每个元素套超时）→ 按需写入缓存
//!   6. 否则 → execute_with_retry（含重试与超时）→ 按需写入缓存

use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, info, trace, warn};

use crate::dsl::StepDef;
use crate::engine::cache::compute_cache_key;
use crate::engine::iterate::execute_iterate;
use crate::error::{WeaveflowError, WeaveflowResult};
use crate::operator::{Operator, OperatorError, get_builtin};
use crate::store::Database;
use crate::tracker::{TaskId, TaskTracker};
use crate::vm::Scope;
use crate::vm::resolver::{resolve_inputs, resolve_ref};

pub async fn execute_step(
    db: Arc<Database>,
    scope: &mut Scope,
    step: &StepDef,
    task_id: &TaskId,
    tracker: &TaskTracker,
) -> Result<(Value, u32, bool), (WeaveflowError, u32)> {
    // 1. 解析输入：将 scope 中的 slots、env、上游输出填入 step 的 inputs 占位符。
    //    iterate 步骤此处仅用于缓存 key 材料（无 locals，as_name 引用保持占位符
    //    字面量）；真正的逐 chunk 解析在 execute_iterate 内进行。
    let inputs = resolve_inputs(scope, step).map_err(|e| (e, 0))?;
    trace!(step = %step.id, op = %step.op.op_type(), "inputs resolved");

    // 2. 解析算子；step.cache 显式覆盖算子 spec().cache 的默认值。
    let op: Box<dyn Operator> = resolve_operator(step).map_err(|e| (e, 0))?;
    let cache_enabled = step.cache.unwrap_or(op.spec().cache);

    // 3. 计算内容寻址缓存 key（粒度 = 整个 step，与是否配置 iterate 无关）；
    //    iterate 步骤将解析后的 over 数组混入 key，
    //    避免相同 inputs 配不同 over 数据命中同一缓存。
    let cache_key = if let Some(cfg) = step.iterate.as_ref() {
        let over = resolve_ref(scope, &cfg.over).map_err(|e| (e, 0))?;
        let mut key_material = serde_json::Map::new();
        key_material.insert("inputs".into(), inputs.clone());
        key_material.insert("over".into(), (*over).clone());
        compute_cache_key(step.op.op_type(), &Value::Object(key_material))
    } else {
        compute_cache_key(step.op.op_type(), &inputs)
    };

    if cache_enabled && let Some(v) = db.check_cache_bytes(&cache_key).map_err(|e| (e, 0))? {
        debug!(step = %step.id, op = %step.op.op_type(), "cache hit");
        scope.set_output(&step.id, v.clone());
        return Ok((v, 0, true));
    }
    debug!(step = %step.id, op = %step.op.op_type(), "cache miss");

    // 4. Iterate 路径：展开输入数组，逐 chunk 绑定 as_name 解析 inputs，聚合结果。
    if let Some(cfg) = step.iterate.as_ref() {
        info!(
            step = %step.id,
            as_name = %cfg.as_name,
            max_workers = ?cfg.max_workers,
            batched = cfg.batch.is_some(),
            "dispatching iterate",
        );
        let (result, attempts) = execute_iterate(scope, step, cfg, task_id, tracker).await?;

        if cache_enabled {
            if let Err(e) = db.set_cache_bytes(&cache_key, &result) {
                warn!(step = %step.id, error = %e, "cache write failed; continuing without cache");
            } else {
                debug!(step = %step.id, "iterate result cached");
            }
        }
        return Ok((result, attempts, false));
    }

    info!(step = %step.id, op = %step.op.op_type(), "executing");
    let (output, attempts) =
        execute_with_retry(db, op.as_ref(), inputs, &cache_key, step, cache_enabled).await?;
    Ok((output, attempts, false))
}

/// 调用 op.run(inputs)，失败时按重试配置自动重试（不含缓存）。
/// 错误同样携带真实已尝试次数。
pub(crate) async fn retry_with_op(
    op: &dyn Operator,
    inputs: Value,
    step: &StepDef,
) -> Result<(Value, u32), (OperatorError, u32)> {
    let retry = step.retry.as_ref();
    let max_attempts = retry.map(|r| r.max_attempts.max(1)).unwrap_or(1);
    let delay_ms = retry.map(|r| r.delay_ms).unwrap_or(1000);
    let backoff = retry.map(|r| &r.backoff);

    let mut inputs = inputs;
    for attempt in 0..max_attempts {
        let is_last = attempt + 1 == max_attempts;
        let attempt_inputs = if is_last {
            std::mem::take(&mut inputs)
        } else {
            inputs.clone()
        };
        match run_with_timeout(op, attempt_inputs, step).await {
            Ok(output) => {
                let tries = attempt + 1;
                if attempt > 0 {
                    info!(step = %step.id, attempt = tries, "retry succeeded");
                }
                return Ok((output, tries));
            }
            Err(e) if !is_last => {
                let wait = match backoff {
                    None | Some(crate::dsl::BackoffStrategy::Fixed) => delay_ms,
                    Some(crate::dsl::BackoffStrategy::Exponential) => {
                        delay_ms.saturating_mul(1u64 << attempt.min(20)).min(60_000)
                    }
                };
                warn!(step = %step.id, attempt = attempt + 1, max_attempts, wait_ms = wait, error = %e, "retrying");
                tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
            }
            Err(e) => {
                warn!(step = %step.id, attempt = attempt + 1, max_attempts, error = %e, "operator failed");
                return Err((e, attempt + 1));
            }
        }
    }
    warn!(step = %step.id, max_attempts, "retry exhausted");
    Err((
        OperatorError::Runtime("retry exhausted".into()),
        max_attempts,
    ))
}

/// 调用 retry_with_op 执行算子并缓存成功输出；缓存写失败降级为告警。
pub async fn execute_with_retry(
    db: Arc<Database>,
    op: &dyn Operator,
    inputs: Value,
    cache_key: &[u8],
    step: &StepDef,
    cache_enabled: bool,
) -> Result<(Value, u32), (WeaveflowError, u32)> {
    let (output, attempts) = retry_with_op(op, inputs, step)
        .await
        .map_err(|(e, a)| (WeaveflowError::from(e), a))?;
    if cache_enabled {
        if let Err(e) = db.set_cache_bytes(cache_key, &output) {
            warn!(step = %step.id, error = %e, "cache write failed; continuing without cache");
        } else {
            trace!(step = %step.id, "output cached");
        }
    }
    Ok((output, attempts))
}

/// 执行算子；step.timeout_sec 设置时用 tokio::time::timeout 包裹，超时映射 OperatorError::Timeout。
pub(crate) async fn run_with_timeout(
    op: &dyn Operator,
    inputs: Value,
    step: &StepDef,
) -> Result<Value, OperatorError> {
    match step.timeout_sec {
        Some(secs) if secs.is_finite() && secs > 0.0 => {
            let duration = std::time::Duration::try_from_secs_f64(secs)
                .map_err(|e| OperatorError::Config(format!("invalid timeout_sec {secs}: {e}")))?;
            match tokio::time::timeout(duration, op.run(inputs)).await {
                Ok(result) => result,
                Err(_) => {
                    warn!(step = %step.id, timeout_sec = secs, "step timed out; operator future cancelled");
                    Err(OperatorError::Timeout)
                }
            }
        }
        _ => op.run(inputs).await,
    }
}

/// 从编译期注册表查找 step 对应的算子。
/// 所有算子（包括 JS）统一通过 op_type 查找，不再有特殊处理分支。
pub fn resolve_operator(step: &StepDef) -> WeaveflowResult<Box<dyn Operator>> {
    let op_type = step.op.op_type();
    trace!(step = %step.id, op_type, "resolving operator");
    get_builtin(op_type).ok_or_else(|| WeaveflowError::Internal(format!("未注册: {op_type}")))
}
