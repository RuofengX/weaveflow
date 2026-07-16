//! Step 执行：输入解析 → 缓存查找 → iterate 分发 → retry 循环。
//!
//! 单个 step 的执行流程：
//!   1. 解析输入（slots、环境变量、上游输出）
//!   2. 基于 (op_type, data, config) 计算内容寻址缓存 key
//!   3. 查缓存 → 命中 → 直接返回
//!   4. 如果配置了 iterate → 展开数组分批执行 → 结果写入缓存
//!   5. 否则 → 解析算子 → execute_with_retry（含重试）→ 写入缓存

use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};

use crate::dsl::StepDef;
use crate::engine::cache::compute_cache_key;
use crate::engine::iterate::execute_iterate;
use crate::error::{WeaveError, WeaveResult};
use crate::operator::{get_builtin, Operator};
use crate::store::Database;
use crate::tracker::{TaskId, TaskTracker};
use crate::vm::resolver::resolve_inputs;
use crate::vm::Scope;

pub async fn execute_step(
    db: Arc<Mutex<Database>>,
    scope: &mut Scope,
    step: &StepDef,
    task_id: &TaskId,
    tracker: &TaskTracker,
) -> WeaveResult<Value> {
    // 1. 解析输入：将 scope 中的 slots、env、上游输出填入 step 的 inputs 占位符。
    let (data, config) = resolve_inputs(scope, step)?;
    trace!(step = %step.id, op = %step.op.op_type(), "inputs resolved");

    // 2. 计算内容寻址缓存 key。
    //    相同 (op_type, data, config) 总是映射到同一 key — iterate 和
    //    非 iterate 路径共享此 key，因此 iterate 的缓存结果可以服务于
    //    非 iterate 运行（反之亦然），只要输入匹配。
    let cache_key = compute_cache_key(step.op.op_type(), &data, &config);
    {
        let db_lock = db.lock().await;
        if let Some(v) = db_lock.check_cache_bytes(&cache_key)? {
            debug!(step = %step.id, op = %step.op.op_type(), "cache hit");
            scope.set_output(&step.id, v.clone());
            return Ok(v);
        }
    }
    debug!(step = %step.id, op = %step.op.op_type(), "cache miss");

    // 3. Iterate 路径：展开输入数组，分批执行，聚合结果。
    if let Some(cfg) = step.iterate.as_ref() {
        info!(
            step = %step.id,
            as_name = %cfg.as_name,
            max_workers = ?cfg.max_workers,
            batched = cfg.batch.is_some(),
            "dispatching iterate",
        );
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
        debug!(step = %step.id, "iterate result cached");
        return Ok(result);
    }

    // 4. 解析算子并直接执行（含重试）。
    let op: Box<dyn Operator> = resolve_operator(step)?;
    info!(step = %step.id, op = %step.op.op_type(), "executing");
    execute_with_retry(db, op.as_ref(), &data, &config, &cache_key, step).await
}

/// 调用 op.run(data, config)，失败时按重试配置自动重试。
/// 成功后输出写入缓存；最终失败时返回最后一次错误。
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
                trace!(step = %step.id, attempt = _attempt + 1, "output cached");
                return Ok(output);
            }
            Err(_) if _attempt + 1 < max_attempts => {
                warn!(step = %step.id, attempt = _attempt + 1, max_attempts, delay_ms, "retrying");
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            Err(e) => {
                warn!(step = %step.id, attempt = _attempt + 1, max_attempts, "operator failed");
                return Err(e.into());
            }
        }
    }
    warn!(step = %step.id, max_attempts, "retry exhausted");
    Err(WeaveError::Operator("retry exhausted".into()))
}

/// 从编译期注册表查找 step 对应的算子。
/// 所有算子（包括 JS）统一通过 op_type 查找，不再有特殊处理分支。
pub fn resolve_operator(step: &StepDef) -> WeaveResult<Box<dyn Operator>> {
    let op_type = step.op.op_type();
    trace!(step = %step.id, op_type, "resolving operator");
    get_builtin(op_type).ok_or_else(|| WeaveError::Internal(format!("未注册: {op_type}")))
}


