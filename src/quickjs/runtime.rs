use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Value;

use crate::error::WeaveflowResult;

/// __native__.inflate 解压输出上限（Rust 侧分配，不受 QuickJS memory_limit 约束）。
const MAX_INFLATE_BYTES: u64 = 256 * 1024 * 1024;

/// run_js future 被 drop（如 step 层 timeout 取消）时置位中断标志，
/// QuickJS interrupt handler 随之触发，spawn_blocking 线程退出。
struct InterruptGuard(Arc<AtomicBool>);

impl Drop for InterruptGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

pub async fn run_js(code: &str, func_name: &str, input: &Value) -> WeaveflowResult<Value> {
    let script = build_script(code, func_name, &serde_json::to_string(input)?);
    let interrupted = Arc::new(AtomicBool::new(false));
    let _guard = InterruptGuard(interrupted.clone());

    tokio::task::spawn_blocking(move || run_in_new_runtime(&script, interrupted))
        .await
        .map_err(|e| crate::error::WeaveflowError::Internal(format!("spawn_blocking: {e}")))?
}

fn build_script(code: &str, func_name: &str, input_json: &str) -> String {
    format!(
        "{code}\n\
        try {{\n\
            var __result__ = {func_name}({input_json});\n\
            JSON.stringify({{__weaveflow_ok__: true, value: __result__}})\n\
        }} catch(__e__) {{\n\
            JSON.stringify({{__weaveflow_ok__: false, message: __e__.message || String(__e__), stack: __e__.stack || ''}})\n\
        }}\n"
    )
}

fn run_in_new_runtime(script: &str, interrupted: Arc<AtomicBool>) -> WeaveflowResult<Value> {
    let rt = rquickjs::Runtime::new()
        .map_err(|e| crate::error::WeaveflowError::Internal(format!("create JS runtime: {e}")))?;
    rt.set_memory_limit(256 * 1024 * 1024);
    rt.set_max_stack_size(1024 * 1024);
    rt.set_interrupt_handler(Some(Box::new(move || interrupted.load(Ordering::SeqCst))));

    let ctx = rquickjs::Context::full(&rt)
        .map_err(|e| crate::error::WeaveflowError::Internal(format!("create JS context: {e}")))?;

    ctx.with(|ctx| {
        {
            let globals = ctx.globals();

            let inflate_fn = rquickjs::Function::new(
                ctx.clone(),
                |data: Vec<u8>| -> rquickjs::Result<Vec<u8>> {
                    use std::io::Read;
                    // 解压炸弹防护：QuickJS 堆的 256MB memory_limit 管不到
                    // Rust 侧分配，必须自行封顶；超限/损坏数据抛 JS 异常而非 panic。
                    let mut decoder =
                        flate2::read::ZlibDecoder::new(&data[..]).take(MAX_INFLATE_BYTES + 1);
                    let mut out = Vec::new();
                    decoder.read_to_end(&mut out).map_err(|e| {
                        rquickjs::Error::new_from_js_message("inflate", "zlib", format!("{e}"))
                    })?;
                    if out.len() as u64 > MAX_INFLATE_BYTES {
                        return Err(rquickjs::Error::new_from_js_message(
                            "inflate",
                            "limit",
                            format!("decompressed data exceeds {} bytes", MAX_INFLATE_BYTES),
                        ));
                    }
                    Ok(out)
                },
            );

            let btoa_fn = rquickjs::Function::new(ctx.clone(), |data: Vec<u8>| -> String {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(&data)
            });

            let atob_fn = rquickjs::Function::new(ctx.clone(), |s: String| -> Vec<u8> {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD
                    .decode(s.as_bytes())
                    .expect("base64 decode failed")
            });

            let native = rquickjs::Object::new(ctx.clone()).map_err(|e| {
                crate::error::WeaveflowError::Internal(format!("create __native__: {e}"))
            })?;
            native
                .set("inflate", inflate_fn)
                .map_err(|e| crate::error::WeaveflowError::Internal(format!("set inflate: {e}")))?;
            native
                .set("btoa", btoa_fn)
                .map_err(|e| crate::error::WeaveflowError::Internal(format!("set btoa: {e}")))?;
            native
                .set("atob", atob_fn)
                .map_err(|e| crate::error::WeaveflowError::Internal(format!("set atob: {e}")))?;
            globals.set("__native__", native).unwrap();
        }

        let json_str: String = ctx.eval(script).map_err(|e| {
            crate::error::WeaveflowError::Internal(format!("JS runtime error: {e}"))
        })?;
        let val: Value = serde_json::from_str(&json_str)
            .map_err(|e| crate::error::WeaveflowError::Internal(format!("parse JS output: {e}")))?;
        let obj = val.as_object();
        let ok = obj
            .and_then(|o| o.get("__weaveflow_ok__"))
            .and_then(|v| v.as_bool());
        match ok {
            Some(true) => Ok(obj
                .and_then(|o| o.get("value").cloned())
                .unwrap_or(Value::Null)),
            _ => {
                let msg = obj
                    .and_then(|o| o.get("message"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                let stack = obj
                    .and_then(|o| o.get("stack"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                Err(crate::error::WeaveflowError::Internal(
                    if stack.is_empty() {
                        format!("JS: {msg}")
                    } else {
                        format!("JS: {msg}\n{stack}")
                    },
                ))
            }
        }
    })
}
