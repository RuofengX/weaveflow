use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::Value;

use crate::error::WeaveResult;

pub async fn run_js(code: &str, func_name: &str, input: &Value, timeout_ms: Option<u64>) -> WeaveResult<Value> {
    let script = build_script(code, func_name, &serde_json::to_string(input)?);
    let interrupted = Arc::new(AtomicBool::new(false));

    if let Some(ms) = timeout_ms {
        let timer_interrupted = interrupted.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(ms));
            timer_interrupted.store(true, Ordering::SeqCst);
        });
    }

    tokio::task::spawn_blocking(move || run_in_new_runtime(&script, interrupted))
        .await
        .map_err(|e| crate::error::WeaveError::Internal(format!("spawn_blocking: {e}")))?
}

fn build_script(code: &str, func_name: &str, input_json: &str) -> String {
    format!(
        "{code}\n\
        try {{\n\
            var __result__ = {func_name}({input_json});\n\
            JSON.stringify(__result__)\n\
        }} catch(__e__) {{\n\
            JSON.stringify({{__weave_error__: true, message: __e__.message || String(__e__), stack: __e__.stack || ''}})\n\
        }}\n"
    )
}

fn run_in_new_runtime(script: &str, interrupted: Arc<AtomicBool>) -> WeaveResult<Value> {
    let rt = rquickjs::Runtime::new()
        .map_err(|e| crate::error::WeaveError::Internal(format!("create JS runtime: {e}")))?;
    rt.set_memory_limit(256 * 1024 * 1024);
    rt.set_max_stack_size(1024 * 1024);
    rt.set_interrupt_handler(Some(Box::new(move || interrupted.load(Ordering::SeqCst))));

    let ctx = rquickjs::Context::full(&rt)
        .map_err(|e| crate::error::WeaveError::Internal(format!("create JS context: {e}")))?;

    ctx.with(|ctx| {
        {
            let globals = ctx.globals();

            let inflate_fn = rquickjs::Function::new(ctx.clone(), |data: Vec<u8>| -> Vec<u8> {
                use std::io::Read;
                let mut decoder = flate2::read::ZlibDecoder::new(&data[..]);
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .expect("zlib decompression failed");
                out
            });

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

            let native = rquickjs::Object::new(ctx.clone())
                .map_err(|e| crate::error::WeaveError::Internal(format!("create __native__: {e}")))?;
            native
                .set("inflate", inflate_fn)
                .map_err(|e| crate::error::WeaveError::Internal(format!("set inflate: {e}")))?;
            native
                .set("btoa", btoa_fn)
                .map_err(|e| crate::error::WeaveError::Internal(format!("set btoa: {e}")))?;
            native
                .set("atob", atob_fn)
                .map_err(|e| crate::error::WeaveError::Internal(format!("set atob: {e}")))?;
            globals.set("__native__", native).unwrap();
        }

        let json_str: String = ctx
            .eval(script)
            .map_err(|e| crate::error::WeaveError::Internal(format!("JS runtime error: {e}")))?;
        let val: Value = serde_json::from_str(&json_str)
            .map_err(|e| crate::error::WeaveError::Internal(format!("parse JS output: {e}")))?;
        if let Some(obj) = val.as_object()
            && obj.get("__weave_error__").and_then(|v| v.as_bool()).unwrap_or(false) {
                let msg = obj.get("message").and_then(|v| v.as_str()).unwrap_or("unknown error");
                let stack = obj.get("stack").and_then(|v| v.as_str()).unwrap_or("");
                return Err(crate::error::WeaveError::Internal(
                    if stack.is_empty() {
                        format!("JS: {msg}")
                    } else {
                        format!("JS: {msg}\n{stack}")
                    }
                ));
            }
        Ok(val)
    })
}
