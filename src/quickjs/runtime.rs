use serde_json::Value;

use crate::error::WeaveResult;

/// QuickJS 内嵌运行时。每个调用创建独立 Runtime（隔离），spawn_blocking 桥接 tokio。
///
/// 每次运行注入 `__native__` 全局对象，提供原生函数：
///   - `inflate(data)`  → Uint8Array  — zlib 解压
///   - `btoa(data)`     → string      — Uint8Array → base64 编码
///   - `atob(s)`        → Uint8Array  — base64 解码
pub async fn run_js(code: &str, func_name: &str, input: &Value) -> WeaveResult<Value> {
    let script = build_script(code, func_name, &serde_json::to_string(input)?);

    tokio::task::spawn_blocking(move || run_in_new_runtime(&script))
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

fn run_in_new_runtime(script: &str) -> WeaveResult<Value> {
    let rt = rquickjs::Runtime::new()
        .map_err(|e| crate::error::WeaveError::Internal(format!("create JS runtime: {e}")))?;
    let ctx = rquickjs::Context::full(&rt)
        .map_err(|e| crate::error::WeaveError::Internal(format!("create JS context: {e}")))?;

    ctx.with(|ctx| {
        // ── 注入原生能力 ────────────────────────────────────
        {
            let globals = ctx.globals();

            // inflate: zlib 解压 (Uint8Array → Uint8Array)
            let inflate_fn = rquickjs::Function::new(ctx.clone(), |data: Vec<u8>| -> Vec<u8> {
                use std::io::Read;
                let mut decoder = flate2::read::ZlibDecoder::new(&data[..]);
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .expect("zlib decompression failed");
                out
            });

            // btoa: Uint8Array → base64 string
            let btoa_fn = rquickjs::Function::new(ctx.clone(), |data: Vec<u8>| -> String {
                use base64::Engine;
                base64::engine::general_purpose::STANDARD.encode(&data)
            });

            // atob: base64 string → Uint8Array
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
        // ─────────────────────────────────────────────────────

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
