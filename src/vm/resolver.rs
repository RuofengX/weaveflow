use std::collections::HashSet;

use serde_json::Value;
use tracing::{debug, warn};

use crate::dsl::pipeline::{parse_template, RefValue, VariableRef};
use crate::error::{WeaveError, WeaveResult};
use crate::vm::Scope;

pub fn resolve_inputs(
    scope: &Scope,
    step: &crate::dsl::pipeline::StepDef,
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

pub fn resolve_ref(scope: &Scope, var: &VariableRef) -> WeaveResult<Vec<u8>> {
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

pub fn resolve_code_templates(code: &str, scope: &Scope) -> WeaveResult<String> {
    let re = regex::Regex::new(r"\{\{([a-zA-Z_][\w.]*)\}\}")
        .map_err(|e| WeaveError::Internal(format!("code template regex: {e}")))?;

    let mut result = code.to_string();
    for cap in re.captures_iter(code) {
        let ref_expr = &cap[1];
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

        let resolved = if parts.len() <= 1 || (parts.len() == 2 && parts[1] == "output") {
            String::from_utf8(bytes).map_err(|e| {
                WeaveError::Internal(format!("code 模板 {step_id}.output 不是 UTF-8: {e}"))
            })?
        } else {
            let v: Value = serde_json::from_slice(&bytes).map_err(|e| {
                WeaveError::Internal(format!("code 模板 {step_id} JSON 解析: {e}"))
            })?;
            let mut current = &v;
            let start = if parts[1] == "output" { 2 } else { 1 };
            for part in &parts[start..] {
                current = current.get(part).unwrap_or(&Value::Null);
            }
            match current {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        };

        result = result.replace(&cap[0], &resolved);
    }

    Ok(result)
}

/// 从 code 中提取 `{{step_id.output}}` 双花括号模板引用中的 step_id。
/// 用于 DAG 依赖分析。
pub fn extract_code_template_deps(code: &str, known_steps: &HashSet<String>) -> Vec<String> {
    let re = regex::Regex::new(r"\{\{([a-zA-Z_][\w.]*)\}\}").unwrap();
    let mut deps = Vec::new();
    for cap in re.captures_iter(code) {
        let ref_expr = &cap[1];
        if let Some(step_id) = ref_expr.split('.').next()
            && known_steps.contains(step_id) {
                deps.push(step_id.to_string());
            }
    }
    deps.sort();
    deps.dedup();
    deps
}
