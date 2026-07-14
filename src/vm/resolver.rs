use std::collections::HashSet;

use serde_json::Value;
use tracing::{debug, warn};

use crate::dsl::VariablePath;
use crate::error::{WeaveError, WeaveResult};
use crate::vm::Scope;

pub fn resolve_inputs(
    scope: &Scope,
    step: &crate::dsl::StepDef,
) -> WeaveResult<(Vec<u8>, Value)> {
    let as_name = step.iterate.as_ref().map(|c| c.as_name.as_str());

    // 将 step.op 序列化为 Value，然后递归解析其中的 RefValue
    let op_value = serde_json::to_value(&step.op)
        .map_err(|e| WeaveError::Internal(format!("step op serialize: {e}")))?;

    resolve_value_tree(scope, &op_value, as_name)
}

/// 递归遍历 Value 树，解析 RefValue 模式，收集到 data + config 中。
fn resolve_value_tree(
    scope: &Scope,
    val: &Value,
    as_name: Option<&str>,
) -> WeaveResult<(Vec<u8>, Value)> {
    match val {
        Value::Object(map) => {
            // 先检查是否为 RefValue::Ref / RefValue::Literal 的序列化格式
            if map.len() == 1 && map.contains_key("Ref") {
                let path: VariablePath = serde_json::from_value(map["Ref"].clone())
                    .map_err(|e| WeaveError::Internal(format!("ref parse: {e}")))?;
                // iterate 中的元素变量跳过解析
                if let Some(as_name) = as_name
                    && path.parts.first().map(|p| p.as_str()) == Some(as_name)
                {
                    return Ok((vec![], Value::String(format!("{{{}}}", path.parts.join(".")))));
                }
                let bytes = resolve_ref(scope, &path)?;
                return Ok((bytes, Value::Null));
            }

            if map.len() == 1 && map.contains_key("Literal") {
                return Ok((vec![], map["Literal"].clone()));
            }

            // 顶层对象：特殊处理 "inputs" / "data" key
            let mut data: Vec<u8> = Vec::new();
            let mut config_map = serde_json::Map::new();

            // 如果存在 "inputs" key，展开它（StepOp 的 content = "inputs" 格式）
            if let Some(inputs_val) = map.get("inputs") {
                if let Value::Object(inputs_map) = inputs_val {
                    for (k, v) in inputs_map {
                        let (d, resolved) = resolve_value_tree(scope, v, as_name)?;
                        if k == "data" {
                            data = d;
                        } else {
                            config_map.insert(k.clone(), resolved);
                        }
                    }
                }
                return Ok((data, Value::Object(config_map)));
            }

            // 普通对象（如 ForkBranch.inputs）
            for (k, v) in map {
                let (d, resolved) = resolve_value_tree(scope, v, as_name)?;
                if k == "data" {
                    data = d;
                } else {
                    config_map.insert(k.clone(), resolved);
                }
            }
            Ok((data, Value::Object(config_map)))
        }
        Value::Array(arr) => {
            let resolved: Vec<Value> = arr
                .iter()
                .map(|v| resolve_value_tree(scope, v, as_name).map(|(_, val)| val))
                .collect::<Result<_, _>>()?;
            Ok((vec![], Value::Array(resolved)))
        }
        other => Ok((vec![], other.clone())),
    }
}

pub fn resolve_ref(scope: &Scope, path: &VariablePath) -> WeaveResult<Vec<u8>> {
    if path.parts.is_empty() {
        return Ok(Vec::new());
    }

    match path.parts[0].as_str() {
        "slots" => {
            let slots_bytes = scope.slots().unwrap_or_default();
            if slots_bytes.is_empty() {
                warn!(
                    "scope has no slots bytes, resolving ref {}",
                    path.parts.join(".")
                );
                return Ok(Vec::new());
            }
            let v: Value = serde_json::from_slice(&slots_bytes)
                .map_err(|e| WeaveError::Internal(format!("slots parse: {e}")))?;
            let mut current = &v;
            for part in &path.parts[1..] {
                let next = current.get(part);
                if next.is_none() {
                    warn!(
                        ref_path = %path.parts.join("."),
                        missing_part = %part,
                        available = %serde_json::to_string(current).unwrap_or_default(),
                        "slot ref path not found, using Null"
                    );
                }
                current = next.unwrap_or(&Value::Null);
            }
            debug!(
                ref_path = %path.parts.join("."),
                resolved = %serde_json::to_string(current).unwrap_or_default(),
                "resolved slot ref"
            );
            serde_json::to_vec(current)
                .map_err(|e| WeaveError::Internal(format!("ref serialize: {e}")))
        }
        "env" => {
            let val = if path.parts.len() >= 2 {
                std::env::var(&path.parts[1]).unwrap_or_default()
            } else {
                String::new()
            };
            Ok(val.into_bytes())
        }
        _ => {
            let step_id = &path.parts[0];
            let bytes = scope.get_output(step_id).ok_or_else(|| {
                WeaveError::Internal(format!("step {step_id} not found in scope"))
            })?;

            if path.parts.len() == 1 || (path.parts.len() == 2 && path.parts[1] == "output") {
                Ok(bytes)
            } else {
                let v: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| WeaveError::Internal(format!("step output parse: {e}")))?;
                let mut current = &v;
                let start = if path.parts.len() >= 2 && path.parts[1] == "output" {
                    2
                } else {
                    1
                };
                for part in &path.parts[start..] {
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

        let resolved =
            if parts.len() <= 1 || (parts.len() == 2 && parts[1] == "output") {
                String::from_utf8(bytes).map_err(|e| {
                    WeaveError::Internal(format!(
                        "code 模板 {step_id}.output 不是 UTF-8: {e}"
                    ))
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

pub fn extract_code_template_deps(code: &str, known_steps: &HashSet<String>) -> Vec<String> {
    let re = regex::Regex::new(r"\{\{([a-zA-Z_][\w.]*)\}\}").unwrap();
    let mut deps = Vec::new();
    for cap in re.captures_iter(code) {
        let ref_expr = &cap[1];
        if let Some(step_id) = ref_expr.split('.').next()
            && known_steps.contains(step_id)
        {
            deps.push(step_id.to_string());
        }
    }
    deps.sort();
    deps.dedup();
    deps
}
