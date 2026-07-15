use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, warn};

use crate::dsl::VariablePath;
use crate::error::{WeaveError, WeaveResult};
use crate::vm::Scope;

pub fn resolve_inputs(
    scope: &Scope,
    step: &crate::dsl::StepDef,
) -> WeaveResult<(Arc<Value>, Value)> {
    let as_name = step.iterate.as_ref().map(|c| c.as_name.as_str());

    let op_value = serde_json::to_value(&step.op)
        .map_err(|e| WeaveError::Internal(format!("step op serialize: {e}")))?;

    resolve_value_tree(scope, &op_value, as_name)
}

fn resolve_value_tree(
    scope: &Scope,
    val: &Value,
    as_name: Option<&str>,
) -> WeaveResult<(Arc<Value>, Value)> {
    match val {
        Value::Object(map) => {
            if map.len() == 1 && map.contains_key("Ref") {
                let path: VariablePath = serde_json::from_value(map["Ref"].clone())
                    .map_err(|e| WeaveError::Internal(format!("ref parse: {e}")))?;
                if let Some(as_name) = as_name
                    && path.parts.first().map(|p| p.as_str()) == Some(as_name)
                {
                    return Ok((Arc::new(Value::Null), Value::String(format!("{{{}}}", path.parts.join(".")))));
                }
                let value = resolve_ref(scope, &path)?;
                let resolved = (*value).clone();
                return Ok((value, resolved));
            }

            if map.len() == 1 && map.contains_key("Literal") {
                let lit = &map["Literal"];
                let (d, resolved) = resolve_value_tree(scope, lit, as_name)?;
                let data = if resolved.is_null() {
                    Arc::new(Value::Null)
                } else if d.is_null() {
                    Arc::new(resolved.clone())
                } else {
                    d
                };
                return Ok((data, resolved));
            }

            let mut data: Arc<Value> = Arc::new(Value::Null);
            let mut config_map = serde_json::Map::new();

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

            for (k, v) in map {
                let (d, resolved) = resolve_value_tree(scope, v, as_name)?;
                if k == "data" {
                    data = d;
                }
                config_map.insert(k.clone(), resolved);
            }
            Ok((data, Value::Object(config_map)))
        }
        Value::Array(arr) => {
            let resolved: Vec<Value> = arr
                .iter()
                .map(|v| resolve_value_tree(scope, v, as_name).map(|(_, val)| val))
                .collect::<Result<_, _>>()?;
            Ok((Arc::new(Value::Null), Value::Array(resolved)))
        }
        other => Ok((Arc::new(Value::Null), other.clone())),
    }
}

pub fn resolve_ref(scope: &Scope, path: &VariablePath) -> WeaveResult<Arc<Value>> {
    if path.parts.is_empty() {
        return Ok(Arc::new(Value::Null));
    }

    match path.parts[0].as_str() {
        "slots" => {
            let slots_val = scope.slots();
            let mut current = &*slots_val;
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
            Ok(Arc::new(current.clone()))
        }
        "env" => {
            let val = if path.parts.len() >= 2 {
                std::env::var(&path.parts[1]).unwrap_or_default()
            } else {
                String::new()
            };
            Ok(Arc::new(Value::String(val)))
        }
        _ => {
            let step_id = &path.parts[0];
            let value = scope.get_output(step_id).ok_or_else(|| {
                WeaveError::Internal(format!("step {step_id} not found in scope"))
            })?;

            if path.parts.len() == 1 || (path.parts.len() == 2 && path.parts[1] == "output") {
                Ok(value)
            } else {
                let mut current = &*value;
                let start = if path.parts.len() >= 2 && path.parts[1] == "output" {
                    2
                } else {
                    1
                };
                for part in &path.parts[start..] {
                    current = current.get(part).unwrap_or(&Value::Null);
                }
                Ok(Arc::new(current.clone()))
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
        let value = scope.get_output(step_id).ok_or_else(|| {
            WeaveError::Internal(format!(
                "code 模板 {{}} 引用了不存在的步骤: {step_id}"
            ))
        })?;

        let resolved =
            if parts.len() <= 1 || (parts.len() == 2 && parts[1] == "output") {
                match &*value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                }
            } else {
                let mut current = &*value;
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
