use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, warn};

use crate::dsl::{StepId, VariablePath};
use crate::error::{WeaveError, WeaveResult};
use crate::vm::Scope;

pub fn resolve_inputs(
    scope: &Scope,
    step: &crate::dsl::StepDef,
) -> WeaveResult<Value> {
    let as_name = step.iterate.as_ref().map(|c| c.as_name.as_str());

    let op_value = serde_json::to_value(&step.op)
        .map_err(|e| WeaveError::Internal(format!("step op serialize: {e}")))?;

    resolve_value_tree(scope, &op_value, as_name)
}

fn resolve_value_tree(
    scope: &Scope,
    val: &Value,
    as_name: Option<&str>,
) -> WeaveResult<Value> {
    match val {
        Value::Object(map) => {
            if map.len() == 1 && map.contains_key("Ref") {
                let path: VariablePath = serde_json::from_value(map["Ref"].clone())
                    .map_err(|e| WeaveError::Internal(format!("ref parse: {e}")))?;
                if let Some(as_name) = as_name
                    && path.parts.first().map(|p| p.as_str()) == Some(as_name)
                {
                    return Ok(Value::String(format!("{{{}}}", path.parts.join("."))));
                }
                let value = resolve_ref(scope, &path)?;
                Ok((*value).clone())
            } else if map.len() == 1 && map.contains_key("Literal") {
                let lit = &map["Literal"];
                resolve_value_tree(scope, lit, as_name)
            } else if let Some(inputs_val) = map.get("inputs") {
                resolve_value_tree(scope, inputs_val, as_name)
            } else {
                let mut resolved_map = serde_json::Map::new();
                for (k, v) in map {
                    resolved_map.insert(k.clone(), resolve_value_tree(scope, v, as_name)?);
                }
                Ok(Value::Object(resolved_map))
            }
        }
        Value::Array(arr) => {
            let resolved: Vec<Value> = arr
                .iter()
                .map(|v| resolve_value_tree(scope, v, as_name))
                .collect::<Result<_, _>>()?;
            Ok(Value::Array(resolved))
        }
        other => Ok(other.clone()),
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
            scope.record_env_value(&val);
            Ok(Arc::new(Value::String(val)))
        }
        _ => {
            let step_id = StepId::from(path.parts[0].clone());
            let value = scope.get_output(&step_id).ok_or_else(|| {
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
