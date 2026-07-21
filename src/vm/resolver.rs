use std::sync::Arc;

use serde_json::Value;
use tracing::{debug, warn};

use crate::dsl::{StepId, TemplatePart, VariablePath};
use crate::error::{WeaveflowError, WeaveflowResult};
use crate::vm::Scope;

/// iterate 逐 chunk 解析时的元素绑定：`(as_name, 当前元素)`。
/// 命中 `{as_name...}` 前缀的引用从元素钻取，等同于把元素注入 op 级 scope 根。
pub type Locals<'a> = Option<(&'a str, &'a Value)>;

pub fn resolve_inputs(scope: &Scope, step: &crate::dsl::StepDef) -> WeaveflowResult<Value> {
    let as_name = step.iterate.as_ref().map(|c| c.as_name.as_str());

    let op_value = serde_json::to_value(&step.op)
        .map_err(|e| WeaveflowError::Internal(format!("step op serialize: {e}")))?;

    resolve_value_tree(scope, &op_value, as_name, None, true, false)
}

pub fn resolve_value_tree(
    scope: &Scope,
    val: &Value,
    as_name: Option<&str>,
    locals: Locals<'_>,
    is_top: bool,
    in_literal: bool,
) -> WeaveflowResult<Value> {
    match val {
        Value::Object(map) => {
            if map.len() == 1 && map.contains_key("Ref") {
                let parsed = serde_json::from_value::<VariablePath>(map["Ref"].clone());
                match parsed {
                    Ok(path) if !path.parts.is_empty() => {
                        resolve_path_value(scope, &path, as_name, locals)
                    }
                    // 用户数据恰好是单键 "Ref" 对象但不是合法引用标签：
                    // 按普通对象递归（与 validator/dag 的回退行为一致）。
                    _ => resolve_plain_map(scope, map, as_name, locals, in_literal),
                }
            } else if map.len() == 1 && map.contains_key("Template") {
                let parsed = serde_json::from_value::<Vec<TemplatePart>>(map["Template"].clone());
                match parsed {
                    Ok(parts) => {
                        let mut out = String::new();
                        for part in &parts {
                            match part {
                                TemplatePart::Lit(s) => out.push_str(s),
                                TemplatePart::Ref(path) if !path.parts.is_empty() => {
                                    let v = resolve_path_value(scope, path, as_name, locals)?;
                                    push_stringified(&mut out, &v);
                                }
                                TemplatePart::Ref(_) => {}
                            }
                        }
                        Ok(Value::String(out))
                    }
                    // 单键 "Template" 用户数据回退：与 "Ref" 标签同一规则。
                    Err(_) => resolve_plain_map(scope, map, as_name, locals, in_literal),
                }
            } else if !in_literal && map.len() == 1 && map.contains_key("Literal") {
                // RefValue::Literal 序列化标签只出现在算子字段位置；
                // Literal 负载内部的单键 "Literal" 对象一律视为用户数据。
                resolve_value_tree(scope, &map["Literal"], as_name, locals, false, true)
            } else if is_top {
                // 顶层 op 信封：有 "inputs" 键取其值；否则（如 noop）移除 "type"
                // 键后以剩余 map 作为 inputs。
                if let Some(inputs_val) = map.get("inputs") {
                    resolve_value_tree(scope, inputs_val, as_name, locals, false, in_literal)
                } else {
                    let mut resolved_map = serde_json::Map::new();
                    for (k, v) in map {
                        if k == "type" {
                            continue;
                        }
                        resolved_map.insert(
                            k.clone(),
                            resolve_value_tree(scope, v, as_name, locals, false, in_literal)?,
                        );
                    }
                    Ok(Value::Object(resolved_map))
                }
            } else {
                resolve_plain_map(scope, map, as_name, locals, in_literal)
            }
        }
        Value::Array(arr) => {
            let resolved: Vec<Value> = arr
                .iter()
                .map(|v| resolve_value_tree(scope, v, as_name, locals, false, in_literal))
                .collect::<Result<_, _>>()?;
            Ok(Value::Array(resolved))
        }
        other => Ok(other.clone()),
    }
}

fn resolve_plain_map(
    scope: &Scope,
    map: &serde_json::Map<String, Value>,
    as_name: Option<&str>,
    locals: Locals<'_>,
    in_literal: bool,
) -> WeaveflowResult<Value> {
    let mut resolved_map = serde_json::Map::new();
    for (k, v) in map {
        resolved_map.insert(
            k.clone(),
            resolve_value_tree(scope, v, as_name, locals, false, in_literal)?,
        );
    }
    Ok(Value::Object(resolved_map))
}

/// 解析单个变量路径：as_name 前缀命中 locals 时从当前元素钻取；
/// 无 locals（缓存 key 材料等场景）保持 `"{...}"` 占位符字面量；
/// 其余走 scope 解析。Ref 标签与 Template 片段共用。
fn resolve_path_value(
    scope: &Scope,
    path: &VariablePath,
    as_name: Option<&str>,
    locals: Locals<'_>,
) -> WeaveflowResult<Value> {
    if let Some(as_name) = as_name
        && path.parts.first().map(|p| p.as_str()) == Some(as_name)
    {
        if let Some((name, element)) = locals
            && name == as_name
        {
            return drill_down(element, &path.parts[1..], &path.parts.join("."));
        }
        return Ok(Value::String(format!("{{{}}}", path.parts.join("."))));
    }
    let value = resolve_ref(scope, path)?;
    Ok((*value).clone())
}

/// f-string 片段的字符串化规则：String 原样、Null → 空串、
/// 数字/布尔/对象/数组 → 紧凑 JSON（`Value::to_string`）。
fn push_stringified(out: &mut String, v: &Value) {
    match v {
        Value::String(s) => out.push_str(s),
        Value::Null => {}
        other => out.push_str(&other.to_string()),
    }
}

/// 从 root 按路径段钻取：数组索引严格（非数字/越界 → 硬错误），
/// 对象缺字段 / 非对象上取段 → Null + warn。slots / step output / locals 三处共用。
fn drill_down(root: &Value, parts: &[String], path_display: &str) -> WeaveflowResult<Value> {
    let mut current = root;
    for part in parts {
        match current {
            Value::Array(arr) => {
                let idx = part.parse::<usize>().map_err(|_| {
                    WeaveflowError::Internal(format!(
                        "ref path {path_display} segment '{part}' is not a valid array index"
                    ))
                })?;
                current = arr.get(idx).ok_or_else(|| {
                    WeaveflowError::Internal(format!(
                        "ref path {path_display} array index {idx} out of bounds (len {})",
                        arr.len()
                    ))
                })?;
            }
            Value::Object(map) => match map.get(part) {
                Some(v) => current = v,
                None => {
                    warn!(
                        ref_path = %path_display,
                        missing_part = %part,
                        "ref path field not found, using Null"
                    );
                    return Ok(Value::Null);
                }
            },
            _ => {
                warn!(
                    ref_path = %path_display,
                    missing_part = %part,
                    "ref path segment on non-object, using Null"
                );
                return Ok(Value::Null);
            }
        }
    }
    Ok(current.clone())
}

pub fn resolve_ref(scope: &Scope, path: &VariablePath) -> WeaveflowResult<Arc<Value>> {
    if path.parts.is_empty() {
        return Ok(Arc::new(Value::Null));
    }

    match path.parts[0].as_str() {
        "slots" => {
            let slots_val = scope.slots();
            let value = drill_down(&slots_val, &path.parts[1..], &path.parts.join("."))?;
            debug!(
                ref_path = %path.parts.join("."),
                "resolved slot ref"
            );
            Ok(Arc::new(value))
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
                WeaveflowError::Internal(format!("step {step_id} not found in scope"))
            })?;

            if path.parts.len() == 1 || (path.parts.len() == 2 && path.parts[1] == "output") {
                Ok(value)
            } else {
                let start = if path.parts.len() >= 2 && path.parts[1] == "output" {
                    2
                } else {
                    1
                };
                let drilled = drill_down(&value, &path.parts[start..], &path.parts.join("."))?;
                Ok(Arc::new(drilled))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn scope_with_step(value: Value) -> Scope {
        let mut scope = Scope::new(HashMap::new());
        scope.set_output(&StepId::from("s"), value);
        scope
    }

    #[test]
    fn slots_array_index_resolves() {
        let mut slots = HashMap::new();
        slots.insert("items".to_string(), serde_json::json!([{ "name": "x" }]));
        let scope = Scope::new(slots);
        let path = VariablePath::parse("{slots.items.0.name}").unwrap();
        let value = resolve_ref(&scope, &path).unwrap();
        assert_eq!(*value, Value::String("x".to_string()));
    }

    #[test]
    fn slots_array_index_out_of_bounds_is_hard_error() {
        let mut slots = HashMap::new();
        slots.insert("items".to_string(), serde_json::json!([1]));
        let scope = Scope::new(slots);
        let path = VariablePath::parse("{slots.items.5}").unwrap();
        assert!(resolve_ref(&scope, &path).is_err());
    }

    #[test]
    fn slots_missing_field_returns_null() {
        let mut slots = HashMap::new();
        slots.insert("cfg".to_string(), serde_json::json!({ "a": 1 }));
        let scope = Scope::new(slots);
        let path = VariablePath::parse("{slots.cfg.missing}").unwrap();
        let value = resolve_ref(&scope, &path).unwrap();
        assert_eq!(*value, Value::Null);
    }

    #[test]
    fn single_key_ref_user_data_passes_through() {
        // 用户数据恰好是单键 "Ref" 对象但值不是合法 VariablePath：
        // 必须按普通数据透传，而不是硬错误。
        let scope = scope_with_step(serde_json::json!({}));
        let input = serde_json::json!({ "body": { "Ref": 123 } });
        let out = resolve_value_tree(&scope, &input, None, None, false, false).unwrap();
        assert_eq!(out, serde_json::json!({ "body": { "Ref": 123 } }));
    }

    #[test]
    fn nested_literal_key_user_data_not_unwrapped() {
        // Literal 负载内部的单键 "Literal" 对象是用户数据，不得拆包。
        let scope = scope_with_step(serde_json::json!({}));
        let input = serde_json::json!({
            "Literal": { "payload": { "Literal": 5 } }
        });
        let out = resolve_value_tree(&scope, &input, None, None, false, false).unwrap();
        assert_eq!(out, serde_json::json!({ "payload": { "Literal": 5 } }));
    }

    #[test]
    fn array_index_path_resolves_element_field() {
        let scope = scope_with_step(serde_json::json!([
            { "name": "a" },
            { "name": "b" }
        ]));
        let path = VariablePath::parse("{s.output.1.name}").unwrap();
        let value = resolve_ref(&scope, &path).unwrap();
        assert_eq!(*value, Value::String("b".to_string()));
    }

    #[test]
    fn array_index_out_of_bounds_returns_error() {
        let scope = scope_with_step(serde_json::json!([1, 2]));
        let path = VariablePath::parse("{s.output.5}").unwrap();
        let err = resolve_ref(&scope, &path).expect_err("out of bounds must error");
        assert!(err.to_string().contains("s.output.5"), "err: {err}");
    }

    #[test]
    fn array_index_non_numeric_returns_error() {
        let scope = scope_with_step(serde_json::json!([1, 2]));
        let path = VariablePath::parse("{s.output.name}").unwrap();
        let err = resolve_ref(&scope, &path).expect_err("non-numeric index must error");
        assert!(err.to_string().contains("name"), "err: {err}");
    }

    #[test]
    fn missing_object_field_returns_null() {
        let scope = scope_with_step(serde_json::json!({ "a": 1 }));
        let path = VariablePath::parse("{s.output.missing}").unwrap();
        let value = resolve_ref(&scope, &path).expect("missing field resolves to Null");
        assert_eq!(*value, Value::Null);
    }

    #[test]
    fn locals_whole_element_resolves() {
        let scope = scope_with_step(serde_json::json!({}));
        let element = serde_json::json!({"id": 7});
        let input = serde_json::json!({"Ref": {"parts": ["item"]}});
        let out = resolve_value_tree(
            &scope,
            &input,
            Some("item"),
            Some(("item", &element)),
            false,
            false,
        )
        .unwrap();
        assert_eq!(out, element);
    }

    #[test]
    fn locals_field_drill_down_resolves() {
        let scope = scope_with_step(serde_json::json!({}));
        let element = serde_json::json!({"user": {"name": "ann"}});
        let input = serde_json::json!({"Ref": {"parts": ["item", "user", "name"]}});
        let out = resolve_value_tree(
            &scope,
            &input,
            Some("item"),
            Some(("item", &element)),
            false,
            false,
        )
        .unwrap();
        assert_eq!(out, serde_json::json!("ann"));
    }

    #[test]
    fn locals_array_index_strict() {
        let scope = scope_with_step(serde_json::json!({}));
        let element = serde_json::json!([1, 2]);
        let input = serde_json::json!({"Ref": {"parts": ["item", "5"]}});
        let result = resolve_value_tree(
            &scope,
            &input,
            Some("item"),
            Some(("item", &element)),
            false,
            false,
        );
        assert!(result.is_err(), "out-of-bounds locals index must error");
    }

    #[test]
    fn locals_absent_as_ref_stays_literal_placeholder() {
        // 无 locals（缓存 key 材料路径）：as_name 引用透传为 "{...}" 字面量。
        let scope = scope_with_step(serde_json::json!({}));
        let input = serde_json::json!({"Ref": {"parts": ["item", "id"]}});
        let out = resolve_value_tree(&scope, &input, Some("item"), None, false, false).unwrap();
        assert_eq!(out, serde_json::json!("{item.id}"));
    }

    #[test]
    fn template_resolves_to_concatenated_string() {
        let scope = scope_with_step(serde_json::json!({"code": 0, "items": [1, 2]}));
        let input = serde_json::json!({
            "Template": [
                {"lit": "code="},
                {"ref": {"parts": ["s", "output", "code"]}},
                {"lit": "&items="},
                {"ref": {"parts": ["s", "output", "items"]}},
            ]
        });
        let out = resolve_value_tree(&scope, &input, None, None, false, false).unwrap();
        assert_eq!(out, serde_json::json!("code=0&items=[1,2]"));
    }

    #[test]
    fn template_null_becomes_empty_string() {
        let scope = scope_with_step(serde_json::json!({"a": 1}));
        let input = serde_json::json!({
            "Template": [
                {"lit": "["},
                {"ref": {"parts": ["s", "output", "missing"]}},
                {"lit": "]"},
            ]
        });
        let out = resolve_value_tree(&scope, &input, None, None, false, false).unwrap();
        assert_eq!(out, serde_json::json!("[]"));
    }

    #[test]
    fn template_locals_ref_drills_into_element() {
        let scope = scope_with_step(serde_json::json!({}));
        let element = serde_json::json!({"name": "ann"});
        let input = serde_json::json!({
            "Template": [
                {"lit": "hello "},
                {"ref": {"parts": ["item", "name"]}},
            ]
        });
        let out = resolve_value_tree(
            &scope,
            &input,
            Some("item"),
            Some(("item", &element)),
            false,
            false,
        )
        .unwrap();
        assert_eq!(out, serde_json::json!("hello ann"));
    }

    #[test]
    fn template_locals_absent_keeps_placeholder() {
        let scope = scope_with_step(serde_json::json!({}));
        let input = serde_json::json!({
            "Template": [
                {"lit": "x="},
                {"ref": {"parts": ["item", "id"]}},
            ]
        });
        let out = resolve_value_tree(&scope, &input, Some("item"), None, false, false).unwrap();
        assert_eq!(out, serde_json::json!("x={item.id}"));
    }

    #[test]
    fn template_single_key_user_data_passes_through() {
        // 单键 "Template" 但值不是合法模板标签：按普通对象递归。
        let scope = scope_with_step(serde_json::json!({}));
        let input = serde_json::json!({"body": {"Template": 123}});
        let out = resolve_value_tree(&scope, &input, None, None, false, false).unwrap();
        assert_eq!(out, serde_json::json!({"body": {"Template": 123}}));
    }

    #[test]
    fn template_nested_inside_literal_object() {
        let scope = scope_with_step(serde_json::json!("tok123"));
        let input = serde_json::json!({
            "Literal": {
                "headers": {
                    "Template": [
                        {"lit": "Bearer "},
                        {"ref": {"parts": ["s", "output"]}},
                    ]
                }
            }
        });
        let out = resolve_value_tree(&scope, &input, None, None, false, false).unwrap();
        assert_eq!(out, serde_json::json!({"headers": "Bearer tok123"}));
    }
}
