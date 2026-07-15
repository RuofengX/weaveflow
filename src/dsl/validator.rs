use std::collections::{HashMap, HashSet};

use crate::dsl::{PipelineDef, RefValue, StepOp, VariablePath};
use serde_json::Value;

// ---------------------------------------------------------------------------
// 校验报告
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
}

impl ValidationReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug)]
pub struct ValidationError {
    pub code: String,
    pub message: String,
}

#[derive(Debug)]
pub struct ValidationWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Default)]
pub struct ValidateOptions {
    pub allow_warnings: Vec<String>,
}

impl ValidateOptions {
    pub fn is_warning_allowed(&self, code: &str) -> bool {
        self.allow_warnings.iter().any(|w| w == code)
    }
}

// ---------------------------------------------------------------------------
// 主校验入口
// ---------------------------------------------------------------------------

pub fn validate(def: &PipelineDef, _options: &ValidateOptions) -> ValidationReport {
    let mut report = ValidationReport::default();

    // ---- 0. 基本结构 ----
    if def.name.is_empty() {
        report.errors.push(ValidationError {
            code: "empty_pipeline_name".into(),
            message: "Pipeline 名称不能为空".into(),
        });
    }
    if def.steps.is_empty() {
        report.errors.push(ValidationError {
            code: "no_steps".into(),
            message: "Pipeline 必须包含至少一个步骤".into(),
        });
    }

    let all_step_ids: HashSet<&str> = def.steps.iter().map(|s| s.id.as_str()).collect();

    // ---- 1. 步骤 ----
    let mut seen_ids = HashSet::new();
    for step in &def.steps {
        if step.id.is_empty() {
            report.errors.push(ValidationError {
                code: "empty_step_id".into(),
                message: "步骤 ID 不能为空".into(),
            });
        }
        if !step.id.is_empty() && !seen_ids.insert(&step.id) {
            report.errors.push(ValidationError {
                code: "duplicate_step_id".into(),
                message: format!("步骤 ID 重复: {}", step.id),
            });
        }

        // after
        if let Some(ref after) = step.after {
            for dep in after {
                if dep == &step.id {
                    report.errors.push(ValidationError {
                        code: "after_self_ref".into(),
                        message: format!("步骤 {} 的 after 不能引用自身", step.id),
                    });
                }
            }
            let mut seen_after: HashSet<&str> = HashSet::new();
            for dep in after {
                if !seen_after.insert(dep) {
                    report.errors.push(ValidationError {
                        code: "duplicate_after_entry".into(),
                        message: format!("步骤 {} 的 after 中存在重复: {}", step.id, dep),
                    });
                }
            }
        }

        // 检查 inputs 中的变量引用
        {
            let op_val =
                serde_json::to_value(&step.op).unwrap_or(Value::Null);
            check_ref_in_json(&op_val, &all_step_ids, &step.id, &mut report);
        }
        check_iterate_config(step.iterate.as_ref(), &step.id, &all_step_ids, &mut report);
    }

    // ---- 2. slots ----
    let mut seen_slots = HashSet::new();
    for slot in &def.slots {
        if slot.name.is_empty() {
            report.errors.push(ValidationError {
                code: "empty_slot_name".into(),
                message: "slot 名称不能为空".into(),
            });
        }
        if !slot.name.is_empty() && !seen_slots.insert(&slot.name) {
            report.errors.push(ValidationError {
                code: "duplicate_slot_name".into(),
                message: format!("slot 名称重复: {}", slot.name),
            });
        }
    }

    // ---- 3. after 引用存在性 ----
    for step in &def.steps {
        if let Some(ref after) = step.after {
            for dep in after {
                if dep != &step.id && !all_step_ids.contains(dep.as_str()) {
                    report.errors.push(ValidationError {
                        code: "after_ref_not_found".into(),
                        message: format!(
                            "步骤 {} 的 after 引用了不存在的步骤: {}",
                            step.id, dep
                        ),
                    });
                }
            }
        }
    }

    // ---- 4. output 引用 ----
    if !def.steps.is_empty() {
        check_output_ref(&def.output, &all_step_ids, &mut report);
    }

    // ---- 5. JSON Schema ----
    for slot in &def.slots {
        if let Err(e) = jsonschema::compile(&slot.schema) {
            report.errors.push(ValidationError {
                code: "invalid_json_schema".into(),
                message: format!("slot {} 的 schema 非法: {}", slot.name, e),
            });
        }
    }

    // ---- 6. 未使用声明 (warning) ----
    let used_slots = collect_slots_used(def);
    for slot in &def.slots {
        if !used_slots.contains(&slot.name) {
            report.warnings.push(ValidationWarning {
                code: "unused_slot".into(),
                message: format!("slot {} 已声明但未在任何步骤中使用", slot.name),
            });
        }
    }

    // orphan step check
    let mut referenced_steps: HashSet<String> = HashSet::new();
    for step in &def.steps {
        let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
        for (prefix, _) in refs_in_json(&op_val) {
            if prefix != "slots" && prefix != "env" {
                referenced_steps.insert(prefix);
            }
        }
        if let Some(ref after) = step.after {
            for dep in after {
                referenced_steps.insert(dep.clone());
            }
        }
    }
    for (prefix, _) in output_refs(&def.output) {
        if prefix != "slots" && prefix != "env" {
            referenced_steps.insert(prefix);
        }
    }

    for step in &def.steps {
        let is_output_target = output_refs(&def.output)
            .iter()
            .any(|(p, _)| p == &step.id);
        let is_referenced = referenced_steps.contains(&step.id) || is_output_target;
        if !is_referenced {
            report.warnings.push(ValidationWarning {
                code: "orphan_step".into(),
                message: format!("步骤 {} 未被任何下游步骤或 output 引用", step.id),
            });
        }
    }

    // ---- 7. JS syntax check ----
    for step in &def.steps {
        if let StepOp::Js(ref inputs) = step.op {
            check_js_syntax(&step.id, &inputs.code, &mut report);
        }
    }

    // ---- 8. Rule validation ----
    let mut seen_rule_ids = HashSet::new();
    for rule in &def.rules {
        if !seen_rule_ids.insert(rule.id.as_str()) {
            report.errors.push(ValidationError {
                code: "duplicate_rule_id".into(),
                message: format!("规则 ID 重复: {}", rule.id),
            });
        }
        if rule.r#type == "js" && let Some(ref code) = rule.code {
            check_js_syntax(&rule.id, code, &mut report);
        }
    }

    // ---- 9. 无上游依赖检查 (warning) ----
    let mut upstream_deps: HashMap<&str, Vec<String>> = HashMap::new();
    for step in &def.steps {
        let sid = step.id.as_str();
        if let Some(ref after) = step.after {
            for dep in after {
                upstream_deps.entry(sid).or_default().push(dep.clone());
            }
        }
        {
            let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
            for (prefix, _) in refs_in_json(&op_val) {
                if prefix != "slots" && prefix != "env" {
                    upstream_deps.entry(sid).or_default().push(prefix);
                }
            }
        }
        if let Some(ref cfg) = step.iterate {
            for (prefix, _) in refs_in_path(&cfg.over) {
                if prefix != "slots" && prefix != "env" {
                    upstream_deps.entry(sid).or_default().push(prefix);
                }
            }
        }
    }

    for step in &def.steps {
        if !upstream_deps.contains_key(step.id.as_str()) {
            report.warnings.push(ValidationWarning {
                code: "no_upstream_deps".into(),
                message: format!(
                    "步骤 {} 没有上游依赖（无 after / inputs 引用 / iterate.over），将作为 DAG 根节点在 layer 0 并行执行",
                    step.id
                ),
            });
        }
    }

    report
}

// ---------------------------------------------------------------------------
// Helpers — JSON-based ref extraction
// ---------------------------------------------------------------------------

fn collect_slots_used(def: &PipelineDef) -> HashSet<String> {
    let mut set = HashSet::new();
    for step in &def.steps {
        let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
        for (prefix, rest) in refs_in_json(&op_val) {
            if prefix == "slots" && !rest.is_empty() {
                set.insert(rest);
            }
        }
    }
    for (prefix, rest) in output_refs(&def.output) {
        if prefix == "slots" && !rest.is_empty() {
            set.insert(rest);
        }
    }
    set
}

fn output_refs(output: &RefValue) -> Vec<(String, String)> {
    match output {
        RefValue::Ref(path) => {
            if path.parts.is_empty() {
                return vec![];
            }
            let prefix = path.parts[0].clone();
            let rest = path.parts[1..].join(".");
            vec![(prefix, rest)]
        }
        RefValue::Literal(lit) => refs_in_json(lit),
    }
}

fn refs_in_path(path: &VariablePath) -> Vec<(String, String)> {
    if path.parts.is_empty() {
        return vec![];
    }
    let prefix = path.parts[0].clone();
    let rest = path.parts[1..].join(".");
    vec![(prefix, rest)]
}

fn refs_in_json(val: &Value) -> Vec<(String, String)> {
    match val {
        Value::Object(map) if map.contains_key("Ref") => {
            if let Some(ref_val) = map.get("Ref")
                && let Ok(path) = serde_json::from_value::<VariablePath>(ref_val.clone())
                && !path.parts.is_empty()
            {
                let prefix = path.parts[0].clone();
                let rest = path.parts[1..].join(".");
                vec![(prefix, rest)]
            } else {
                vec![]
            }
        }
        Value::Object(map) => {
            let mut all = Vec::new();
            for v in map.values() {
                all.extend(refs_in_json(v));
            }
            all
        }
        Value::String(s) => extract_refs(s),
        Value::Array(arr) => {
            let mut all = Vec::new();
            for v in arr {
                all.extend(refs_in_json(v));
            }
            all
        }
        _ => vec![],
    }
}

fn extract_refs(s: &str) -> Vec<(String, String)> {
    let mut refs = Vec::new();
    let mut chars = s.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch == '{' {
            let mut content = String::new();
            for (_, c) in chars.by_ref() {
                if c == '}' {
                    break;
                }
                content.push(c);
            }
            if let Some(dot) = content.find('.') {
                let prefix = content[..dot].trim();
                if prefix.is_empty() || !prefix.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    continue;
                }
                let rest = content[dot + 1..].to_string();
                if !rest.is_empty() {
                    refs.push((prefix.to_string(), rest));
                }
            }
        }
    }
    refs
}

fn check_ref_in_json(
    val: &Value,
    all_ids: &HashSet<&str>,
    step_id: &str,
    report: &mut ValidationReport,
) {
    match val {
        Value::Object(map) if map.contains_key("Ref") => {
            if let Some(ref_val) = map.get("Ref")
                && let Ok(path) = serde_json::from_value::<VariablePath>(ref_val.clone())
                && let Some(prefix) = path.parts.first()
            {
                if prefix != "slots" && prefix != "env" && !all_ids.contains(prefix.as_str()) {
                    report.errors.push(ValidationError {
                        code: "variable_ref_not_found".into(),
                        message: format!(
                            "步骤 {} 中引用了不存在的步骤: {}",
                            step_id, prefix
                        ),
                    });
                }
            }
        }
        Value::Object(map) => {
            for v in map.values() {
                check_ref_in_json(v, all_ids, step_id, report);
            }
        }
        Value::String(s) => {
            for (prefix, _path) in extract_refs(s) {
                if prefix != "slots" && prefix != "env" && !all_ids.contains(prefix.as_str()) {
                    report.errors.push(ValidationError {
                        code: "variable_ref_not_found".into(),
                        message: format!(
                            "步骤 {} 中引用了不存在的步骤: {}",
                            step_id, prefix
                        ),
                    });
                }
            }
        }
        Value::Array(arr) => {
            for v in arr {
                check_ref_in_json(v, all_ids, step_id, report);
            }
        }
        _ => {}
    }
}

fn check_output_ref(
    output: &RefValue,
    all_ids: &HashSet<&str>,
    report: &mut ValidationReport,
) {
    match output {
        RefValue::Ref(path) => {
            if let Some(prefix) = path.parts.first()
                && prefix != "slots" && prefix != "env" && !all_ids.contains(prefix.as_str()) {
                    report.errors.push(ValidationError {
                        code: "output_ref_not_found".into(),
                        message: format!("output 引用了不存在的步骤: {}", prefix),
                    });
            }
        }
        RefValue::Literal(lit) => {
            if let Value::String(s) = lit {
                for (prefix, _path) in extract_refs(s) {
                    if prefix != "slots" && prefix != "env" && !all_ids.contains(prefix.as_str()) {
                        report.errors.push(ValidationError {
                            code: "output_ref_not_found".into(),
                            message: format!("output 引用了不存在的步骤: {}", prefix),
                        });
                    }
                }
            }
        }
    }
}

fn check_iterate_config(
    cfg: Option<&crate::dsl::IterateConfig>,
    step_id: &str,
    all_step_ids: &HashSet<&str>,
    report: &mut ValidationReport,
) {
    if let Some(cfg) = cfg {
        if cfg.max_workers == Some(0) {
            report.errors.push(ValidationError {
                code: "invalid_iterate_config".into(),
                message: format!(
                    "步骤 {} 的 iterate.max_workers 不能为 0（省缺可移除该字段）",
                    step_id
                ),
            });
        }
        if let Some(prefix) = cfg.over.parts.first()
            && prefix != "slots" && prefix != "env" && !all_step_ids.contains(prefix.as_str()) {
                report.errors.push(ValidationError {
                    code: "variable_ref_not_found".into(),
                    message: format!(
                        "步骤 {} 的 iterate.over 引用了不存在的步骤: {}",
                        step_id, prefix
                    ),
                });
        }
    }
}

fn check_js_syntax(id: &str, code: &str, report: &mut ValidationReport) {
    let re = regex::Regex::new(r"\{\{[a-zA-Z_][\w.]*\}\}")
        .expect("template regex");
    let sanitized = re.replace_all(code, "\"__placeholder__\"");
    let script = format!(
        "{sanitized}\ntry {{ var __check__ = function(){{}}; }} catch(__e__) {{}}\n"
    );
    let rt = match rquickjs::Runtime::new() {
        Ok(r) => r,
        Err(_) => {
            report.errors.push(ValidationError {
                code: "js_runtime_error".into(),
                message: format!("步骤/规则 {} 的 JS 运行时创建失败", id),
            });
            return;
        }
    };
    let ctx = match rquickjs::Context::full(&rt) {
        Ok(c) => c,
        Err(_) => {
            report.errors.push(ValidationError {
                code: "js_runtime_error".into(),
                message: format!("步骤/规则 {} 的 JS 上下文创建失败", id),
            });
            return;
        }
    };
    ctx.with(|ctx| {
        if let Err(e) = ctx.eval::<rquickjs::Value, _>(script.as_str()) {
            report.errors.push(ValidationError {
                code: "js_syntax_error".into(),
                message: format!("步骤/规则 {} 的 JS 代码语法错误: {}", id, e),
            });
        }
    });
}

// ---------------------------------------------------------------------------
// 单元测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::step_op::*;
    use crate::dsl::*;
    use serde_json::json;

    fn step_http(url: &str) -> StepDef {
        StepDef {
            id: "fetch".into(),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout: None,
            op: StepOp::Http(HttpInputs {
                url: var_ref(url),
                method: None,
                headers: None,
                body: None,
            }),
        }
    }

    fn step_noop(id: &str, after: Vec<&str>) -> StepDef {
        StepDef {
            id: id.into(),
            after: if after.is_empty() { None } else { Some(after.into_iter().map(|s| s.into()).collect()) },
            iterate: None,
            cache: None,
            retry: None,
            timeout: None,
            op: StepOp::Noop,
        }
    }

    fn var_ref(s: &str) -> RefValue {
        RefValue::Ref(VariablePath::parse(s).unwrap())
    }

    fn literal(v: Value) -> RefValue {
        RefValue::Literal(v)
    }

    fn valid_def() -> PipelineDef {
        PipelineDef {
            name: "test".into(),
            description: None,
            storage: None,
            slots: vec![SlotDef {
                name: "url".into(),
                schema: json!({"type": "string"}),
            }],
            steps: vec![step_http("{slots.url}")],
            output: var_ref("{fetch.output}"),
            rules: vec![],
        }
    }

    #[test]
    fn valid_pipeline_passes() {
        let report = validate(&valid_def(), &ValidateOptions::default());
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn empty_pipeline_name() {
        let mut def = valid_def();
        def.name = "".into();
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "empty_pipeline_name"));
    }

    #[test]
    fn no_steps() {
        let mut def = valid_def();
        def.steps.clear();
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "no_steps"));
    }

    #[test]
    fn empty_step_id() {
        let mut def = valid_def();
        def.steps[0].id = "".into();
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "empty_step_id"));
    }

    #[test]
    fn duplicate_step_id() {
        let mut def = valid_def();
        def.steps.push(def.steps[0].clone());
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "duplicate_step_id"));
    }

    #[test]
    fn duplicate_slot_name() {
        let mut def = valid_def();
        def.slots.push(SlotDef {
            name: "url".into(),
            schema: json!({"type": "number"}),
        });
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "duplicate_slot_name"));
    }

    #[test]
    fn empty_slot_name() {
        let mut def = valid_def();
        def.slots[0].name = "".into();
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "empty_slot_name"));
    }

    #[test]
    fn after_self_ref() {
        let mut def = valid_def();
        def.steps[0].after = Some(vec!["fetch".into()]);
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "after_self_ref"));
    }

    #[test]
    fn after_duplicate_entry() {
        let mut def = valid_def();
        def.steps.push(step_noop("step_b", vec!["fetch", "fetch"]));
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "duplicate_after_entry"));
    }

    #[test]
    fn after_ref_not_found() {
        let mut def = valid_def();
        def.steps[0].after = Some(vec!["nonexistent".into()]);
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "after_ref_not_found"));
    }

    #[test]
    fn after_ref_found() {
        let mut def = valid_def();
        def.steps.push(step_noop("step_b", vec!["fetch"]));
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn variable_ref_not_found() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Http(HttpInputs {
            url: var_ref("{nonexistent.output}"),
            method: None,
            headers: None,
            body: None,
        });
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "variable_ref_not_found"));
    }

    #[test]
    fn output_ref_not_found() {
        let mut def = valid_def();
        def.output = var_ref("{nonexistent.output}");
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "output_ref_not_found"));
    }

    #[test]
    fn slots_env_refs_are_not_checked() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Http(HttpInputs {
            url: var_ref("{slots.source_url}"),
            method: None,
            headers: Some({
                let mut h = HashMap::new();
                h.insert("Authorization".into(), var_ref("{env.API_KEY}"));
                h
            }),
            body: None,
        });
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn empty_slots() {
        let mut def = valid_def();
        def.slots.clear();
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.is_ok());
    }

    #[test]
    fn invalid_iterate_config() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "item".into(),
            max_workers: Some(0),
            batch: None,
        });
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "invalid_iterate_config"));
    }

    #[test]
    fn valid_iterate_config() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "item".into(),
            max_workers: Some(4),
            batch: None,
        });
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn json_schema_valid() {
        let mut def = valid_def();
        def.slots[0].schema = json!({"type": "string", "minLength": 1});
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.is_ok());
    }

    #[test]
    fn nested_ref_in_object() {
        let mut def = valid_def();
        // Use VarOutput with a literal object containing a template string inside
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(json!({ "Authorization": "{nonexistent.output}" }))),
        });
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.iter().any(|e| e.code == "variable_ref_not_found"));
    }

    #[test]
    fn multiple_errors_at_once() {
        let mut def = valid_def();
        def.steps.push(def.steps[0].clone());
        def.steps[0].after = Some(vec!["ghost".into()]);
        def.output = var_ref("{ghost.output}");
        let report = validate(&def, &ValidateOptions::default());
        assert!(report.errors.len() >= 2);
    }
}
