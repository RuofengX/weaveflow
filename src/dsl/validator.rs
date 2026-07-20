use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::dsl::{PipelineDef, RefValue, StepDef, StepId, StepOp, VariablePath};
use serde_json::Value;
use tracing::debug;

const FILTER_OPERATORS: [&str; 8] = ["eq", "ne", "gt", "gte", "lt", "lte", "in", "contains"];
const SORT_ORDERS: [&str; 2] = ["asc", "desc"];
const MAX_ITERATE_WORKERS: u32 = 1024;
const MAX_RETRY_ATTEMPTS: u32 = 100;
const MAX_RETRY_DELAY_MS: u64 = 3_600_000;
const BASE64_MODES: [&str; 2] = ["encode", "decode"];
const HTTP_METHODS: [&str; 4] = ["GET", "POST", "PUT", "DELETE"];

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

// ---------------------------------------------------------------------------
// 主校验入口
// ---------------------------------------------------------------------------

pub fn validate(def: &PipelineDef) -> ValidationReport {
    let mut report = ValidationReport::default();

    // ---- 0. 基本结构 ----
    if def.name.is_empty() {
        report.errors.push(ValidationError {
            code: "empty_pipeline_name".into(),
            message: "Pipeline 名称不能为空".into(),
        });
    } else if !is_valid_pipeline_name(&def.name) {
        report.errors.push(ValidationError {
            code: "invalid_name_charset".into(),
            message: format!(
                "Pipeline 名称含非法字符（仅允许 [A-Za-z0-9_.-]）: {}",
                def.name
            ),
        });
    } else if def.name.chars().all(|c| c == '.') {
        report.errors.push(ValidationError {
            code: "invalid_pipeline_name".into(),
            message: format!(
                "Pipeline 名称不能全为 '.'（URL 归一化后不可达）: {}",
                def.name
            ),
        });
    }
    if def.steps.is_empty() {
        report.errors.push(ValidationError {
            code: "no_steps".into(),
            message: "Pipeline 必须包含至少一个步骤".into(),
        });
    }

    let all_step_ids: HashSet<StepId> = def.steps.iter().map(|s| s.id.clone()).collect();

    // ---- 1. 步骤 ----
    let mut seen_ids = HashSet::new();
    for step in &def.steps {
        if step.id.0.is_empty() {
            report.errors.push(ValidationError {
                code: "empty_step_id".into(),
                message: "步骤 ID 不能为空".into(),
            });
        } else if !is_valid_step_id(&step.id.0) {
            report.errors.push(ValidationError {
                code: "invalid_name_charset".into(),
                message: format!("步骤 ID 含非法字符（仅允许 [A-Za-z0-9_-]）: {}", step.id),
            });
        }
        if step.id.0 == "slots" || step.id.0 == "env" {
            report.errors.push(ValidationError {
                code: "reserved_step_id".into(),
                message: format!("步骤 ID 不能使用保留名称: {}", step.id),
            });
        }
        if !step.id.0.is_empty() && !seen_ids.insert(&step.id) {
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
            let mut seen_after: HashSet<&StepId> = HashSet::new();
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
            let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
            let as_name = step.iterate.as_ref().map(|c| c.as_name.as_str());
            check_ref_in_json(&op_val, &all_step_ids, &step.id, as_name, &mut report);
        }
        check_iterate_config(step.iterate.as_ref(), &step.id, &all_step_ids, &mut report);
        if let Some(ref retry) = step.retry {
            if retry.max_attempts == 0 {
                report.errors.push(ValidationError {
                    code: "invalid_retry_config".into(),
                    message: format!("步骤 {} 的 retry.max_attempts 不能为 0", step.id),
                });
            } else if retry.max_attempts > MAX_RETRY_ATTEMPTS {
                report.errors.push(ValidationError {
                    code: "invalid_retry_config".into(),
                    message: format!(
                        "步骤 {} 的 retry.max_attempts 超过上限 {}: {}",
                        step.id, MAX_RETRY_ATTEMPTS, retry.max_attempts
                    ),
                });
            }
            if retry.delay_ms > MAX_RETRY_DELAY_MS {
                report.errors.push(ValidationError {
                    code: "invalid_retry_config".into(),
                    message: format!(
                        "步骤 {} 的 retry.delay_ms 超过上限 {}: {}",
                        step.id, MAX_RETRY_DELAY_MS, retry.delay_ms
                    ),
                });
            }
        }
        if let Some(t) = step.timeout_sec {
            if !t.is_finite() || t <= 0.0 {
                report.errors.push(ValidationError {
                    code: "invalid_timeout".into(),
                    message: format!("步骤 {} 的 timeout_sec 必须为正数", step.id),
                });
            } else if t > 365.0 * 86400.0 {
                report.errors.push(ValidationError {
                    code: "invalid_timeout".into(),
                    message: format!(
                        "步骤 {} 的 timeout_sec 超过上限（最大 365 天 = {} 秒）",
                        step.id,
                        365 * 86400
                    ),
                });
            }
        }

        // 算子枚举白名单
        match &step.op {
            StepOp::Filter(inputs) => {
                if !FILTER_OPERATORS.contains(&inputs.operator.as_str()) {
                    report.errors.push(ValidationError {
                        code: "invalid_operator_config".into(),
                        message: format!(
                            "步骤 {} 的 filter.operator 非法: {}（可选: {}）",
                            step.id,
                            inputs.operator,
                            FILTER_OPERATORS.join("/")
                        ),
                    });
                }
            }
            StepOp::Sort(inputs) if !SORT_ORDERS.contains(&inputs.order.as_str()) => {
                report.errors.push(ValidationError {
                    code: "invalid_operator_config".into(),
                    message: format!(
                        "步骤 {} 的 sort.order 非法: {}（可选: {}）",
                        step.id,
                        inputs.order,
                        SORT_ORDERS.join("/")
                    ),
                });
            }
            _ => {}
        }

        // base64 mode / http method / llm temperature 校验
        match &step.op {
            StepOp::Base64(inputs) => {
                if let Some(ref mode) = inputs.mode
                    && !BASE64_MODES.contains(&mode.as_str())
                {
                    report.errors.push(ValidationError {
                        code: "invalid_operator_config".into(),
                        message: format!(
                            "步骤 {} 的 base64.mode 非法: {}（可选: {}）",
                            step.id,
                            mode,
                            BASE64_MODES.join("/")
                        ),
                    });
                }
            }
            StepOp::Http(inputs) => {
                if let Some(ref method) = inputs.method
                    && !HTTP_METHODS.contains(&method.to_uppercase().as_str())
                {
                    report.errors.push(ValidationError {
                        code: "invalid_operator_config".into(),
                        message: format!(
                            "步骤 {} 的 http.method 非法: {}（可选: {}）",
                            step.id,
                            method,
                            HTTP_METHODS.join("/")
                        ),
                    });
                }
            }
            StepOp::Llm(inputs) => {
                if let Some(t) = inputs.temperature
                    && !t.is_finite()
                {
                    report.errors.push(ValidationError {
                        code: "invalid_operator_config".into(),
                        message: format!(
                            "步骤 {} 的 llm.temperature 必须是有限数值: {}",
                            step.id, t
                        ),
                    });
                }
                if let Some(RefValue::Literal(_)) = &inputs.api_key {
                    report.errors.push(ValidationError {
                        code: "insecure_api_key".into(),
                        message: format!(
                            "步骤 {} 的 llm.api_key 不允许明文配置（会明文进入流水线定义）。\
                             请改用引用，如 api_key: \"{{env.OPENAI_API_KEY}}\"，\
                             或经 var/file 步骤输出注入",
                            step.id
                        ),
                    });
                }
            }
            _ => {}
        }
    }

    // ---- 2. slots ----
    let mut seen_slots = HashSet::new();
    for slot in &def.slots {
        if slot.name.is_empty() {
            report.errors.push(ValidationError {
                code: "empty_slot_name".into(),
                message: "slot 名称不能为空".into(),
            });
        } else if !is_valid_slot_name(&slot.name) {
            report.errors.push(ValidationError {
                code: "invalid_name_charset".into(),
                message: format!("slot 名称含非法字符（仅允许 [A-Za-z0-9_-]）: {}", slot.name),
            });
        }
        if !slot.name.is_empty() && !seen_slots.insert(&slot.name) {
            report.errors.push(ValidationError {
                code: "duplicate_slot_name".into(),
                message: format!("slot 名称重复: {}", slot.name),
            });
        }
    }

    // ---- 3. slot 引用存在性 ----
    let declared_slots: HashSet<&str> = def.slots.iter().map(|s| s.name.as_str()).collect();
    let mut slot_refs: Vec<(String, String)> = Vec::new();
    for step in &def.steps {
        let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
        for (prefix, rest) in refs_in_json(&op_val) {
            if prefix == "slots" {
                slot_refs.push((rest, format!("步骤 {}", step.id)));
            }
        }
        if let Some(ref cfg) = step.iterate {
            for (prefix, rest) in refs_in_path(&cfg.over) {
                if prefix == "slots" {
                    slot_refs.push((rest, format!("步骤 {} 的 iterate.over", step.id)));
                }
            }
        }
    }
    for (prefix, rest) in output_refs(&def.output) {
        if prefix == "slots" {
            slot_refs.push((rest, "output".to_string()));
        }
    }
    for (rest, context) in slot_refs {
        let name = rest.split('.').next().unwrap_or("");
        if !name.is_empty() && !declared_slots.contains(name) {
            report.errors.push(ValidationError {
                code: "slot_not_found".into(),
                message: format!("{} 引用了未声明的 slot: {}", context, name),
            });
        }
    }

    // ---- 4. after 引用存在性 ----
    for step in &def.steps {
        if let Some(ref after) = step.after {
            for dep in after {
                if dep != &step.id && !all_step_ids.contains(dep.0.as_str()) {
                    report.errors.push(ValidationError {
                        code: "after_ref_not_found".into(),
                        message: format!("步骤 {} 的 after 引用了不存在的步骤: {}", step.id, dep),
                    });
                }
            }
        }
    }

    // ---- 5. output 引用 ----
    if !def.steps.is_empty() {
        check_output_ref(&def.output, &all_step_ids, &mut report);
    }

    // ---- 6. JSON Schema ----
    for slot in &def.slots {
        if let Err(e) = jsonschema::compile(&slot.schema) {
            report.errors.push(ValidationError {
                code: "invalid_json_schema".into(),
                message: format!("slot {} 的 schema 非法: {}", slot.name, e),
            });
        }
    }

    // ---- 7. 未使用声明 (warning) ----
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
    let mut referenced_steps: HashSet<StepId> = HashSet::new();
    for step in &def.steps {
        let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
        for (prefix, _) in refs_in_json(&op_val) {
            if prefix != "slots" && prefix != "env" {
                referenced_steps.insert(StepId::from(prefix));
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
            referenced_steps.insert(StepId::from(prefix));
        }
    }

    for step in &def.steps {
        let is_output_target = output_refs(&def.output)
            .iter()
            .any(|(p, _)| StepId::from(p.as_str()) == step.id);
        let is_referenced = referenced_steps.contains(&step.id) || is_output_target;
        if !is_referenced {
            report.warnings.push(ValidationWarning {
                code: "orphan_step".into(),
                message: format!("步骤 {} 未被任何下游步骤或 output 引用", step.id),
            });
        }
    }

    // ---- 8. JS syntax check ----
    for step in &def.steps {
        if let StepOp::Js(ref inputs) = step.op {
            check_js_syntax(&step.id, &inputs.code, &mut report);
        }
    }

    // ---- 9. 无上游依赖检查 (warning) ----
    let mut upstream_deps: HashMap<StepId, Vec<StepId>> = HashMap::new();
    for step in &def.steps {
        let sid = step.id.clone();
        if let Some(ref after) = step.after {
            for dep in after {
                upstream_deps
                    .entry(sid.clone())
                    .or_default()
                    .push(dep.clone());
            }
        }
        {
            let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
            for (prefix, _) in refs_in_json(&op_val) {
                if prefix != "slots" && prefix != "env" {
                    upstream_deps
                        .entry(sid.clone())
                        .or_default()
                        .push(StepId::from(prefix));
                }
            }
        }
        if let Some(ref cfg) = step.iterate {
            for (prefix, _) in refs_in_path(&cfg.over) {
                if prefix != "slots" && prefix != "env" {
                    upstream_deps
                        .entry(sid.clone())
                        .or_default()
                        .push(StepId::from(prefix));
                }
            }
        }
    }

    for step in &def.steps {
        if !upstream_deps.contains_key(&step.id) {
            report.warnings.push(ValidationWarning {
                code: "no_upstream_deps".into(),
                message: format!(
                    "步骤 {} 没有上游依赖（无 after / inputs 引用 / iterate.over），将作为 DAG 根节点在 layer 0 并行执行",
                    step.id
                ),
            });
        }
    }

    // ---- 10. DAG 环检测 ----
    if has_dependency_cycle(def, &upstream_deps) {
        report.errors.push(ValidationError {
            code: "cycle_detected".into(),
            message: "步骤依赖存在环（after / inputs 引用 / iterate.over）".into(),
        });
    }

    debug!(
        pipeline = %def.name,
        errors = report.errors.len(),
        warnings = report.warnings.len(),
        "validation complete"
    );
    report
}

// ---------------------------------------------------------------------------
// Helpers — JSON-based ref extraction
// ---------------------------------------------------------------------------

fn is_valid_pipeline_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

fn is_valid_step_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_valid_slot_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn has_dependency_cycle(def: &PipelineDef, upstream_deps: &HashMap<StepId, Vec<StepId>>) -> bool {
    let mut unique_steps: Vec<&StepDef> = Vec::new();
    let mut seen: HashSet<&StepId> = HashSet::new();
    for step in &def.steps {
        if seen.insert(&step.id) {
            unique_steps.push(step);
        }
    }
    let ids: HashSet<&StepId> = unique_steps.iter().map(|s| &s.id).collect();

    let mut in_degree: HashMap<&StepId, usize> =
        unique_steps.iter().map(|s| (&s.id, 0usize)).collect();
    let mut out_edges: HashMap<&StepId, Vec<&StepId>> = HashMap::new();
    for step in &unique_steps {
        let mut deps: HashSet<&StepId> = HashSet::new();
        if let Some(list) = upstream_deps.get(&step.id) {
            for dep in list {
                if dep != &step.id && ids.contains(dep) {
                    deps.insert(dep);
                }
            }
        }
        for dep in deps {
            *in_degree.entry(&step.id).or_default() += 1;
            out_edges.entry(dep).or_default().push(&step.id);
        }
    }

    let mut queue: VecDeque<&StepId> = in_degree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(&id, _)| id)
        .collect();
    let mut visited = 0usize;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        if let Some(children) = out_edges.get(node) {
            for child in children {
                if let Some(d) = in_degree.get_mut(child) {
                    *d -= 1;
                    if *d == 0 {
                        queue.push_back(child);
                    }
                }
            }
        }
    }
    visited != unique_steps.len()
}

fn collect_slots_used(def: &PipelineDef) -> HashSet<String> {
    let mut set = HashSet::new();
    for step in &def.steps {
        let op_val = serde_json::to_value(&step.op).unwrap_or(Value::Null);
        for (prefix, rest) in refs_in_json(&op_val) {
            if prefix == "slots"
                && let Some(name) = rest.split('.').next()
                && !name.is_empty()
            {
                set.insert(name.to_string());
            }
        }
    }
    for (prefix, rest) in output_refs(&def.output) {
        if prefix == "slots"
            && let Some(name) = rest.split('.').next()
            && !name.is_empty()
        {
            set.insert(name.to_string());
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

/// 与 dag.rs 的提取规则一致：仅单键 `{"Ref": ...}` 且值可解析为 VariablePath
/// 才识别为内联 ref 标签；多键对象或无法解析的值按普通对象继续递归。
fn parse_ref_tag(map: &serde_json::Map<String, Value>) -> Option<VariablePath> {
    if map.len() != 1 || !map.contains_key("Ref") {
        return None;
    }
    let path = serde_json::from_value::<VariablePath>(map.get("Ref")?.clone()).ok()?;
    if path.parts.is_empty() {
        return None;
    }
    Some(path)
}

fn refs_in_json(val: &Value) -> Vec<(String, String)> {
    match val {
        Value::Object(map) => {
            if let Some(path) = parse_ref_tag(map) {
                let prefix = path.parts[0].clone();
                let rest = path.parts[1..].join(".");
                return vec![(prefix, rest)];
            }
            let mut all = Vec::new();
            for v in map.values() {
                all.extend(refs_in_json(v));
            }
            all
        }
        Value::Array(arr) => {
            let mut all = Vec::new();
            for v in arr {
                all.extend(refs_in_json(v));
            }
            all
        }
        // 裸字符串不构成引用（与 resolver/dag 一致）
        _ => vec![],
    }
}

fn check_step_ref_prefix(
    prefix: &str,
    all_ids: &HashSet<StepId>,
    step_id: &StepId,
    as_name: Option<&str>,
    report: &mut ValidationReport,
) {
    // iterate 的 as_name 前缀在运行时透传为字面量（当前元素经 "data" 键注入），
    // 不参与步骤引用校验。
    if Some(prefix) == as_name {
        return;
    }
    if prefix == step_id.0 {
        report.errors.push(ValidationError {
            code: "self_reference".into(),
            message: format!("步骤 {} 的 inputs 引用了自身", step_id),
        });
    } else if prefix != "slots" && prefix != "env" && !all_ids.contains(prefix) {
        report.errors.push(ValidationError {
            code: "variable_ref_not_found".into(),
            message: format!("步骤 {} 中引用了不存在的步骤: {}", step_id, prefix),
        });
    }
}

fn check_ref_in_json(
    val: &Value,
    all_ids: &HashSet<StepId>,
    step_id: &StepId,
    as_name: Option<&str>,
    report: &mut ValidationReport,
) {
    match val {
        Value::Object(map) => {
            if let Some(path) = parse_ref_tag(map) {
                check_step_ref_prefix(&path.parts[0], all_ids, step_id, as_name, report);
            } else {
                for v in map.values() {
                    check_ref_in_json(v, all_ids, step_id, as_name, report);
                }
            }
        }
        Value::Array(arr) => {
            for v in arr {
                check_ref_in_json(v, all_ids, step_id, as_name, report);
            }
        }
        // 裸字符串只可能来自 String 类型字段，resolver 从不解析它们 —— 不校验。
        _ => {}
    }
}

fn check_output_ref(output: &RefValue, all_ids: &HashSet<StepId>, report: &mut ValidationReport) {
    match output {
        RefValue::Ref(path) => {
            if let Some(prefix) = path.parts.first()
                && prefix != "slots"
                && prefix != "env"
                && !all_ids.contains(prefix.as_str())
            {
                report.errors.push(ValidationError {
                    code: "output_ref_not_found".into(),
                    message: format!("output 引用了不存在的步骤: {}", prefix),
                });
            }
        }
        RefValue::Literal(lit) => {
            for (prefix, _) in refs_in_json(lit) {
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

fn check_iterate_config(
    cfg: Option<&crate::dsl::IterateConfig>,
    step_id: &StepId,
    all_step_ids: &HashSet<StepId>,
    report: &mut ValidationReport,
) {
    if let Some(cfg) = cfg {
        // as_name 校验：首段等于 as_name 的 ref 在运行时被透传为字面量，
        // 与保留前缀或步骤 id 冲突会静默劫持对应引用，必须拒绝。
        if cfg.as_name.is_empty() {
            report.errors.push(ValidationError {
                code: "invalid_iterate_config".into(),
                message: format!("步骤 {} 的 iterate.as 不能为空", step_id),
            });
        } else {
            if !cfg
                .as_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                report.errors.push(ValidationError {
                    code: "invalid_iterate_config".into(),
                    message: format!(
                        "步骤 {} 的 iterate.as 含非法字符（仅允许 [A-Za-z0-9_]）: {}",
                        step_id, cfg.as_name
                    ),
                });
            }
            if cfg.as_name == "slots" || cfg.as_name == "env" {
                report.errors.push(ValidationError {
                    code: "invalid_iterate_config".into(),
                    message: format!(
                        "步骤 {} 的 iterate.as 不能使用保留前缀: {}",
                        step_id, cfg.as_name
                    ),
                });
            }
            if all_step_ids.contains(cfg.as_name.as_str()) {
                report.errors.push(ValidationError {
                    code: "invalid_iterate_config".into(),
                    message: format!(
                        "步骤 {} 的 iterate.as 与步骤 id 冲突: {}",
                        step_id, cfg.as_name
                    ),
                });
            }
        }
        if cfg.max_workers == Some(0) {
            report.errors.push(ValidationError {
                code: "invalid_iterate_config".into(),
                message: format!(
                    "步骤 {} 的 iterate.max_workers 不能为 0（缺省可移除该字段）",
                    step_id
                ),
            });
        }
        if let Some(w) = cfg.max_workers
            && w > MAX_ITERATE_WORKERS
        {
            report.errors.push(ValidationError {
                code: "invalid_iterate_config".into(),
                message: format!(
                    "步骤 {} 的 iterate.max_workers 超过上限 {}: {}",
                    step_id, MAX_ITERATE_WORKERS, w
                ),
            });
        }
        if let Some(ref batch) = cfg.batch
            && batch.size == 0
        {
            report.errors.push(ValidationError {
                code: "invalid_iterate_config".into(),
                message: format!(
                    "步骤 {} 的 iterate.batch.size 不能为 0（缺省可移除 batch 字段）",
                    step_id
                ),
            });
        }
        if let Some(prefix) = cfg.over.parts.first() {
            if prefix == &step_id.0 {
                report.errors.push(ValidationError {
                    code: "self_reference".into(),
                    message: format!("步骤 {} 的 iterate.over 引用了自身", step_id),
                });
            } else if prefix != "slots"
                && prefix != "env"
                && !all_step_ids.contains(prefix.as_str())
            {
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
}

/// 只拒绝语法错误。eval 会真实执行代码，因此加 64MB 内存上限 + 2 秒
/// 看门狗中断；运行时异常（ReferenceError 等）与中断一律视为语法无误放行。
fn check_js_syntax(id: &StepId, code: &RefValue, report: &mut ValidationReport) {
    let source = match code {
        RefValue::Literal(serde_json::Value::String(s)) => s.as_str(),
        _ => return,
    };
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
    rt.set_memory_limit(64 * 1024 * 1024);
    let interrupted = Arc::new(AtomicBool::new(false));
    {
        let flag = interrupted.clone();
        rt.set_interrupt_handler(Some(Box::new(move || flag.load(Ordering::SeqCst))));
    }
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
    let done = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let interrupted = interrupted.clone();
        let done = done.clone();
        std::thread::spawn(move || {
            for _ in 0..40 {
                if done.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            interrupted.store(true, Ordering::SeqCst);
        })
    };
    let syntax_err: Option<String> = ctx.with(|ctx| match ctx.eval::<rquickjs::Value, _>(source) {
        Ok(_) => None,
        Err(rquickjs::Error::Exception) => {
            let ex = ctx.catch();
            if let Some(obj) = ex.as_object() {
                let name = obj.get::<_, String>("name").unwrap_or_default();
                if name == "SyntaxError" {
                    let message = obj.get::<_, String>("message").unwrap_or_default();
                    return Some(format!("{name}: {message}"));
                }
            }
            None
        }
        Err(_) => None,
    });
    done.store(true, Ordering::SeqCst);
    let _ = watchdog.join();
    if let Some(msg) = syntax_err {
        report.errors.push(ValidationError {
            code: "js_syntax_error".into(),
            message: format!("步骤/规则 {} 的 JS 代码语法错误: {}", id, msg),
        });
    }
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
            timeout_sec: None,
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
            after: if after.is_empty() {
                None
            } else {
                Some(after.into_iter().map(|s| s.into()).collect())
            },
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
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
        }
    }

    #[test]
    fn valid_pipeline_passes() {
        let report = validate(&valid_def());
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn empty_pipeline_name() {
        let mut def = valid_def();
        def.name = "".into();
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "empty_pipeline_name")
        );
    }

    #[test]
    fn no_steps() {
        let mut def = valid_def();
        def.steps.clear();
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "no_steps"));
    }

    #[test]
    fn empty_step_id() {
        let mut def = valid_def();
        def.steps[0].id = "".into();
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "empty_step_id"));
    }

    #[test]
    fn reserved_step_id_slots() {
        let mut def = valid_def();
        def.steps[0].id = "slots".into();
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "reserved_step_id"));
    }

    #[test]
    fn duplicate_step_id() {
        let mut def = valid_def();
        def.steps.push(def.steps[0].clone());
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "duplicate_step_id"));
    }

    #[test]
    fn duplicate_slot_name() {
        let mut def = valid_def();
        def.slots.push(SlotDef {
            name: "url".into(),
            schema: json!({"type": "number"}),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "duplicate_slot_name")
        );
    }

    #[test]
    fn empty_slot_name() {
        let mut def = valid_def();
        def.slots[0].name = "".into();
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "empty_slot_name"));
    }

    #[test]
    fn after_self_ref() {
        let mut def = valid_def();
        def.steps[0].after = Some(vec!["fetch".into()]);
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "after_self_ref"));
    }

    #[test]
    fn after_duplicate_entry() {
        let mut def = valid_def();
        def.steps.push(step_noop("step_b", vec!["fetch", "fetch"]));
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "duplicate_after_entry")
        );
    }

    #[test]
    fn after_ref_not_found() {
        let mut def = valid_def();
        def.steps[0].after = Some(vec!["nonexistent".into()]);
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "after_ref_not_found")
        );
    }

    #[test]
    fn after_ref_found() {
        let mut def = valid_def();
        def.steps.push(step_noop("step_b", vec!["fetch"]));
        let report = validate(&def);
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
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "variable_ref_not_found")
        );
    }

    #[test]
    fn output_ref_not_found() {
        let mut def = valid_def();
        def.output = var_ref("{nonexistent.output}");
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "output_ref_not_found")
        );
    }

    #[test]
    fn slots_env_refs_are_not_checked() {
        let mut def = valid_def();
        def.slots.push(SlotDef {
            name: "source_url".into(),
            schema: json!({"type": "string"}),
        });
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
        let report = validate(&def);
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn empty_slots() {
        let mut def = valid_def();
        def.slots.clear();
        def.steps[0].op = StepOp::Http(HttpInputs {
            url: literal(json!("https://example.com")),
            method: None,
            headers: None,
            body: None,
        });
        let report = validate(&def);
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
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_iterate_config")
        );
    }

    #[test]
    fn iterate_batch_size_zero_rejected() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "item".into(),
            max_workers: None,
            batch: Some(BatchConfig { size: 0 }),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_iterate_config")
        );
    }

    #[test]
    fn iterate_as_name_reserved_prefix_rejected() {
        for reserved in ["slots", "env"] {
            let mut def = valid_def();
            def.steps[0].iterate = Some(IterateConfig {
                over: VariablePath::parse("{slots.url}").unwrap(),
                as_name: reserved.into(),
                max_workers: None,
                batch: None,
            });
            let report = validate(&def);
            assert!(
                report
                    .errors
                    .iter()
                    .any(|e| e.code == "invalid_iterate_config"),
                "as_name '{reserved}' must be rejected"
            );
        }
    }

    #[test]
    fn iterate_as_name_step_id_conflict_rejected() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "fetch".into(),
            max_workers: None,
            batch: None,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_iterate_config")
        );
    }

    #[test]
    fn iterate_as_name_ref_passthrough_allowed() {
        // {item.xxx}（item 为本步骤 iterate.as）在运行时透传为字面量，
        // validator 不得误报 variable_ref_not_found。
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "item".into(),
            max_workers: None,
            batch: None,
        });
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(var_ref("{item.name}")),
        });
        let report = validate(&def);
        assert!(
            !report
                .errors
                .iter()
                .any(|e| e.code == "variable_ref_not_found"),
            "as_name ref must not be flagged: {:?}",
            report.errors
        );
    }

    #[test]
    fn retry_bounds_enforced() {
        let mut def = valid_def();
        def.steps[0].retry = Some(RetryDef {
            max_attempts: 101,
            backoff: BackoffStrategy::default(),
            delay_ms: 1000,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_retry_config")
        );

        let mut def = valid_def();
        def.steps[0].retry = Some(RetryDef {
            max_attempts: 3,
            backoff: BackoffStrategy::default(),
            delay_ms: u64::MAX,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_retry_config")
        );
    }

    #[test]
    fn base64_mode_whitelist() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Base64(Base64Inputs {
            data: Some(literal(json!("x"))),
            mode: Some("rot13".into()),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_operator_config")
        );

        let mut def = valid_def();
        def.steps[0].op = StepOp::Base64(Base64Inputs {
            data: Some(literal(json!("x"))),
            mode: Some("decode".into()),
        });
        let report = validate(&def);
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn llm_temperature_must_be_finite() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Llm(LlmInputs {
            url: var_ref("{slots.url}"),
            model: "m".into(),
            prompt: literal(json!("hi")),
            system: None,
            images_b64: None,
            image_type: None,
            max_tokens: 100,
            temperature: Some(f64::NAN),
            skip_vision_check: None,
            api_key: None,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_operator_config")
        );
    }

    #[test]
    fn llm_api_key_literal_rejected() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Llm(LlmInputs {
            url: var_ref("{slots.url}"),
            model: "m".into(),
            prompt: literal(json!("hi")),
            system: None,
            images_b64: None,
            image_type: None,
            max_tokens: 100,
            temperature: None,
            skip_vision_check: None,
            api_key: Some(literal(json!("sk-plaintext"))),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "insecure_api_key"),
            "expected insecure_api_key error: {:?}",
            report.errors
        );
    }

    #[test]
    fn llm_api_key_ref_accepted() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Llm(LlmInputs {
            url: var_ref("{slots.url}"),
            model: "m".into(),
            prompt: literal(json!("hi")),
            system: None,
            images_b64: None,
            image_type: None,
            max_tokens: 100,
            temperature: None,
            skip_vision_check: None,
            api_key: Some(var_ref("{env.OPENAI_API_KEY}")),
        });
        let report = validate(&def);
        assert!(
            !report.errors.iter().any(|e| e.code == "insecure_api_key"),
            "env ref api_key must be accepted: {:?}",
            report.errors
        );
    }

    #[test]
    fn iterate_batch_size_nonzero_accepted() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "item".into(),
            max_workers: None,
            batch: Some(BatchConfig { size: 8 }),
        });
        let report = validate(&def);
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
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
        let report = validate(&def);
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn json_schema_valid() {
        let mut def = valid_def();
        def.slots[0].schema = json!({"type": "string", "minLength": 1});
        let report = validate(&def);
        assert!(report.is_ok());
    }

    #[test]
    fn nested_ref_in_object() {
        let mut def = valid_def();
        // Literal 负载中的内联 ref 标签（raw.rs 会把整串 "{...}" 转成该形式）
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(
                json!({ "Authorization": { "Ref": { "parts": ["nonexistent", "output"] } } }),
            )),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "variable_ref_not_found")
        );
    }

    #[test]
    fn multiple_errors_at_once() {
        let mut def = valid_def();
        def.steps.push(def.steps[0].clone());
        def.steps[0].after = Some(vec!["ghost".into()]);
        def.output = var_ref("{ghost.output}");
        let report = validate(&def);
        assert!(report.errors.len() >= 2);
    }

    #[test]
    fn retry_max_attempts_zero() {
        let mut def = valid_def();
        def.steps[0].retry = Some(RetryDef {
            max_attempts: 0,
            backoff: BackoffStrategy::default(),
            delay_ms: 1000,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_retry_config")
        );
    }

    #[test]
    fn retry_max_attempts_one_passes() {
        let mut def = valid_def();
        def.steps[0].retry = Some(RetryDef {
            max_attempts: 1,
            backoff: BackoffStrategy::default(),
            delay_ms: 1000,
        });
        let report = validate(&def);
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn embedded_ref_in_longer_string_not_flagged() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(json!("prefix {nonexistent.output} suffix"))),
        });
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "embedded ref should not be flagged: {:?}",
            report.errors
        );
    }

    #[test]
    fn whole_string_ref_inside_literal_not_flagged() {
        // RefValue::Literal 负载中的整串 "{...}" 在运行时按字面量透传
        // （裸字符串只在 String 字段或 Literal 负载中出现，resolver 均不解析），
        // 因此 validator 也不应报错。真实 YAML 中的整串 "{...}" 会被 raw.rs
        // 转成 RefValue::Ref，走另一条校验路径。
        let mut def = valid_def();
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(json!("{nonexistent.output}"))),
        });
        let report = validate(&def);
        assert!(
            !report
                .errors
                .iter()
                .any(|e| e.code == "variable_ref_not_found"),
            "literal string must not be flagged: {:?}",
            report.errors
        );
    }

    #[test]
    fn embedded_ref_in_output_literal_not_flagged() {
        let mut def = valid_def();
        def.output = literal(json!("prefix {ghost.output} suffix"));
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "embedded ref in output should not be flagged: {:?}",
            report.errors
        );
    }

    fn step_var_ref(id: &str, ref_str: &str) -> StepDef {
        StepDef {
            id: id.into(),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Var(VarInputs {
                value: Some(var_ref(ref_str)),
            }),
        }
    }

    #[test]
    fn cycle_via_after_detected() {
        let mut def = valid_def();
        def.steps = vec![step_noop("a", vec!["b"]), step_noop("b", vec!["a"])];
        def.output = var_ref("{a.output}");
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "cycle_detected"));
    }

    #[test]
    fn cycle_via_input_refs_detected() {
        let mut def = valid_def();
        def.steps = vec![
            step_var_ref("a", "{b.output}"),
            step_var_ref("b", "{a.output}"),
        ];
        def.output = var_ref("{a.output}");
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "cycle_detected"));
    }

    #[test]
    fn acyclic_pipeline_not_flagged() {
        let mut def = valid_def();
        def.steps.push(step_var_ref("b", "{fetch.output}"));
        def.output = var_ref("{b.output}");
        let report = validate(&def);
        assert!(!report.errors.iter().any(|e| e.code == "cycle_detected"));
    }

    #[test]
    fn slot_ref_not_found() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Http(HttpInputs {
            url: var_ref("{slots.urll}"),
            method: None,
            headers: None,
            body: None,
        });
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "slot_not_found"));
    }

    #[test]
    fn iterate_over_undeclared_slot_not_found() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.items}").unwrap(),
            as_name: "item".into(),
            max_workers: None,
            batch: None,
        });
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "slot_not_found"));
    }

    #[test]
    fn self_reference_in_inputs() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Http(HttpInputs {
            url: var_ref("{fetch.output.url}"),
            method: None,
            headers: None,
            body: None,
        });
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "self_reference"));
    }

    #[test]
    fn self_reference_in_iterate_over() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{fetch.output}").unwrap(),
            as_name: "item".into(),
            max_workers: None,
            batch: None,
        });
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "self_reference"));
    }

    #[test]
    fn invalid_pipeline_name_charset() {
        let mut def = valid_def();
        def.name = "bad/name?x".into();
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_name_charset")
        );
    }

    #[test]
    fn pipeline_name_with_dot_dash_underscore_accepted() {
        let mut def = valid_def();
        def.name = "my-pipe_v1.2".into();
        let report = validate(&def);
        assert!(report.is_ok(), "expected no errors: {:?}", report.errors);
    }

    #[test]
    fn invalid_step_id_charset() {
        let mut def = valid_def();
        def.steps[0].id = "bad.id".into();
        def.output = var_ref("{bad.output}");
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_name_charset")
        );
    }

    #[test]
    fn slot_name_invalid_charset() {
        let mut def = valid_def();
        def.slots[0].name = "my.slot".into();
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_name_charset")
        );
    }

    #[test]
    fn pipeline_name_all_dots_rejected() {
        let mut def = valid_def();
        def.name = "..".into();
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_pipeline_name")
        );
    }

    #[test]
    fn iterate_max_workers_over_limit_rejected() {
        let mut def = valid_def();
        def.steps[0].iterate = Some(IterateConfig {
            over: VariablePath::parse("{slots.url}").unwrap(),
            as_name: "item".into(),
            max_workers: Some(2048),
            batch: None,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_iterate_config")
        );
    }

    #[test]
    fn filter_unknown_operator_rejected() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Filter(FilterInputs {
            data: None,
            operator: "like".into(),
            field: None,
            value: None,
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_operator_config")
        );
    }

    #[test]
    fn filter_valid_operator_accepted() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Filter(FilterInputs {
            data: None,
            operator: "contains".into(),
            field: None,
            value: None,
        });
        let report = validate(&def);
        assert!(
            !report
                .errors
                .iter()
                .any(|e| e.code == "invalid_operator_config")
        );
    }

    #[test]
    fn sort_unknown_order_rejected() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Sort(SortInputs {
            data: None,
            field: None,
            order: "ascending".into(),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "invalid_operator_config")
        );
    }

    #[test]
    fn multi_key_object_with_ref_key_not_treated_as_ref() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(json!({
                "Ref": { "parts": ["ghost", "output"] },
                "other": "plain"
            }))),
        });
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "multi-key object containing a Ref key must not be treated as a ref tag: {:?}",
            report.errors
        );
    }

    #[test]
    fn unparsable_ref_value_falls_through_to_sibling_check() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(json!({
                "Ref": 123,
                "sibling": { "Ref": { "parts": ["ghost", "output"] } }
            }))),
        });
        let report = validate(&def);
        assert!(
            report
                .errors
                .iter()
                .any(|e| e.code == "variable_ref_not_found")
        );
    }

    #[test]
    fn embedded_slot_ref_in_command_string_not_flagged() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Command(CommandInputs {
            command: literal(json!(
                "curl -H 'Authorization: Bearer {slots.token}' https://api.example.com"
            )),
            shell: None,
            stdin: None,
        });
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "embedded {{slots.token}} in command string must stay literal: {:?}",
            report.errors
        );
    }

    #[test]
    fn embedded_slot_ref_in_js_code_not_flagged() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Js(JsInputs {
            code: literal(json!(
                "function run(data) { return data + '{slots.token}'; }"
            )),
            data: None,
        });
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "embedded {{slots.token}} in js code must stay literal: {:?}",
            report.errors
        );
    }

    #[test]
    fn multi_ref_whole_string_treated_as_literal() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Var(VarInputs {
            value: Some(literal(json!("{a.output} {b.output}"))),
        });
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "whole string with multiple refs must be literal: {:?}",
            report.errors
        );
    }

    #[test]
    fn js_runtime_error_passes_syntax_check() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Js(JsInputs {
            code: literal(json!("nonexistent_function_call();")),
            data: None,
        });
        let report = validate(&def);
        assert!(
            report.is_ok(),
            "ReferenceError is a runtime error, not a syntax error: {:?}",
            report.errors
        );
    }

    #[test]
    fn js_syntax_error_rejected() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Js(JsInputs {
            code: literal(json!("function run( {")),
            data: None,
        });
        let report = validate(&def);
        assert!(report.errors.iter().any(|e| e.code == "js_syntax_error"));
    }

    #[test]
    fn js_infinite_loop_check_terminates() {
        let mut def = valid_def();
        def.steps[0].op = StepOp::Js(JsInputs {
            code: literal(json!("while(1){}")),
            data: None,
        });
        let start = std::time::Instant::now();
        let _ = validate(&def);
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "syntax check on while(1){{}} must terminate via watchdog interrupt"
        );
    }
}
