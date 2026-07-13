use std::collections::{HashMap, HashSet, VecDeque};

use crate::dsl::schema::{PipelineDef, StepDef};

/// DAG 构建或执行中的错误。
#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected, remaining nodes: {0:?}")]
    CycleFound(Vec<String>),
    #[error("after reference not found: {0}")]
    RefNotFound(String),
    #[error("DAG has no steps")]
    EmptyGraph,
}

/// 拓扑排序后的一层，可并行执行的步骤 ID 集合。
pub type DagLayer = Vec<String>;

/// DAG 拓扑结构。
#[derive(Debug, Clone)]
pub struct Dag {
    /// 步骤定义，以 step_id 索引。
    steps: HashMap<String, StepDef>,
    /// 每个步的入边（依赖它的前驱）。
    in_edges: HashMap<String, Vec<String>>,
    /// 每个步的出边（它依赖的后继）。
    out_edges: HashMap<String, Vec<String>>,
}

impl Dag {
    /// 从 PipelineDef 构建 DAG。
    ///
    /// 隐式依赖（变量引用 `{step_id.output.field}`）和显式依赖（`after`）均被解析为边。
    /// 目前以 `after` 为主要依赖来源。
    pub fn from_pipeline(def: &PipelineDef) -> Result<Self, DagError> {
        if def.steps.is_empty() {
            return Err(DagError::EmptyGraph);
        }

        let mut steps = HashMap::new();
        let mut step_ids = HashSet::new();
        let mut in_edges: HashMap<String, Vec<String>> = HashMap::new();
        let mut out_edges: HashMap<String, Vec<String>> = HashMap::new();

        // 收集所有 step id
        for step in &def.steps {
            if steps.contains_key(&step.id) {
                continue;
            }
            steps.insert(step.id.clone(), step.clone());
            step_ids.insert(step.id.clone());
            in_edges.entry(step.id.clone()).or_default();
            out_edges.entry(step.id.clone()).or_default();
        }

        // 解析 after 依赖
        for step in &def.steps {
            if let Some(ref after_list) = step.after {
                for after_id in after_list {
                    if !steps.contains_key(after_id) {
                        return Err(DagError::RefNotFound(after_id.clone()));
                    }
                    // after_id → step.id
                    out_edges.entry(after_id.clone()).or_default().push(step.id.clone());
                    in_edges.entry(step.id.clone()).or_default().push(after_id.clone());
                }
            }
        }

        // 解析隐式依赖：变量引用 `{step_id.output.field}` + iterate.over + code 中的 {{}}
        for step in &def.steps {
            let deps = extract_output_refs(&step.inputs, &step_ids);
            // Also check iterate.over for dependencies
            let iterate_deps = step.iterate.as_ref()
                .map(|cfg| extract_output_refs(&Some(serde_json::json!({"over": cfg.over})), &step_ids))
                .unwrap_or_default();
            // Also check code for {{step_id.output}} template refs
            let code_deps = step.code.as_deref()
                .map(|c| extract_code_template_deps(c, &step_ids))
                .unwrap_or_default();
            let all_deps: Vec<String> = deps.into_iter()
                .chain(iterate_deps)
                .chain(code_deps)
                .collect();
            for dep_id in all_deps {
                if !steps.contains_key(&dep_id) {
                    return Err(DagError::RefNotFound(dep_id));
                }
                out_edges.entry(dep_id.clone()).or_default().push(step.id.clone());
                in_edges.entry(step.id.clone()).or_default().push(dep_id);
            }
        }

        Ok(Dag { steps, in_edges, out_edges })
    }

    /// Kahn 拓扑排序，返回分层结果。
    ///
    /// 每一层中的步骤没有相互依赖，可以并行执行。
    /// 返回 `Err` 表示存在环，无法完成拓扑排序。
    pub fn topological_sort(&self) -> Result<Vec<DagLayer>, DagError> {
        let mut in_degree: HashMap<&str, usize> = self.steps.keys()
            .map(|id| (id.as_str(), self.in_edges.get(id).map(|e| e.len()).unwrap_or(0)))
            .collect();

        let mut queue: VecDeque<&str> = in_degree.iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut layers: Vec<DagLayer> = Vec::new();
        let mut visited = 0usize;

        while !queue.is_empty() {
            let mut layer = Vec::new();
            for _ in 0..queue.len() {
                if let Some(node) = queue.pop_front() {
                    layer.push(node.to_string());
                    visited += 1;
                    if let Some(children) = self.out_edges.get(node) {
                        for child in children {
                            if let Some(deg) = in_degree.get_mut(child.as_str()) {
                                *deg -= 1;
                                if *deg == 0 {
                                    queue.push_back(child);
                                }
                            }
                        }
                    }
                }
            }
            layers.push(layer);
        }

        if visited != self.steps.len() {
            let remaining: Vec<String> = self.steps.keys()
                .filter(|id| in_degree.get(id.as_str()).is_none_or(|&d| d != 0))
                .map(|id| format!("{} ({})", id, in_degree[id.as_str()]))
                .collect();
            return Err(DagError::CycleFound(remaining));
        }

        Ok(layers)
    }

    /// 检查是否存在环。
    pub fn has_cycle(&self) -> bool {
        self.topological_sort().is_err()
    }

    /// 返回所有 step id 列表。
    pub fn step_ids(&self) -> Vec<String> {
        self.steps.keys().cloned().collect()
    }

    /// 返回某个 step 的定义。
    pub fn step(&self, id: &str) -> Option<&StepDef> {
        self.steps.get(id)
    }

    /// 返回某个 step 的前驱（它依赖的步）。
    pub fn predecessors(&self, id: &str) -> Option<&[String]> {
        self.in_edges.get(id).map(|v| v.as_slice())
    }

    /// 返回某个 step 的后继（依赖它的步）。
    pub fn successors(&self, id: &str) -> Option<&[String]> {
        self.out_edges.get(id).map(|v| v.as_slice())
    }
}

/// 从步骤的 inputs 中提取 `{step_id.output...}` 隐式依赖。
fn extract_output_refs(
    inputs: &Option<serde_json::Value>,
    known_steps: &HashSet<String>,
) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(inputs_val) = inputs {
        collect_refs(inputs_val, &mut refs, known_steps);
    }
    refs.sort();
    refs.dedup();
    refs
}

/// 从 code 中提取 `{{step_id.output}}` 双花括号模板引用中的 step_id。
fn extract_code_template_deps(code: &str, known_steps: &HashSet<String>) -> Vec<String> {
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

/// 递归遍历 JSON Value，提取变量引用中指向已知 step 的依赖。
fn collect_refs(
    val: &serde_json::Value,
    results: &mut Vec<String>,
    known_steps: &HashSet<String>,
) {
    match val {
        serde_json::Value::String(s) => {
            let mut start = 0;
            while let Some(brace) = s[start..].find('{') {
                let brace_abs = start + brace;
                if let Some(end) = s[brace_abs..].find('}') {
                    let inner = &s[brace_abs..brace_abs + end + 1];
                    if let Some(var_ref) = crate::dsl::schema::parse_variable_ref(inner) {
                        // 如果第一个路径段是已知 step_id → 隐式依赖
                        if let Some(first) = var_ref.parts.first()
                            && known_steps.contains(first.as_str()) {
                                results.push(first.clone());
                            }
                    }
                    start = brace_abs + end + 1;
                } else {
                    break;
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr { collect_refs(v, results, known_steps); }
        }
        serde_json::Value::Object(map) => {
            for v in map.values() { collect_refs(v, results, known_steps); }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::schema::*;

    fn make_pipeline(steps: Vec<StepDef>) -> PipelineDef {
        PipelineDef {
            name: "test".to_string(),
            description: None,
            storage: None,
            slots: vec![],
            steps,
            output: "{}".into(),
        }
    }

    fn step(id: &str, after: Vec<&str>) -> StepDef {
        StepDef {
            id: id.into(),
            r#type: "noop".into(),
            after: if after.is_empty() { None } else { Some(after.into_iter().map(|s| s.into()).collect()) },
            iterate: None,
            inputs: None,
            cache: None,
            retry: None,
            timeout: None,
            code: None,
        }
    }

    #[test]
    fn empty_graph_errors() {
        let p = make_pipeline(vec![]);
        let result = Dag::from_pipeline(&p);
        assert!(result.is_err());
    }

    #[test]
    fn single_step() {
        let steps = vec![step("a", vec![])];
        let p = make_pipeline(steps);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0], vec!["a"]);
    }

    #[test]
    fn linear_dag() {
        let steps = vec![
            step("a", vec![]),
            step("b", vec!["a"]),
            step("c", vec!["b"]),
        ];
        let p = make_pipeline(steps);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers.len(), 3);
        assert_eq!(layers[0], vec!["a"]);
        assert_eq!(layers[1], vec!["b"]);
        assert_eq!(layers[2], vec!["c"]);
    }

    #[test]
    fn parallel_layers() {
        let steps = vec![
            step("a", vec![]),
            step("b", vec![]),
            step("c", vec!["a", "b"]),
        ];
        let p = make_pipeline(steps);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers.len(), 2);
        // Layer 0: a, b (order unspecified)
        assert_eq!(layers[0].len(), 2);
        assert!(layers[0].contains(&"a".to_string()));
        assert!(layers[0].contains(&"b".to_string()));
        assert_eq!(layers[1], vec!["c"]);
    }

    #[test]
    fn cycle_detected() {
        let steps = vec![
            step("a", vec!["c"]),
            step("b", vec!["a"]),
            step("c", vec!["b"]),
        ];
        let p = make_pipeline(steps);
        let dag = Dag::from_pipeline(&p).unwrap();
        assert!(dag.has_cycle());
        assert!(dag.topological_sort().is_err());
    }

    #[test]
    fn ref_not_found() {
        let steps = vec![
            step("a", vec!["nonexistent"]),
        ];
        let p = make_pipeline(steps);
        let result = Dag::from_pipeline(&p);
        assert!(result.is_err());
    }

    #[test]
    fn no_deps_parallel() {
        let steps = vec![
            step("a", vec![]),
            step("b", vec![]),
            step("c", vec![]),
            step("d", vec![]),
        ];
        let p = make_pipeline(steps);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].len(), 4);
    }

    #[test]
    fn implicit_dep_via_input_ref() {
        // step "e" has inputs referencing "d.output"
        let s_e = StepDef {
            id: "e".into(),
            r#type: "noop".into(),
            after: None,
            iterate: None,
            inputs: Some(serde_json::json!({"data": "{d.output.items}"})),
            cache: None,
            retry: None,
            timeout: None,
            code: None,
        };
        let p = make_pipeline(vec![step("d", vec![]), s_e]);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0], vec!["d"]);
        assert_eq!(layers[1], vec!["e"]);
    }

    #[test]
    fn implicit_dep_via_input_ref_nested() {
        let s_b = StepDef {
            id: "b".into(),
            r#type: "noop".into(),
            after: None,
            iterate: None,
            inputs: Some(serde_json::json!({"url": "{a.output.url}"})),
            cache: None,
            retry: None,
            timeout: None,
            code: None,
        };
        let p = make_pipeline(vec![step("a", vec![]), s_b]);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers[0], vec!["a"]);
        assert_eq!(layers[1], vec!["b"]);
    }
}
