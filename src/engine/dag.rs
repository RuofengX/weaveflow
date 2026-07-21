use std::collections::{HashMap, HashSet, VecDeque};
use tracing::debug;

use crate::dsl::{PipelineDef, StepDef, StepId, VariablePath};

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected, remaining nodes: {0:?}")]
    CycleFound(Vec<StepId>),
    #[error("after reference not found: {0}")]
    RefNotFound(StepId),
    #[error("duplicate step id: {0}")]
    DuplicateStepId(StepId),
    #[error("DAG has no steps")]
    EmptyGraph,
}

pub type DagLayer = Vec<StepId>;

#[derive(Debug, Clone)]
pub struct Dag {
    steps: HashMap<StepId, StepDef>,
    in_edges: HashMap<StepId, Vec<StepId>>,
    out_edges: HashMap<StepId, Vec<StepId>>,
}

impl Dag {
    pub fn from_pipeline(def: &PipelineDef) -> Result<Self, DagError> {
        debug!(pipeline = %def.name, steps = def.steps.len(), "building DAG");
        if def.steps.is_empty() {
            return Err(DagError::EmptyGraph);
        }

        let mut steps = HashMap::new();
        let mut step_ids = HashSet::new();
        let mut in_edges: HashMap<StepId, Vec<StepId>> = HashMap::new();
        let mut out_edges: HashMap<StepId, Vec<StepId>> = HashMap::new();

        for step in &def.steps {
            if steps.contains_key(&step.id) {
                return Err(DagError::DuplicateStepId(step.id.clone()));
            }
            steps.insert(step.id.clone(), step.clone());
            step_ids.insert(step.id.clone());
            in_edges.entry(step.id.clone()).or_default();
            out_edges.entry(step.id.clone()).or_default();
        }

        for step in &def.steps {
            if let Some(ref after_list) = step.after {
                for after_id in after_list {
                    if !steps.contains_key(after_id) {
                        return Err(DagError::RefNotFound(after_id.clone()));
                    }
                    out_edges
                        .entry(after_id.clone())
                        .or_default()
                        .push(step.id.clone());
                    in_edges
                        .entry(step.id.clone())
                        .or_default()
                        .push(after_id.clone());
                }
            }
        }

        for step in &def.steps {
            let deps = extract_output_refs_from_step(step, &step_ids);
            let iterate_deps = step
                .iterate
                .as_ref()
                .map(|cfg| extract_refs_from_path(&cfg.over, &step_ids))
                .unwrap_or_default();
            let all_deps: Vec<StepId> = deps.into_iter().chain(iterate_deps).collect();
            for dep_id in all_deps {
                if !steps.contains_key(&dep_id) {
                    return Err(DagError::RefNotFound(dep_id));
                }
                out_edges
                    .entry(dep_id.clone())
                    .or_default()
                    .push(step.id.clone());
                in_edges.entry(step.id.clone()).or_default().push(dep_id);
            }
        }

        Ok(Dag {
            steps,
            in_edges,
            out_edges,
        })
    }

    pub fn topological_sort(&self) -> Result<Vec<DagLayer>, DagError> {
        debug!(steps = self.steps.len(), "topological sort");
        let mut in_degree: HashMap<&StepId, usize> = self
            .steps
            .keys()
            .map(|id| (id, self.in_edges.get(id).map(|e| e.len()).unwrap_or(0)))
            .collect();

        let mut queue: VecDeque<&StepId> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut layers: Vec<DagLayer> = Vec::new();
        let mut visited = 0usize;

        while !queue.is_empty() {
            let mut layer = Vec::new();
            for _ in 0..queue.len() {
                if let Some(node) = queue.pop_front() {
                    layer.push(node.clone());
                    visited += 1;
                    if let Some(children) = self.out_edges.get(node) {
                        for child in children {
                            if let Some(deg) = in_degree.get_mut(child) {
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
            let remaining: Vec<StepId> = self
                .steps
                .keys()
                .filter(|id| in_degree.get(id).is_none_or(|&d| d != 0))
                .cloned()
                .collect();
            return Err(DagError::CycleFound(remaining));
        }

        Ok(layers)
    }

    pub fn has_cycle(&self) -> bool {
        self.topological_sort().is_err()
    }

    pub fn step_ids(&self) -> Vec<StepId> {
        self.steps.keys().cloned().collect()
    }

    pub fn step(&self, id: &StepId) -> Option<&StepDef> {
        self.steps.get(id)
    }

    pub fn predecessors(&self, id: &StepId) -> Option<&[StepId]> {
        self.in_edges.get(id).map(|v| v.as_slice())
    }

    pub fn successors(&self, id: &StepId) -> Option<&[StepId]> {
        self.out_edges.get(id).map(|v| v.as_slice())
    }
}

fn extract_output_refs_from_step(step: &StepDef, known_steps: &HashSet<StepId>) -> Vec<StepId> {
    let Ok(op_value) = serde_json::to_value(&step.op) else {
        return vec![];
    };
    let mut refs = Vec::new();
    collect_refs(&op_value, &mut refs, known_steps);
    refs.sort();
    refs.dedup();
    refs
}

fn extract_refs_from_path(path: &VariablePath, known_steps: &HashSet<StepId>) -> Vec<StepId> {
    if let Some(first) = path.parts.first()
        && known_steps.contains(first.as_str())
    {
        return vec![StepId::from(first.clone())];
    }
    vec![]
}

fn collect_refs(val: &serde_json::Value, results: &mut Vec<StepId>, known_steps: &HashSet<StepId>) {
    match val {
        serde_json::Value::Object(map) => {
            if map.len() == 1 && map.contains_key("Ref") {
                let path_val = &map["Ref"];
                match serde_json::from_value::<VariablePath>(path_val.clone()) {
                    Ok(path) => {
                        if let Some(first) = path.parts.first()
                            && known_steps.contains(first.as_str())
                        {
                            results.push(StepId::from(first.clone()));
                        }
                    }
                    // 用户数据恰好是单键 "Ref" 对象但不是合法引用标签：
                    // 按普通对象继续递归（与 validator/resolver 的回退行为一致）。
                    Err(_) => collect_refs(path_val, results, known_steps),
                }
            } else if map.len() == 1 && map.contains_key("Template") {
                match serde_json::from_value::<Vec<crate::dsl::TemplatePart>>(
                    map["Template"].clone(),
                ) {
                    Ok(parts) => {
                        for part in parts {
                            if let crate::dsl::TemplatePart::Ref(path) = part {
                                results.extend(extract_refs_from_path(&path, known_steps));
                            }
                        }
                    }
                    // 单键 "Template" 用户数据回退：与 "Ref" 标签同一规则。
                    Err(_) => collect_refs(&map["Template"], results, known_steps),
                }
            } else if map.len() == 1 && map.contains_key("Literal") {
                if let Some(lit) = map.get("Literal") {
                    collect_refs(lit, results, known_steps);
                }
            } else {
                for v in map.values() {
                    collect_refs(v, results, known_steps);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_refs(v, results, known_steps);
            }
        }
        // 裸字符串只可能来自 String 类型字段（method/field/mode 等），
        // resolver 从不解析它们 —— 不构成依赖，与 resolver 保持一致。
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::step_op::{FilterInputs, VarInputs};
    use crate::dsl::*;

    fn make_pipeline(steps: Vec<StepDef>) -> PipelineDef {
        PipelineDef {
            name: "test".to_string(),
            description: None,
            storage: None,
            slots: vec![],
            steps,
            output: serde_json::json!("ok"),
        }
    }

    fn step(id: &str, after: Vec<&str>) -> StepDef {
        StepDef {
            id: StepId::from(id),
            after: if after.is_empty() {
                None
            } else {
                Some(after.into_iter().map(StepId::from).collect())
            },
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Noop,
        }
    }

    fn sid(s: &str) -> StepId {
        StepId::from(s)
    }

    #[test]
    fn string_field_with_brace_pattern_is_not_a_dep() {
        // String 类型字段（如 filter.field）中的整串 "{...}" 是字面量：
        // resolver 不解析，DAG 也不得据此建边（否则两个互相 "{x.output}"
        // 的 String 字段会造成 cycle 假阳性）。
        let mut s1 = step("a", vec![]);
        s1.op = StepOp::Filter(FilterInputs {
            data: None,
            operator: "eq".into(),
            field: Some("{b.output}".into()),
            value: None,
        });
        let mut s2 = step("b", vec![]);
        s2.op = StepOp::Filter(FilterInputs {
            data: None,
            operator: "eq".into(),
            field: Some("{a.output}".into()),
            value: None,
        });
        let p = make_pipeline(vec![s1, s2]);
        let dag = Dag::from_pipeline(&p).expect("no cycle from literal String fields");
        assert!(dag.predecessors(&sid("a")).unwrap().is_empty());
        assert!(dag.predecessors(&sid("b")).unwrap().is_empty());
    }

    #[test]
    fn unparsable_ref_tag_falls_back_to_recursion() {
        // 单键 "Ref" 用户数据（值不可解析为 VariablePath）按普通对象递归，
        // 其中嵌套的合法 ref 标签仍应被发现。
        let mut s1 = step("a", vec![]);
        s1.op = StepOp::Var(VarInputs {
            value: Some(RefValue::Literal(serde_json::json!({
                "Ref": { "nested": { "Ref": { "parts": ["b", "output"] } } }
            }))),
        });
        let s2 = step("b", vec![]);
        let p = make_pipeline(vec![s1, s2]);
        let dag = Dag::from_pipeline(&p).unwrap();
        assert_eq!(dag.predecessors(&sid("a")).unwrap(), &[sid("b")]);
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
        assert_eq!(layers[0], vec![sid("a")]);
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
        assert_eq!(layers[0], vec![sid("a")]);
        assert_eq!(layers[1], vec![sid("b")]);
        assert_eq!(layers[2], vec![sid("c")]);
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
        assert_eq!(layers[0].len(), 2);
        assert!(layers[0].contains(&sid("a")));
        assert!(layers[0].contains(&sid("b")));
        assert_eq!(layers[1], vec![sid("c")]);
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
        let steps = vec![step("a", vec!["nonexistent"])];
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
        let s_e = StepDef {
            id: StepId::from("e"),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Var(step_op::VarInputs {
                value: Some(RefValue::Ref(
                    VariablePath::parse("{d.output.items}").unwrap(),
                )),
            }),
        };
        let p = make_pipeline(vec![step("d", vec![]), s_e]);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0], vec![sid("d")]);
        assert_eq!(layers[1], vec![sid("e")]);
    }

    #[test]
    fn implicit_dep_via_input_ref_nested() {
        let s_b = StepDef {
            id: StepId::from("b"),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Http(step_op::HttpInputs {
                url: RefValue::Ref(VariablePath::parse("{a.output.url}").unwrap()),
                method: None,
                headers: None,
                body: None,
            }),
        };
        let p = make_pipeline(vec![step("a", vec![]), s_b]);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers[0], vec![sid("a")]);
        assert_eq!(layers[1], vec![sid("b")]);
    }

    #[test]
    fn duplicate_step_id_errors() {
        let steps = vec![step("a", vec![]), step("a", vec![])];
        let p = make_pipeline(steps);
        let result = Dag::from_pipeline(&p);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DagError::DuplicateStepId(_)));
    }

    #[test]
    fn implicit_dep_via_ref_tag_nested_in_literal_object() {
        let s_b = StepDef {
            id: StepId::from("b"),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Merge(step_op::MergeInputs {
                b: RefValue::Literal(serde_json::json!({
                    "nested": {
                        "deep": [
                            { "Ref": { "parts": ["a", "output", "val"] } }
                        ]
                    }
                })),
                a: None,
                deep: None,
            }),
        };
        let p = make_pipeline(vec![step("a", vec![]), s_b]);
        let dag = Dag::from_pipeline(&p).unwrap();
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers[0], vec![sid("a")]);
        assert_eq!(layers[1], vec![sid("b")]);
    }

    #[test]
    fn embedded_ref_in_string_literal_creates_no_ghost_edge() {
        // a 的 inputs 含内嵌 "{b.output}" 的字面量字符串（不插值、非依赖），
        // b 真实引用 a —— 不应构成环，a 也不应有幽灵入边。
        let s_a = StepDef {
            id: StepId::from("a"),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Var(step_op::VarInputs {
                value: Some(RefValue::Literal(serde_json::json!(
                    "prefix {b.output} suffix"
                ))),
            }),
        };
        let s_b = StepDef {
            id: StepId::from("b"),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout_sec: None,
            op: StepOp::Var(step_op::VarInputs {
                value: Some(RefValue::Ref(VariablePath::parse("{a.output}").unwrap())),
            }),
        };
        let p = make_pipeline(vec![s_a, s_b]);
        let dag = Dag::from_pipeline(&p).expect("no cycle from embedded literal");
        assert!(dag.predecessors(&sid("a")).unwrap().is_empty());
        let layers = dag.topological_sort().unwrap();
        assert_eq!(layers[0], vec![sid("a")]);
        assert_eq!(layers[1], vec![sid("b")]);
    }
}
