use std::collections::{HashMap, HashSet, VecDeque};
use tracing::debug;

use crate::dsl::{PipelineDef, StepDef, StepOp, VariablePath};
use crate::vm::resolver::extract_code_template_deps;

#[derive(Debug, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected, remaining nodes: {0:?}")]
    CycleFound(Vec<String>),
    #[error("after reference not found: {0}")]
    RefNotFound(String),
    #[error("DAG has no steps")]
    EmptyGraph,
}

pub type DagLayer = Vec<String>;

#[derive(Debug, Clone)]
pub struct Dag {
    steps: HashMap<String, StepDef>,
    in_edges: HashMap<String, Vec<String>>,
    out_edges: HashMap<String, Vec<String>>,
}

impl Dag {
    pub fn from_pipeline(def: &PipelineDef) -> Result<Self, DagError> {
        debug!(pipeline = %def.name, steps = def.steps.len(), "building DAG");
        if def.steps.is_empty() {
            return Err(DagError::EmptyGraph);
        }

        let mut steps = HashMap::new();
        let mut step_ids = HashSet::new();
        let mut in_edges: HashMap<String, Vec<String>> = HashMap::new();
        let mut out_edges: HashMap<String, Vec<String>> = HashMap::new();

        for step in &def.steps {
            if steps.contains_key(&step.id) {
                continue;
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
                    out_edges.entry(after_id.clone()).or_default().push(step.id.clone());
                    in_edges.entry(step.id.clone()).or_default().push(after_id.clone());
                }
            }
        }

        for step in &def.steps {
            let deps = extract_output_refs_from_step(step, &step_ids);
            let iterate_deps = step.iterate
                .as_ref()
                .map(|cfg| extract_refs_from_path(&cfg.over, &step_ids))
                .unwrap_or_default();
            let code_deps = match &step.op {
                StepOp::Js(inputs) => extract_code_template_deps(&inputs.code, &step_ids),
                _ => vec![],
            };
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

    pub fn topological_sort(&self) -> Result<Vec<DagLayer>, DagError> {
        debug!(steps = self.steps.len(), "topological sort");
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

    pub fn has_cycle(&self) -> bool {
        self.topological_sort().is_err()
    }

    pub fn step_ids(&self) -> Vec<String> {
        self.steps.keys().cloned().collect()
    }

    pub fn step(&self, id: &str) -> Option<&StepDef> {
        self.steps.get(id)
    }

    pub fn predecessors(&self, id: &str) -> Option<&[String]> {
        self.in_edges.get(id).map(|v| v.as_slice())
    }

    pub fn successors(&self, id: &str) -> Option<&[String]> {
        self.out_edges.get(id).map(|v| v.as_slice())
    }
}

fn extract_output_refs_from_step(
    step: &StepDef,
    known_steps: &HashSet<String>,
) -> Vec<String> {
    let Ok(op_value) = serde_json::to_value(&step.op) else { return vec![] };
    let mut refs = Vec::new();
    collect_refs(&op_value, &mut refs, known_steps);
    refs.sort();
    refs.dedup();
    refs
}

fn extract_refs_from_path(
    path: &VariablePath,
    known_steps: &HashSet<String>,
) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(first) = path.parts.first()
        && known_steps.contains(first.as_str()) {
            refs.push(first.clone());
        }
    refs
}

fn collect_refs(
    val: &serde_json::Value,
    results: &mut Vec<String>,
    known_steps: &HashSet<String>,
) {
    match val {
        serde_json::Value::Object(map) => {
            if map.len() == 1 && map.contains_key("Ref") {
                if let Some(path_val) = map.get("Ref")
                    && let Ok(path) = serde_json::from_value::<VariablePath>(path_val.clone())
                    && let Some(first) = path.parts.first()
                        && known_steps.contains(first.as_str()) {
                            results.push(first.clone());
                        }
            } else if map.len() == 1 && map.contains_key("Literal") {
                if let Some(lit) = map.get("Literal") {
                    collect_string_refs(lit, results, known_steps);
                }
            } else {
                for v in map.values() {
                    collect_refs(v, results, known_steps);
                }
            }
        }
        serde_json::Value::String(_s) => {
            collect_string_refs(val, results, known_steps);
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_refs(v, results, known_steps);
            }
        }
        _ => {}
    }
}

fn collect_string_refs(
    val: &serde_json::Value,
    results: &mut Vec<String>,
    known_steps: &HashSet<String>,
) {
    let Some(s) = val.as_str() else { return };
    let mut start = 0;
    while let Some(brace) = s[start..].find('{') {
        let brace_abs = start + brace;
        if let Some(end) = s[brace_abs..].find('}') {
            let inner = &s[brace_abs..brace_abs + end + 1];
            if let Some(var_ref) = VariablePath::parse(inner)
                && let Some(first) = var_ref.parts.first()
                    && known_steps.contains(first.as_str()) {
                        results.push(first.clone());
                    }
            start = brace_abs + end + 1;
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::*;

    fn make_pipeline(steps: Vec<StepDef>) -> PipelineDef {
        PipelineDef {
            name: "test".to_string(),
            description: None,
            storage: None,
            slots: vec![],
            steps,
            output: RefValue::Literal(serde_json::json!("ok")),
            rules: vec![],
        }
    }

    fn step(id: &str, after: Vec<&str>) -> StepDef {
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
        let s_e = StepDef {
            id: "e".into(),
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout: None,
            op: StepOp::Var(step_op::VarInputs {
                value: Some(RefValue::Ref(VariablePath::parse("{d.output.items}").unwrap())),
            }),
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
            after: None,
            iterate: None,
            cache: None,
            retry: None,
            timeout: None,
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
        assert_eq!(layers[0], vec!["a"]);
        assert_eq!(layers[1], vec!["b"]);
    }
}
