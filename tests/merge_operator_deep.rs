
// merge_operator_deep

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn merge_operator_deep() {
    let yaml = r#"
name: merge_deep_test
steps:
  - id: m1
    type: merge
    inputs:
      a: { cfg: { x: 1, y: 2 }, name: "weaveflow" }
      b: { cfg: { y: 3, z: 4 } }
      deep: true
output: "{m1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["cfg"], json!({ "x": 1, "y": 3, "z": 4 }));
    assert_eq!(result["name"], json!("weaveflow"));
}

#[test]
fn merge_operator_default_stays_shallow() {
    let yaml = r#"
name: merge_shallow_test
steps:
  - id: m1
    type: merge
    inputs:
      a: { cfg: { x: 1, y: 2 } }
      b: { cfg: { y: 3 } }
output: "{m1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["cfg"], json!({ "y": 3 }));
}
