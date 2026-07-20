// merge_operator_objects

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn merge_operator_objects() {
    let yaml = r#"
name: merge_test
steps:
  - id: m1
    type: merge
    inputs:
      a: { name: "weaveflow", version: "0.1" }
      b: { version: "0.2", author: "dev" }
output: "{m1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["name"], json!("weaveflow"));
    assert_eq!(result["version"], json!("0.2"));
    assert_eq!(result["author"], json!("dev"));
}
