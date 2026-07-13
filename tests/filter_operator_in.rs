
// filter_operator_in

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn filter_operator_in() {
    let yaml = r#"
name: filter_in
steps:
  - id: f1
    type: filter
    inputs:
      data:
        - "alice"
        - "bob"
        - "charlie"
      operator: "in"
      value: ["alice", "charlie"]
output: "{f1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!(["alice", "charlie"]));
}
