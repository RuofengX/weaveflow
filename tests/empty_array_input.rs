
// empty_array_input

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn empty_array_input() {
    let yaml = r#"
name: empty_test
steps:
  - id: f1
    type: filter
    inputs:
      data: []
      field: "x"
      operator: "gt"
      value: 0
output: "{f1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!([]));
}
