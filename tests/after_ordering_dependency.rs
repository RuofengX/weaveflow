
// after_ordering_dependency

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn after_ordering_dependency() {
    let yaml = r#"
name: after_test
steps:
  - id: step_a
    type: var
    inputs:
      value: "first"
  - id: step_b
    type: var
    after: [step_a]
    inputs:
      value: "second"
output: "{step_b.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["value"], json!("second"));
}
