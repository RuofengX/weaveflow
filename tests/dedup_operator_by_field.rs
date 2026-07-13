
// dedup_operator_by_field

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use std::collections::HashMap;

#[test]
fn dedup_operator_by_field() {
    let yaml = r#"
name: dedup_test
steps:
  - id: d1
    type: dedup
    inputs:
      data:
        - { id: 1, name: "a" }
        - { id: 2, name: "b" }
        - { id: 1, name: "a_dup" }
      field: "id"
output: "{d1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result.as_array().unwrap().len(), 2);
}
