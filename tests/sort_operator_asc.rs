// sort_operator_asc

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn sort_operator_asc() {
    let yaml = r#"
name: sort_asc
steps:
  - id: s1
    type: sort
    inputs:
      data:
        - { name: "c" }
        - { name: "a" }
        - { name: "b" }
      field: "name"
      order: "asc"
output: "{s1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(
        result,
        json!([{ "name": "a" }, { "name": "b" }, { "name": "c" }])
    );
}
