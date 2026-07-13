
// sort_operator_desc

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn sort_operator_desc() {
    let yaml = r#"
name: sort_desc
steps:
  - id: s1
    type: sort
    inputs:
      data:
        - { score: 10 }
        - { score: 30 }
        - { score: 20 }
      field: "score"
      order: "desc"
output: "{s1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!([{ "score": 30 }, { "score": 20 }, { "score": 10 }]));
}
