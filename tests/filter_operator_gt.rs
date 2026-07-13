
// filter_operator_gt

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn filter_operator_gt() {
    let yaml = r#"
name: filter_gt
steps:
  - id: f1
    type: filter
    inputs:
      data:
        - { price: 10 }
        - { price: 25 }
        - { price: 5 }
      field: "price"
      operator: "gt"
      value: 10
output: "{f1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!([{ "price": 25 }]));
}
