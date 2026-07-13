
// split_operator_chunks

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn split_operator_chunks() {
    let yaml = r#"
name: split_test
steps:
  - id: s1
    type: split
    inputs:
      data: [1, 2, 3, 4, 5]
      size: 2
output: "{s1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0], json!([1, 2]));
    assert_eq!(arr[1], json!([3, 4]));
    assert_eq!(arr[2], json!([5]));
}
