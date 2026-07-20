// merge_literal_inputs_key: 字面对象中的 inputs 键不应坍缩，兄弟键必须保留

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn merge_preserves_inputs_key_inside_literal() {
    let yaml = r#"
name: merge_inputs_key
steps:
  - id: m1
    type: merge
    inputs:
      a: { x: 1 }
      b:
        inputs: { y: 2 }
        other: 1
output: "{m1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["x"], json!(1));
    assert_eq!(result["inputs"], json!({ "y": 2 }));
    assert_eq!(result["other"], json!(1));
}
