
// hex_digest_field_access

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn hex_digest_field_access() {
    // {s1.output.msg} should resolve through hex digest to nested field
    let yaml = r#"
name: hex_digest_test
steps:
  - id: s1
    type: var
    inputs:
      msg: "hello"
      code: 200
output: "{s1.output.msg}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!("hello"));
}
