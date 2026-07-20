// hex_digest_deep_field_access

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn hex_digest_deep_field_access() {
    let yaml = r#"
name: hex_deep_test
steps:
  - id: s1
    type: var
    inputs:
      value:
        result:
          data:
            count: 42
output: "{s1.output.value.result.data.count}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!(42));
}
