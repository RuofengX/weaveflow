// unknown_operator_fails_validation

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use std::collections::HashMap;

#[test]
fn unknown_operator_fails_validation() {
    let yaml = r#"
name: bad_op
steps:
  - id: s1
    type: nonexistent_operator
    inputs:
      x: 1
output: "{s1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new());
    assert!(result.is_err());
}
