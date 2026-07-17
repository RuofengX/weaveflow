
// array_index_path: {step.output.0.name} 数组下标路径解析；越界返回错误

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn array_index_path_resolves_value() {
    let yaml = r#"
name: array_index_path
steps:
  - id: s
    type: var
    inputs:
      value:
        - name: "a"
        - name: "b"
  - id: t
    type: var
    inputs:
      value: "{s.output.value.1.name}"
output: "{t.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["value"], json!("b"));
}

#[test]
fn array_index_out_of_bounds_fails_run() {
    let yaml = r#"
name: array_index_oob
steps:
  - id: s
    type: var
    inputs:
      value: [1, 2]
  - id: t
    type: var
    inputs:
      value: "{s.output.value.5}"
output: "{t.output}"
"#;
    let err = run_yaml(yaml, HashMap::new()).expect_err("out of bounds must fail");
    assert!(err.to_string().contains("s.output.value.5"), "err: {err}");
}
