
// output_step_field_ref: pipeline output 为 {step.field} 时应返回字段值而非整个 step 输出

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn output_step_field_ref_returns_field_value() {
    let yaml = r#"
name: output_step_field
steps:
  - id: s
    type: var
    inputs:
      value:
        name: "weaveflow"
        version: "0.1"
output: "{s.value}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!({ "name": "weaveflow", "version": "0.1" }));
}
