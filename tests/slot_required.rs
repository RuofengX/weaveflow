// 必填 slot（schema 无 default）缺失时必须报错，不能静默按 null 继续

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use std::collections::HashMap;

#[test]
fn missing_required_slot_is_error() {
    let yaml = r#"
name: slot_required
slots:
  - name: items
    schema:
      type: array
steps:
  - id: s
    type: js
    inputs:
      code: "function run(d){ return d; }"
      data: "{slots.items}"
output: "{s.output}"
"#;
    let err = run_yaml(yaml, HashMap::new()).expect_err("missing slot must fail");
    assert!(err.to_string().contains("缺少必填 slot"), "err: {err}");
}

#[test]
fn slot_with_default_still_optional() {
    let yaml = r#"
name: slot_default
slots:
  - name: n
    schema:
      type: number
      default: 42
steps:
  - id: s
    type: js
    inputs:
      code: "function run(d){ return d; }"
      data: "{slots.n}"
output: "{s.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, serde_json::json!(42));
}
