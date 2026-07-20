// null_slot_value

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn null_slot_value() {
    let yaml = r#"
name: null_slot
slots:
  - name: maybe
    schema: { type: ["string", "null"] }
steps:
  - id: s1
    type: var
    inputs:
      value: "{slots.maybe}"
output: "{s1.output.value}"
"#;
    let mut slots = HashMap::new();
    slots.insert("maybe".into(), json!(null));
    let result = run_yaml(yaml, slots).expect("run");
    assert_eq!(result, json!(null));
}
