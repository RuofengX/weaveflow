// noop_output: 顶层 op 信封处理 — noop（无 inputs）输出不被 {"type":"noop"} 污染；
// iterate 场景注入的 "data" 必须存活

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn noop_output_is_empty_object() {
    let yaml = r#"
name: noop_plain
steps:
  - id: n
    type: noop
output: "{n.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!({}));
}

#[test]
fn iterate_noop_output_contains_injected_data() {
    let yaml = r#"
name: noop_iterate
slots:
  - name: items
    schema:
      type: array
steps:
  - id: n
    type: noop
    iterate:
      over: "{slots.items}"
      as: "item"
      max_workers: 2
output: "{n.output}"
"#;
    let mut slots = HashMap::new();
    slots.insert("items".into(), json!([1, 2]));
    let result = run_yaml(yaml, slots).expect("run");
    assert_eq!(result, json!([{ "data": 1 }, { "data": 2 }]));
}
