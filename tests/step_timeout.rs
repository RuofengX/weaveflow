
// step_timeout

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn step_timeout_triggers() {
    let yaml = r#"
name: timeout_test
steps:
  - id: slow
    type: command
    timeout_sec: 0.5
    inputs:
      command: "sleep 5"
output: "{slow.output}"
"#;
    let err = run_yaml(yaml, HashMap::new()).expect_err("step should time out");
    assert!(err.to_string().contains("timeout"), "err: {err}");
}

#[test]
fn iterate_element_timeout_triggers() {
    let yaml = r#"
name: iterate_timeout
slots:
  - name: items
    schema:
      type: array
steps:
  - id: slow
    type: command
    timeout_sec: 1
    iterate:
      over: "{slots.items}"
      as: "item"
      max_workers: 2
    inputs:
      command: "sleep 5"
output: "{slow.output}"
"#;
    let mut slots = HashMap::new();
    slots.insert("items".into(), json!([1, 2]));
    let err = run_yaml(yaml, slots).expect_err("iterate element should time out");
    assert!(err.to_string().contains("timeout"), "err: {err}");
}

#[test]
fn step_within_timeout_succeeds() {
    let yaml = r#"
name: timeout_ok
steps:
  - id: fast
    type: command
    timeout_sec: 30
    inputs:
      command: "echo hi"
output: "{fast.output.stdout}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!("hi\n"));
}
