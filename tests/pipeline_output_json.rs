// pipeline_output_json: output 支持任意 JSON（对象/数组字面量 + 内联 ref 深解析）

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn output_object_with_inline_refs() {
    let yaml = r#"
name: output_object
steps:
  - id: a
    type: var
    inputs:
      value: {"n": 1}
  - id: b
    type: var
    inputs:
      value: {"m": 2}
output:
  first: "{a.output.value}"
  second_n: "{b.output.value.m}"
  static: "hello"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(
        result,
        json!({"first": {"n": 1}, "second_n": 2, "static": "hello"})
    );
}

#[test]
fn output_array_and_scalar_literals() {
    let yaml = r#"
name: output_array
steps:
  - id: a
    type: var
    inputs:
      value: [1, 2, 3]
output: ["{a.output.value.0}", "{a.output.value.2}", 42]
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!([1, 3, 42]));
}

#[test]
fn output_embedded_ref_string_stays_literal() {
    // 非整串 "{...}" 一律字面量，与 inputs 行为一致
    let yaml = r#"
name: output_embedded
steps:
  - id: a
    type: var
    inputs:
      value: "x"
output: "prefix {a.output} suffix"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!("prefix {a.output} suffix"));
}

#[test]
fn output_whole_string_ref_still_works() {
    let yaml = r#"
name: output_whole_ref
steps:
  - id: a
    type: var
    inputs:
      value: {"k": "v"}
output: "{a.output.value}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!({"k": "v"}));
}
