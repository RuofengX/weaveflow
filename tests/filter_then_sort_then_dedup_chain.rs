// filter_then_sort_then_dedup_chain

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn filter_then_sort_then_dedup_chain() {
    let yaml = r#"
name: chain
steps:
  - id: src
    type: var
    inputs:
      value:
        items:
          - { x: 3, k: "c" }
          - { x: 1, k: "a" }
          - { x: 2, k: "c" }
          - { x: 5, k: "b" }
  - id: flt
    type: filter
    inputs:
      data: "{src.output.value.items}"
      field: "x"
      operator: "gte"
      value: 2
  - id: srt
    type: sort
    inputs:
      data: "{flt.output}"
      field: "x"
      order: "asc"
  - id: dedup
    type: dedup
    inputs:
      data: "{srt.output}"
      field: "k"
output: "{dedup.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["x"], json!(2));
    assert_eq!(arr[1]["x"], json!(5));
}
