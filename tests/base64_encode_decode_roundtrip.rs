
// base64_encode_decode_roundtrip

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn base64_encode_decode_roundtrip() {
    let yaml = r#"
name: base64_test
steps:
  - id: enc
    type: base64
    inputs:
      data: "hello weave"
  - id: dec
    type: base64
    inputs:
      data: "{enc.output}"
      mode: "decode"
output: "{dec.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, json!("hello weave"));
}
