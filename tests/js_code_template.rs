
// js_code_template — 测试 code 字段中使用 { } RefValue 引用上游输出的 JS 代码

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;

#[test]
fn code_literal() {
    let yaml = r#"
name: no_template
steps:
  - id: greet
    type: js
    inputs:
      code: |
        function run(data) { return { msg: "hello" }; }
output: "{greet.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["msg"], json!("hello"));
}

#[test]
fn code_ref_from_file() {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(
        b"function greet(name) { return 'Hi, ' + name; }\n\nfunction run(data) {\n  return { msg: greet('Weave') };\n}",
    )
    .expect("write");
    let path = f.path().to_string_lossy().to_string();

    let yaml = r#"
name: code_ref_from_file
steps:
  - id: load_util
    type: file
    inputs:
      path: "PATH_PLACEHOLDER"
  - id: decode_util
    type: base64
    inputs:
      data: "{load_util.output.content}"
      mode: decode
  - id: use_util
    type: js
    inputs:
      code: "{decode_util.output}"
output: "{use_util.output}"
"#
    .replace("PATH_PLACEHOLDER", &path);
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["msg"], json!("Hi, Weave"));
}

#[test]
fn code_ref_from_var() {
    let yaml = r#"
name: code_ref_from_var
steps:
  - id: util_code
    type: var
    inputs:
      value: |
        function add(a, b) {
          return a + b;
        }

        function run(data) {
          return { sum: add(1, 2) };
        }
  - id: use_util
    type: js
    inputs:
      code: "{util_code.output.value}"
output: "{use_util.output}"
"#;
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["sum"], json!(3));
}

// ── binary data ───────────────────────────────────────────────────

#[test]
fn js_binary_from_file_has_base64() {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(b"\x00\xFF\x42\x00\x7F").expect("write");
    let path = f.path().to_string_lossy().to_string();

    let yaml = r#"
name: file_binary_js
steps:
  - id: read
    type: file
    inputs:
      path: "PATH_PLACEHOLDER"
  - id: check
    type: js
    inputs:
      data: "{read.output}"
      code: |
        function run(data) {
          var decoded = __native__.atob(data.content);
          return {
            has_base64: typeof data.content === "string",
            bytes_len: decoded.length,
            byte0: decoded[0],
            byte1: decoded[1]
          };
        }
output: "{check.output}"
"#
    .replace("PATH_PLACEHOLDER", &path);
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["has_base64"], json!(true));
    assert_eq!(result["bytes_len"], json!(5));
    assert_eq!(result["byte0"], json!(0));
    assert_eq!(result["byte1"], json!(255));
}

// ── __native__ bindings ───────────────────────────────────────────

#[test]
fn js_native_inflate() {
    let raw = b"Hello, weave!";
    let compressed = {
        use std::io::Write;
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(raw).expect("write");
        e.finish().expect("finish")
    };
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&compressed);

    let yaml = r#"
name: js_inflate
steps:
  - id: setup
    type: var
    inputs:
      value:
        b64: "B64_PLACEHOLDER"
  - id: inflate_test
    type: js
    inputs:
      data: "{setup.output}"
      code: |
        function run(data) {
          var raw = __native__.atob(data.value.b64);
          var decompressed = __native__.inflate(raw);
          var s = "";
          for (var i = 0; i < decompressed.length; i++) {
            s += String.fromCharCode(decompressed[i]);
          }
          return { text: s };
        }
output: "{inflate_test.output}"
"#
    .replace("B64_PLACEHOLDER", &b64);
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["text"], json!("Hello, weave!"));
}

#[test]
fn js_native_btoa_atob_roundtrip() {
    let yaml = r#"
name: js_b64_roundtrip
steps:
  - id: r
    type: js
    inputs:
      code: |
        function run() {
          var original = "Hello, QuickJS!";
          var bytes = [];
          for (var i = 0; i < original.length; i++) bytes.push(original.charCodeAt(i));
          var encoded = __native__.btoa(bytes);
          var decoded = __native__.atob(encoded);
          var restored = "";
          for (var i = 0; i < decoded.length; i++) restored += String.fromCharCode(decoded[i]);
          return { encoded: encoded, restored: restored };
        }
output: "{r.output}"
"#;
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert!(result["encoded"].as_str().unwrap().len() > 0);
    assert_eq!(result["restored"], json!("Hello, QuickJS!"));
}
