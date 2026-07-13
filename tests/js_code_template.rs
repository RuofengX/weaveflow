
// js_code_template — 测试 code 中的 {{}} 模板引用

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;

#[test]
fn code_template_basic() {
    // 纯 JS（无模板引用），验证正常运行
    let yaml2 = r#"
name: no_template
steps:
  - id: greet
    type: js
    code: |
      function run(input) { return { msg: "hello" }; }
output: "{greet.output}"
"#;
    let result = run_yaml(yaml2, HashMap::new()).expect("run");
    assert_eq!(result["msg"], json!("hello"));
}

#[test]
fn code_template_with_file() {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(b"function greet(name) { return 'Hi, ' + name; }").expect("write");
    let path = f.path().to_string_lossy().to_string();

    let yaml = format!(
        r#"
name: code_template_file
steps:
  - id: load_util
    type: file
    inputs:
      path: "{path}"
  - id: use_util
    type: js
    inputs:
      data: "{{load_util.output}}"
    code: |
      {{{{load_util.output}}}}

      function run(input) {{
        return {{ msg: greet('Weave') }};
      }}
output: "{{use_util.output}}"
"#
    );
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["msg"], json!("Hi, Weave"));
}

#[test]
fn code_template_with_var() {
    let yaml = r#"
name: code_template_var
steps:
  - id: util_code
    type: var
    inputs:
      code: |
        function add(a, b) {
          return a + b;
        }
  - id: use_util
    type: js
    code: |
      {{util_code.output.code}}

      function run(input) {
        return { sum: add(1, 2) };
      }
output: "{use_util.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["sum"], json!(3));
}

// ── binary data ───────────────────────────────────────────────────

#[test]
fn js_binary_from_file_has_base64() {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(b"\x00\xFF\x42\x00\x7F").expect("write");
    let path = f.path().to_string_lossy().to_string();

    let yaml = format!(
        r#"
name: file_binary_js
steps:
  - id: read
    type: file
    inputs:
      path: "{path}"
  - id: check
    type: js
    inputs:
      data: "{{read.output}}"
    code: |
      function run(input) {{
        var decoded = __native__.atob(input.data_base64);
        return {{
          has_base64: typeof input.data_base64 === "string",
          bytes_len: decoded.length,
          byte0: decoded[0],
          byte1: decoded[1]
        }};
      }}
output: "{{check.output}}"
"#
    );
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

    let yaml = format!(
        r#"
name: js_inflate
steps:
  - id: setup
    type: var
    inputs:
      b64: "{b64}"
  - id: inflate_test
    type: js
    inputs:
      data: "{{setup.output}}"
    code: |
      function run(input) {{
        var raw = __native__.atob(input.data.b64);
        var decompressed = __native__.inflate(raw);
        var s = "";
        for (var i = 0; i < decompressed.length; i++) {{
          s += String.fromCharCode(decompressed[i]);
        }}
        return {{ text: s }};
      }}
output: "{{inflate_test.output}}"
"#
    );
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
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert!(result["encoded"].as_str().unwrap().len() > 0);
    assert_eq!(result["restored"], json!("Hello, QuickJS!"));
}
