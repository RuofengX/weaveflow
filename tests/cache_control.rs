
// cache_control

#[path = "common/mod.rs"]
mod common;
use common::{run_yaml_with_db, temp_db};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use weaveflow::engine::cache::compute_cache_key;
use weaveflow::store::Database;

fn cache_lookup(db: &Arc<Database>, key: &[u8]) -> Option<Value> {
    db.check_cache_bytes(key).expect("check cache")
}

fn cache_seed(db: &Arc<Database>, key: &[u8], value: &Value) {
    db.set_cache_bytes(key, value).expect("seed cache");
}

const VAR_YAML_CACHED: &str = r#"
name: cache_on
steps:
  - id: s
    type: var
    inputs:
      value: 42
output: "{s.output}"
"#;

const VAR_YAML_NO_CACHE: &str = r#"
name: cache_off
steps:
  - id: s
    type: var
    cache: false
    inputs:
      value: 42
output: "{s.output}"
"#;

#[test]
fn cacheable_step_writes_cache() {
    let (db, _dir) = temp_db();
    let db = Arc::new(db);
    let key = compute_cache_key("var", &json!({"value": 42}));

    let result = run_yaml_with_db(VAR_YAML_CACHED, HashMap::new(), db.clone()).expect("run");
    assert_eq!(result, json!({"value": 42}));
    assert_eq!(cache_lookup(&db, &key), Some(json!({"value": 42})));
}

#[test]
fn cacheable_step_reads_cache() {
    let (db, _dir) = temp_db();
    let db = Arc::new(db);
    let key = compute_cache_key("var", &json!({"value": 42}));
    cache_seed(&db, &key, &json!("poisoned"));

    let result = run_yaml_with_db(VAR_YAML_CACHED, HashMap::new(), db.clone()).expect("run");
    assert_eq!(result, json!("poisoned"), "cacheable step should hit seeded cache");
}

#[test]
fn cache_false_step_skips_cache_read_write() {
    let (db, _dir) = temp_db();
    let db = Arc::new(db);
    let key = compute_cache_key("var", &json!({"value": 42}));
    cache_seed(&db, &key, &json!("poisoned"));

    let result = run_yaml_with_db(VAR_YAML_NO_CACHE, HashMap::new(), db.clone()).expect("run");
    assert_eq!(result, json!({"value": 42}), "cache:false must not read cache");
    assert_eq!(
        cache_lookup(&db, &key),
        Some(json!("poisoned")),
        "cache:false must not write cache"
    );
}

#[test]
fn command_operator_is_not_cached() {
    let (db, _dir) = temp_db();
    let db = Arc::new(db);
    let key = compute_cache_key(
        "command",
        &json!({"command": "echo hi", "shell": null, "stdin": null}),
    );

    let yaml = r#"
name: cmd_cache
steps:
  - id: c
    type: command
    inputs:
      command: "echo hi"
output: "{c.output}"
"#;
    for _ in 0..2 {
        let result = run_yaml_with_db(yaml, HashMap::new(), db.clone()).expect("run");
        assert_eq!(result["stdout"], json!("hi\n"));
        assert_eq!(
            cache_lookup(&db, &key),
            None,
            "command (spec().cache == false) must not be cached"
        );
    }
}

#[test]
fn iterate_cache_key_includes_over_array() {
    let (db, _dir) = temp_db();
    let db = Arc::new(db);

    let yaml = r#"
name: iter_cache
slots:
  - name: items
    schema:
      type: array
steps:
  - id: p
    type: base64
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      mode: "encode"
output: "{p.output}"
"#;

    let mut slots = HashMap::new();
    slots.insert("items".into(), json!(["a", "b"]));
    let r1 = run_yaml_with_db(yaml, slots, db.clone()).expect("run 1");
    assert_eq!(r1, json!(["YQ==", "Yg=="]));

    let mut slots = HashMap::new();
    slots.insert("items".into(), json!(["c"]));
    let r2 = run_yaml_with_db(yaml, slots, db.clone()).expect("run 2");
    assert_eq!(
        r2,
        json!(["Yw=="]),
        "different over data must not hit the previous iterate cache entry"
    );
}
