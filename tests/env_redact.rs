#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use weave::dsl::parser::parse;
use weave::dsl::validator::{ValidateOptions, validate};
use weave::engine::dag::Dag;
use weave::engine::runner::Runner;
use weave::tracker::{LayerInfo, TaskTracker};

#[test]
fn env_value_redacted_in_persisted_snapshot() {
    unsafe { std::env::set_var("WEAVE_TEST_REDACT_SECRET", "sk-live-secret-12345") };
    let yaml = r#"
name: env_redact
steps:
  - id: leak
    type: var
    inputs:
      value:
        api_key: "{env.WEAVE_TEST_REDACT_SECRET}"
        note: "plain"
output: "{leak.output.value}"
"#;
    let def = parse(yaml).expect("parse");
    let report = validate(&def, &ValidateOptions::default());
    assert!(report.is_ok(), "validation: {:?}", report.errors);

    let (db, _dir) = common::temp_db();
    let db = Arc::new(Mutex::new(db));
    let tracker = TaskTracker::new();
    let dag = Dag::from_pipeline(&def).expect("dag");
    let layers = dag.topological_sort().expect("topo");
    let all_step_ids = layers.iter().flatten().cloned().collect::<Vec<_>>();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo {
            index: i,
            step_ids: step_ids.clone(),
        })
        .collect();
    let task_id = {
        let guard = db.try_lock().expect("db lock");
        guard
            .create_task(&def.name, serde_json::json!({}), 3600)
            .expect("create task")
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(tracker.create(
        task_id,
        def.name.to_string(),
        all_step_ids,
        layer_infos,
    ));

    let runner = Runner::new(def, db.clone(), tracker);
    let result = rt
        .block_on(runner.run(task_id, HashMap::new()))
        .expect("run");
    let output: Value = serde_json::from_slice(&result).expect("output json");
    assert_eq!(output["api_key"], "sk-live-secret-12345");

    let snaps = db
        .try_lock()
        .expect("db lock")
        .load_snapshots(&task_id)
        .expect("load snapshots");
    assert_eq!(snaps.len(), 1);
    let persisted: Value = serde_json::from_slice(&snaps[0].1.output).expect("snapshot json");
    assert_eq!(persisted["value"]["api_key"], "***");
    assert_eq!(persisted["value"]["note"], "plain");
    let raw = String::from_utf8_lossy(&snaps[0].1.output);
    assert!(!raw.contains("sk-live-secret-12345"));
}

#[test]
fn short_env_value_not_redacted() {
    unsafe { std::env::set_var("WEAVE_TEST_SHORT", "abc") };
    let yaml = r#"
name: env_short
steps:
  - id: s1
    type: var
    inputs:
      value: "{env.WEAVE_TEST_SHORT}"
output: "{s1.output.value}"
"#;
    let result = common::run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result, Value::String("abc".to_string()));
}
