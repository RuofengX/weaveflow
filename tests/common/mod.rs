use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Mutex;
use weave::dsl::parser::parse;
use weave::dsl::validator::{validate, ValidateOptions};
use weave::error::WeaveResult;
use weave::runtime::dag::Dag;
use weave::runtime::Executor;
use weave::store::Database;
use weave::task::{LayerInfo, TaskTracker};

pub fn temp_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("weave.redb"))
        .expect("open db");
    (db, dir)
}

pub fn run_yaml(yaml: &str, slots: HashMap<String, Value>) -> WeaveResult<Value> {
    let def = parse(yaml).expect("parse");
    let report = validate(&def, &ValidateOptions::default());
    assert!(report.is_ok(), "validation: {:?}", report.errors);

    let (db, _dir) = temp_db();
    let db = Arc::new(Mutex::new(db));
    let tracker = Arc::new(TaskTracker::new());

    let dag = Dag::from_pipeline(&def).expect("dag");
    let layers = dag.topological_sort().expect("topo");
    let all_step_ids: Vec<String> = layers.iter().flatten().cloned().collect();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo {
            index: i,
            step_ids: step_ids.clone(),
        })
        .collect();

    let task_id = {
        let db_guard = db.try_lock().expect("db lock");
        db_guard
            .create_task(&def.name, serde_json::json!(slots), 3600)
            .expect("create task")
    };
    let rt = rt();
    rt.block_on(tracker.create(
        task_id,
        def.name.to_string(),
        all_step_ids,
        layer_infos,
    ));

    let executor = Executor::new(def, db.clone(), tracker);
    let result_bytes = rt.block_on(executor.run(task_id, slots, 3600))?;
    Ok(serde_json::from_slice(&result_bytes).unwrap_or(Value::Null))
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}
