use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use weaveflow::dsl::parser::parse;
use weaveflow::dsl::validator::validate;
use weaveflow::engine::dag::Dag;
use weaveflow::engine::runner::Runner;
use weaveflow::error::WeaveflowResult;
use weaveflow::store::Database;
use weaveflow::tracker::{LayerInfo, TaskTracker};

pub fn temp_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("weaveflow.redb"))
        .expect("open db");
    (db, dir)
}

#[allow(dead_code)]
pub fn run_yaml(yaml: &str, slots: HashMap<String, Value>) -> WeaveflowResult<Value> {
    let (db, _dir) = temp_db();
    run_yaml_with_db(yaml, slots, Arc::new(db))
}

pub fn run_yaml_with_db(
    yaml: &str,
    slots: HashMap<String, Value>,
    db: Arc<Database>,
) -> WeaveflowResult<Value> {
    let def = parse(yaml)?;
    let report = validate(&def);
    assert!(report.is_ok(), "validation: {:?}", report.errors);

    let tracker = TaskTracker::new();

    let dag = Dag::from_pipeline(&def).expect("dag");
    let layers = dag.topological_sort().expect("topo");
    let steps_with_timeout = layers
        .iter()
        .flatten()
        .map(|id| (id.clone(), dag.step(id).and_then(|s| s.timeout_sec)))
        .collect::<Vec<_>>();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo {
            index: i,
            step_ids: step_ids.clone(),
        })
        .collect();

    let task_id = db
        .create_task(&def.name, serde_json::json!(slots), 3600)
        .expect("create task");
    let rt = rt();
    rt.block_on(tracker.create(
        task_id,
        def.name.to_string(),
        steps_with_timeout,
        layer_infos,
    ));

    let runner = Runner::new(def, db.clone(), tracker);
    let result_bytes = rt.block_on(runner.run(task_id, slots))?;
    Ok(serde_json::from_slice(&result_bytes).unwrap_or(Value::Null))
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}
