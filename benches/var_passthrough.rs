use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;

#[path = "shared.rs"]
mod shared;
use shared::*;

fn bench_var_passthrough(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_passthrough");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));
    let tmpdir = TempDir::new("var");

    let yaml = build_var_passthrough_pipeline();
    let def = weaveflow::dsl::parser::parse(&yaml).expect("parse");
    let counter = AtomicUsize::new(0);

    group.bench_function("var_json_roundtrip", |b| {
        b.iter(|| {
            let (db, dir) = fresh_db(&tmpdir, "var", &counter);
            let n = run_once(&rt, &def, &db, slots.clone());
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
            black_box(n);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_var_passthrough);
criterion_main!(benches);
