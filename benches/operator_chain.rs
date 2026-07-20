use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;

#[path = "shared.rs"]
mod shared;
use shared::*;

fn bench_operator_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("operator_chain");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let items = generate_nested_data(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("items".into(), json!(items));
    let tmpdir = TempDir::new("chain");

    let yaml = build_chained_pipeline();
    let def = weaveflow::dsl::parser::parse(&yaml).expect("parse");

    let counter = AtomicUsize::new(0);
    group.bench_function("chain_filter_sort_dedup", |b| {
        b.iter(|| {
            let (db, dir) = fresh_db(&tmpdir, "chain", &counter);
            let n = run_once(&rt, &def, &db, slots.clone());
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
            black_box(n);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_operator_chain);
criterion_main!(benches);
