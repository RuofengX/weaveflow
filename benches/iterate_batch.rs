use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;

#[path = "shared.rs"]
mod shared;
use shared::*;

fn bench_iterate_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterate_batch");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));
    let tmpdir = TempDir::new("iter");

    for &bs in &[Some(100), Some(200), Some(500), None] {
        let label = bs.map(|n| format!("batch_size_{n}")).unwrap_or_else(|| "batch_none".into());
        let yaml = build_iterate_pipeline(bs);
        let def = weaveflow::dsl::parser::parse(&yaml).expect("parse");

        let counter = AtomicUsize::new(0);
        group.bench_function(format!("{label}_miss"), |b| {
            b.iter(|| {
                let (db, dir) = fresh_db(&tmpdir, &format!("m-{label}"), &counter);
                let n = run_once(&rt, &def, &db, slots.clone());
                drop(db);
                let _ = std::fs::remove_dir_all(&dir);
                black_box(n);
            })
        });

        let (db_hit, _dir_hit) = fresh_db(&tmpdir, &format!("h-{label}"), &AtomicUsize::new(0));
        run_once(&rt, &def, &db_hit, slots.clone());
        group.bench_function(format!("{label}_hit"), |b| {
            b.iter(|| {
                let n = run_once(&rt, &def, &db_hit, slots.clone());
                black_box(n);
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_iterate_batch);
criterion_main!(benches);
