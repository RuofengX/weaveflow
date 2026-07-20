// scripts/bench_js_with_mongo.js — ETL pipeline with per-step MongoDB persistence
// Usage: cat input.json | node scripts/bench_js_with_mongo.js
// Each step writes to its own collection, simulating snapshots.

const { MongoClient } = require('mongodb');

const MONGO_URL = 'mongodb://localhost:27017';
const DB_NAME = 'weaveflow_bench';
const INPUT_COL = 'users';
const STEP_NAMES = ['filter_adults', 'sort_by_age', 'dedup_email', 'add_processed'];

async function main() {
    const client = new MongoClient(MONGO_URL);
    await client.connect();
    const db = client.db(DB_NAME);
    await db.dropDatabase();

    // Read stdin
    let raw = '';
    for await (const chunk of process.stdin) raw += chunk;
    let users = JSON.parse(raw);

    const start = performance.now();
    const totalSteps = STEP_NAMES.length;

    // Step 1: filter
    let s1 = performance.now();
    users = users.filter(u => u.age >= 18);
    await db.collection(STEP_NAMES[0]).insertMany(users);
    let elapsed1 = performance.now() - s1;

    // Step 2: sort
    let s2 = performance.now();
    users.sort((a, b) => a.age - b.age);
    await db.collection(STEP_NAMES[1]).insertMany(users);
    let elapsed2 = performance.now() - s2;

    // Step 3: dedup
    let s3 = performance.now();
    const seen = {};
    const deduped = [];
    for (const u of users) {
        if (!seen[u.email]) { seen[u.email] = true; deduped.push(u); }
    }
    users = deduped;
    await db.collection(STEP_NAMES[2]).insertMany(users);
    let elapsed3 = performance.now() - s3;

    // Step 4: transform
    let s4 = performance.now();
    users.forEach(u => { u.processed = true; });
    await db.collection(STEP_NAMES[3]).insertMany(users);
    let elapsed4 = performance.now() - s4;

    const total = performance.now() - start;

    // Cleanup
    await db.dropDatabase();
    await client.close();

    process.stdout.write(JSON.stringify({
        count: users.length,
        per_step_ms: [elapsed1, elapsed2, elapsed3, elapsed4].map(x => x.toFixed(2)),
        total_ms: total.toFixed(2),
    }));
}

main().catch(e => { process.stderr.write(String(e)); process.exit(1); });
