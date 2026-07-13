// scripts/bench_ecomm_mongo.js — Full-scope snapshots per step (same as Rust)
// Each step snapshot = {slots: orders, steps: {all previous: {output: data}}}

const { MongoClient } = require('mongodb');

const MONGO_URL = 'mongodb://localhost:27017';
const DB_NAME = 'weave_bench_ecomm';
const STEP_IDS = ['filter_paid', 'calc_item_totals', 'dedup_cities', 'sort_by_total', 'enrich_region'];

async function main() {
    const client = new MongoClient(MONGO_URL);
    await client.connect();
    const db = client.db(DB_NAME);
    const snapshots = db.collection('snapshots');
    await db.dropDatabase();

    let raw = '';
    for await (const chunk of process.stdin) raw += chunk;
    let orders = JSON.parse(raw);

    const scope = { slots: { orders }, steps: {} };
    const start = performance.now();

    // Step 1: filter paid
    let filtered = orders.filter(o => o.status === 'paid');
    scope.steps[STEP_IDS[0]] = { output: filtered };
    await snapshots.insertOne({ seq: 1, step_id: STEP_IDS[0], scope: JSON.stringify(scope) });

    // Step 2: calc item totals
    orders.forEach(o => {
        o.item_totals = o.items.map(it => ({ name: it.name, total: it.qty * it.unit_price }));
    });
    scope.steps[STEP_IDS[1]] = { output: orders };
    await snapshots.insertOne({ seq: 2, step_id: STEP_IDS[1], scope: JSON.stringify(scope) });

    // Step 3: dedup cities
    const seen = {};
    const cities = [];
    orders.forEach(o => {
        if (!seen[o.city]) { seen[o.city] = true; cities.push(o); }
    });
    scope.steps[STEP_IDS[2]] = { output: cities };
    await snapshots.insertOne({ seq: 3, step_id: STEP_IDS[2], scope: JSON.stringify(scope) });

    // Step 4: sort by total desc
    filtered.sort((a, b) => b.total - a.total);
    scope.steps[STEP_IDS[3]] = { output: filtered };
    await snapshots.insertOne({ seq: 4, step_id: STEP_IDS[3], scope: JSON.stringify(scope) });

    // Step 5: enrich region
    const regions = {
        Beijing: 'North', Shanghai: 'East', Shenzhen: 'South',
        Hangzhou: 'East', Chengdu: 'West', Guangzhou: 'South',
        Nanjing: 'East', Wuhan: 'Central', Xian: 'West', Chongqing: 'West'
    };
    filtered.forEach(o => { o.region = regions[o.city] || 'Unknown'; });
    scope.steps[STEP_IDS[4]] = { output: filtered };
    await snapshots.insertOne({ seq: 5, step_id: STEP_IDS[4], scope: JSON.stringify(scope) });

    const total = performance.now() - start;

    await db.dropDatabase();
    await client.close();

    process.stdout.write(JSON.stringify({
        count: filtered.length,
        total_ms: total.toFixed(0),
    }));
}

main().catch(e => { process.stderr.write(String(e)); process.exit(1); });
