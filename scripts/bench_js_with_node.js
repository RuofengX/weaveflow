// scripts/bench_js_with_node.js — pure JS ETL, take JSON input, output JSON
const readline = require('readline');

let input = '';
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line) => { input += line; });
rl.on('close', () => {
    const users = JSON.parse(input);

    const start = performance.now();

    const filtered = users.filter(u => u.age >= 18);
    filtered.sort((a, b) => a.age - b.age);
    const seen = {};
    const deduped = [];
    for (let i = 0; i < filtered.length; i++) {
        const email = filtered[i].email;
        if (!seen[email]) {
            seen[email] = true;
            deduped.push(filtered[i]);
        }
    }
    deduped.forEach(u => { u.processed = true; });

    const elapsed = performance.now() - start;
    process.stdout.write(JSON.stringify({ count: deduped.length, ms: elapsed.toFixed(2) }));
});
