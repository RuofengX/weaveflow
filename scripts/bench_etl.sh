#!/bin/bash
# E-commerce ETL benchmark: Node.js + MongoDB (per-step persistence)
# Compares against `cargo bench --bench etl` (Rust + redb)
#
# Usage: bash scripts/bench_etl.sh

set -e

echo "=== E-commerce ETL Benchmark ==="
echo ""

# Generate orders JSON via Python (same data as Rust bench)
python3 -c "
import json, sys
statuses = ['paid','pending','paid','cancelled','paid']
cities = ['Beijing','Shanghai','Shenzhen','Hangzhou','Chengdu','Guangzhou','Nanjing','Wuhan','Xian','Chongqing']
items_pool = ['laptop','phone','tablet','monitor','keyboard','mouse']
orders = []
for i in range(10000):
    item_count = 1 + (i % 5)
    items = []
    for j in range(item_count):
        idx = (i + j) % len(items_pool)
        qty = 1 + ((i * 3 + j) % 10)
        unit_price = (100.0 + ((i * 7 + j * 13) % 9000)) / 10.0
        items.append({'name': items_pool[idx], 'qty': qty, 'unit_price': unit_price})
    total = sum(it['qty'] * it['unit_price'] for it in items)
    orders.append({
        'order_id': f'ORD-{i:06d}',
        'user_id': f'U{i % 1000}',
        'city': cities[i % len(cities)],
        'status': statuses[i % len(statuses)],
        'items': items,
        'total': round(total, 2),
    })
with open('/tmp/weaveflow_ecomm_input.json', 'w') as f:
    json.dump(orders, f)
print(f'Generated {len(orders)} orders → /tmp/weaveflow_ecomm_input.json')
"

echo ""
echo "--- Node.js + MongoDB ---"
for i in 1 2 3; do
    cat /tmp/weaveflow_ecomm_input.json | node scripts/bench_ecomm_mongo.js
    echo ""
done

echo ""
echo "--- Rust + redb ---"
echo "Run: cargo bench --bench etl"
echo ""
