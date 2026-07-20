# weaveflow — DAG batch engine

## Quick reference

```bash
cargo build                    # compile
cargo test --lib               # 186 unit tests (no external deps)
cargo test --test '*'          # 28 integration test binaries (51 tests) — in-process
                               # Runner + temp redb, NO daemon/binary required
                               # (see tests/common/mod.rs)
cargo test --test js_code_template   # single integration file
cargo test <name_substring>    # single test by name
cargo bench --bench '*'        # 5 ETL benches (+ benches/shared.rs helper)
```

`cargo clippy --all-targets` is clean (0 warnings). No lint/typecheck config beyond `cargo clippy` / `cargo fmt` defaults.

## 提交约定

**每次提交必须在 commit message 末尾包含 trailer 行 `Model: <model-id>`**（如
`Model: kimi-for-coding/k3`），标明编写该提交代码的模型；多模型协作时逐行标注各自的 Model trailer。

## Architecture

```
src/
├── main.rs              # CLI entry (clap) — all subcommands defined here
├── lib.rs               # library facade: dsl/engine/error/operator/quickjs/store/tracker/vm
│                        # (server/ + cli/ are binary-only modules, not in lib)
├── dsl/                 # YAML → PipelineDef parsing & validation
│   ├── parser.rs         YAML parsing (rust-yaml); ParseError::Raw 透传 raw 层错误
│   ├── raw.rs            RawStepDef → StepDef: explicit field-level conversion, no serde_json::from_value;
│   │                     #[serde(deny_unknown_fields)] on ALL Raw structs — misspelled YAML keys are rejected
│   ├── step_op.rs        StepOp tagged enum + per-operator Inputs structs (12 operators)
│   ├── step.rs           StepDef (id, after, iterate, retry, cache, timeout_sec, #[serde(flatten)] op)
│   ├── variable.rs       RefValue { Literal | Ref }, VariablePath, parse_string_to_refvalue
│   ├── pipeline.rs       PipelineDef (name, slots, steps, output)
│   ├── validator.rs      compile-time-like checks (step ids, refs, iterate config + as_name
│   │                     reserved-prefix/collision rules, JSON Schema, filter/sort/base64/http
│   │                     enum whitelist, retry ≤ 100 attempts / ≤ 1h delay, timeout ≤ 365d,
│   │                     llm.temperature finite, sandboxed JS syntax check)
│   ├── rule.rs           RuleDef
│   ├── storage.rs        StorageDef { result_ttl } + Ttl ("30d" — chrono TimeDelta, overflow/non-ASCII → error)
│   └── retry.rs          RetryDef, BackoffStrategy (deny_unknown_fields)
├── engine/              # DAG executor
│   ├── dag.rs            Dag + Kahn topological sort + implicit deps from input refs
│   │                     (deps come only from RefValue::Ref + inline tags; plain String fields
│   │                     like filter.field/method/mode are literal — never deps, never interpolated)
│   ├── runner.rs         Top-level run orchestrator (sole writer of Completed step state)
│   ├── step.rs           Single-step runner (resolve → cache → operator → retry + step timeout)
│   ├── cache.rs          compute_cache_key = SHA256(op_type + ":" + inputs_json)
│   └── iterate.rs        Iterate expansion + effective_max_workers (default = available_parallelism)
├── operator/            # Operator trait + builtins
│   ├── types.rs          Operator trait, OperatorSpec { iterate, cache }, OperatorError
│   └── builtin/          12 builtin operators + http_client shared hardened client
├── vm/                  # Variable resolution & scope
│   ├── scope.rs          Scope (HashMap<StepId, Arc<Value>> + env redact set, poison-tolerant Mutex)
│   └── resolver.rs       resolve_value_tree — recursive RefValue::Ref → Value from scope
├── tracker/             # In-memory runtime state + WS broadcast
│   ├── tracker.rs        TaskTracker (HashMap<TaskId, RunState> + broadcast) — snapshot_and_subscribe atomic
│   ├── state.rs          StepState, TaskStatus state machines
│   ├── snapshot.rs       Snapshot (binary v2 layout) + SnapshotKey
│   └── meta.rs           TaskMeta
├── store/               # redb embedded KV
│   ├── mod.rs            Database facade (RwLock<redb::Database>, write lock only for compact):
│   │                     PIPELINE/TASK/SNAPSHOT/OBJECT/CACHE tables + prune + v0 auto-migration
│   ├── database.rs       redb table defs + (de)serialization helpers + SnapshotHeader view
│   └── object.rs         ObjectDigest (SHA256) + ObjectValue (all inline, no spill files, no ref_count)
├── quickjs/             # QuickJS sandbox (rquickjs), one Runtime per call; drop-guard interrupt
├── server/              # daemon side (binary-only)
│   ├── daemon.rs         Axum HTTP server + daemon lifecycle (pidfile) + graceful drain shutdown
│   └── logging.rs        ring-buffer log store, absolute offsets (X-Log-Offset / X-Log-Truncated)
└── cli/                 # CLI client side (binary-only)
    ├── client.rs         HTTP/WS client for CLI→daemon (encode_segment, per-endpoint timeouts)
    └── watch.rs          ratatui TUI + --text-output progress rendering (non-TTY auto-fallback)
```

## Key design decisions

| Dimension | Decision |
|-----------|----------|
| Operator extension | Compile-time match in `builtin/mod.rs::get_builtin`, `#[async_trait]` + `Operator` trait |
| Step config | `StepOp` tagged enum (`#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]`) — 12 variants, each with typed Inputs struct |
| Input model | Pipeline-level `slots` (placeholders), step-level `inputs` (per-operator typed structs) |
| Raw → Pipeline conversion | `raw.rs` uses `Raw*Inputs` structs with plain types (no RefValue); `From<RawStepOp> for StepOp` explicitly converts `"{...}"` strings to `RefValue::Ref` |
| Unknown YAML fields | `#[serde(deny_unknown_fields)]` on all Raw structs — misspelled keys are parse errors |
| Scope | HashMap<StepId, Arc<Value>> — O(1) get/set, clone = refcount inc; env secret set behind poison-tolerant Mutex |
| Storage | redb — all values inline, no external spill files; schema versioned via `::vN` type names; v0 DBs auto-migrate (`.v0.bak` backup, PIPELINE/TASK kept, SNAPSHOT/OBJECT/CACHE dropped) |
| Snapshot encoding | Custom binary v2: `seq(8B BE) \| step_id_len(4B BE) \| step_id \| output` (type name `weaveflow::Snapshot::v2`); `SnapshotHeader` view lists/counts without copying output |
| Cache | `SHA256(op_type + ":" + inputs_json)`; iterate steps mix the resolved `over` array into the key |
| Variables | `{slots.name}` / `{env.KEY}` / `{step_id.output}` / `{step_id.output.field}` / `{step_id.output.0.field}` (array indices supported, strict) |
| Concurrency | DAG layer `join_all` + iterate chunk `join_all` (default workers = `available_parallelism`) + rayon inside operators |
| Daemon concurrency | `--max-concurrent-tasks` flag or `WEAVEFLOW_MAX_CONCURRENT_TASKS` env (default unlimited), semaphore in daemon.rs; permit acquired inside the background task |
| Shutdown | Graceful drain: on signal, `/runs` → 503 and in-flight tasks drain up to 30s (`SHUTDOWN_DRAIN_SECS`) |

## Commands

```
weaveflow daemon start [--bind 127.0.0.1:9928] [--max-concurrent-tasks N] [--allow-remote]
weaveflow daemon stop|restart|log [-f]   # stop 等待优雅排空（最长 SHUTDOWN_DRAIN_SECS+5s）再 SIGKILL
weaveflow serve --bind ...         # hidden; foreground equivalent of daemon start
weaveflow pipeline apply -f <file.yml> | -d '<yaml string>'   # -f and -d are FLAGS, not positional
weaveflow pipeline ls              # alias: list
weaveflow pipeline inspect <name>
weaveflow pipeline delete <name>
weaveflow run <name> [-i k=v] [-i k=@file.json] [--watch|--text-output]  # mutually exclusive; task Failed → exit 1
weaveflow check -f <file.yml>      # local validation, no daemon needed
weaveflow task ls
weaveflow task snapshot list <task_id>
weaveflow task snapshot show <task_id> <seq>
weaveflow system prune [--force] [--dry-run]   # output includes snapshots_removed
weaveflow system operators
```

Global flag: `--daemon <host:port>` (default `127.0.0.1:9928`; `http://`/`https://` prefixes accepted, trailing `/` trimmed).
Data dir: `WEAVEFLOW_DATA` env var only (default `~/.weaveflow`). There is **no working `--data-dir` flag**.
`weaveflow run` on a non-TTY stdout automatically falls back to `--text-output`.

## StepOp: tagged enum with per-operator Inputs

```rust
// src/dsl/step_op.rs — 12 variants; variant name lowercased IS the DSL `type:` value
#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]
pub enum StepOp {
    Http(HttpInputs), Js(JsInputs), Filter(FilterInputs), Sort(SortInputs),
    Dedup(DedupInputs), Merge(MergeInputs), Base64(Base64Inputs), Noop,
    Var(VarInputs), File(FileInputs), Command(CommandInputs), Llm(LlmInputs),
}
```

### Adding a new operator — three places (missing any = compile error)

1. **`src/dsl/step_op.rs`** — variant + `op_type()` match arm + Inputs struct
2. **`src/dsl/raw.rs`** — `RawStepOp` variant + `Raw*Inputs` struct (`deny_unknown_fields`) + `From<RawStepOp> for StepOp` arm (use `yaml_to_refvalue` for fields that accept `{...}` refs)
3. **`src/operator/builtin/`** — implement `Operator` trait + add a match arm in `builtin/mod.rs::get_builtin`

### IterateConfig

```rust
pub struct IterateConfig {
    pub over: VariablePath,          // not a plain String
    #[serde(rename = "as")]          // YAML key is "as", Rust field is as_name
    pub as_name: String,
    pub max_workers: Option<u32>,    // validator: != 0, <= 1024
    pub batch: Option<BatchConfig>,  // validator + engine: batch.size != 0
}
```

Default concurrency: `effective_max_workers` = explicit `max_workers.max(1)`, else `available_parallelism()` (fallback 4).

## Operator trait

```rust
// src/operator/types.rs
#[async_trait]
pub trait Operator: Send + Sync {
    fn spec(&self) -> OperatorSpec;
    async fn run(&self, inputs: Value) -> Result<Value, OperatorError>;
}
```

All operator outputs are JSON `Value`; Scope stores `Arc<Value>`.
`OperatorSpec.cache` is honored by the engine: `cache_enabled = step.cache.unwrap_or(op.spec().cache)` — http/command/llm/file default to **no cache**, overridable per-step with `cache: true`.

### Builtin operators (12)

| Operator | DSL type | Feature |
|----------|----------|---------|
| HTTP | `http` | GET/POST/PUT/DELETE via shared client: no redirects (3xx returned as-is), full-DNS SSRF check (metadata IP always blocked; private IPs with `WEAVEFLOW_HTTP_BLOCK_PRIVATE=1`), 64MB streamed body cap |
| JS sandbox | `js` | Inline QuickJS, `code` field (RefValue: literal or `{step.output}` ref). **No `timeout` input field** — governed by step `timeout_sec`; on timeout the dropped future triggers the QuickJS interrupt handler via a drop-guard (real cancellation of `while(1){}`) |
| Filter | `filter` | Array filter by field/operator/value (rayon); `operator` whitelisted (eq/ne/gt/gte/lt/lte/in/contains) at validator + runtime |
| Sort | `sort` | Array sort by field/order (rayon); `order` whitelisted (asc/desc); integer-exact comparison shared with filter (`compare_json_numbers`) |
| Dedup | `dedup` | Array dedup by field; missing-field elements skipped with one aggregated warn |
| Merge | `merge` | Merge two objects (`b` over `a`); `deep: true` recurses nested objects (arrays/scalars always overwritten by `b`) |
| Base64 | `base64` | Encode/decode base64; missing `data` → Config error |
| Noop | `noop` | Passthrough (test helper); output is `{}` — not polluted by the `{"type":"noop"}` envelope |
| Var | `var` | Variable placeholder |
| File | `file` | Read local files (canonicalize + `WEAVEFLOW_FILE_ALLOW_ROOTS` allowlist, Once-warn when unset) or URLs (SSRF-checked); 64MB cap |
| Command | `command` | `sh -c` execution with `env_clear` + minimal env whitelist, `kill_on_drop`, 10MB stdout/stderr caps (keeps draining, sets `truncated: true`) |
| LLM | `llm` | OpenAI-compatible API + images_b64 multimodal |

Detailed input fields: [docs/operators.md](docs/operators.md).

## Raw → Pipeline conversion & RefValue encoding

```
YAML → serde_yaml → RawPipelineDef (Raw*Inputs with plain types)
     → TryFrom → PipelineDef (Inputs with RefValue fields)
```

`yaml_to_refvalue(&Value)` detects whole-string `"{...}"` patterns. Embedded `{...}` inside a longer string stays a **literal** everywhere — parser, resolver, validator and DAG all agree (whole-string guard).

Top-level `Value::Object`/`Value::Array` literals have nested `"{...}"` strings replaced with `{"Ref": {"parts": [...]}}` **inline tags**, so the resolver finds refs deep inside literal JSON. Resolver, validator (`parse_ref_tag`) and DAG (`collect_refs`) all require the object to have **exactly one key** (`len == 1`) before treating `"Ref"` as a tag — user data containing a `"Ref"` key alongside other keys passes through untouched. A single-key `"Ref"` object whose value is NOT a valid VariablePath (e.g. `{"Ref": 123}`, CloudFormation-style `{"Ref": "MyResource"}`) is user data: all three consumers fall back to treating it as a plain object (resolver recurses, validator skips, DAG recurses). Single-key `"Literal"` objects are RefValue serde tags **only at operator-field positions** — inside a Literal payload they are user data and pass through unwrapped.

Plain `String` typed fields (`http.method`, `filter.field/operator`, `sort.field/order`, `dedup.field`, `base64.mode`, `command.shell`, `llm.model/image_type`) are **always literal** — a whole-string `"{...}"` there is NOT a ref: resolver never parses bare strings, and validator/DAG symmetrically ignore them (no false cycle, no false variable_ref_not_found).

## Resolver

`resolve_value_tree` (vm/resolver.rs) resolves `RefValue::Ref` → Value from scope and merges everything into one `Value::Object` — no data/config split. Path semantics: `{step}` or `{step.output}` = whole step output; `{step.output.field}` = field drill-down; `{step.output.0.name}` = array index (non-numeric segment on an array or out-of-bounds index is a **hard error**). Missing object fields / segments on non-objects resolve to `Null` with a `warn!` log. `slots` paths follow the same rules (array indices supported and strict since 2026-07-20; missing object keys → `Null`).

## Engine behavior — gotchas

- **iterate `over` must include braces** (`over: "{slots.items}"`) — `raw.rs` rejects anything else with `ParseError::InvalidIterateOver`.
- **iterate `as_name` is not actually bound**: the current element is injected into inputs under the fixed key `"data"`; `{item}`/`{item.field}` refs pass through as **literal strings** (validator allows them since 2026-07-20). Write steps so the operator consumes `data` (JS, command stdin, etc.). Validator rejects `as` values that are empty, non-`[A-Za-z0-9_]`, `slots`/`env`, or colliding with a step id.
- **iterate steps are retried per element** (`retry_with_op` wraps each chunk), not as a whole.
- **iterate cache key includes the resolved `over` array** — same inputs with different `over` data do not collide.
- Step timeout (`timeout_sec`) applies to every attempt of every iterate chunk; it cancels the operator future (for JS, the drop-guard interrupts the blocking thread).
- Cache writes are best-effort: a failed `set_cache_bytes` logs `warn!` and the step still succeeds. Cache hits report `attempts = 0, cached = true`.

## Security posture

- **No auth on any endpoint (C6 still open).** `--allow-remote` is required to bind non-loopback addresses, but bearer-token auth is NOT implemented — binding `0.0.0.0` is unauthenticated RCE via `command`/`file`; even on localhost, browser CSRF (simple POST, no preflight) can create+run pipelines. Treat the daemon as localhost-only.
- `command` runs `sh -c` with `env_clear` + a minimal whitelist (PATH/HOME/LANG/LC_ALL/TZ); `file` canonicalizes both the target and each `WEAVEFLOW_FILE_ALLOW_ROOTS` root before the prefix check (empty segments filtered with a warn; unset → one `Once` warn and allow-all); `{env.KEY}` values are recorded and redacted in persisted snapshots.
- Shared HTTP client hardening: no redirects, per-DNS-result SSRF check (169.254.169.254 always blocked; IPv4-mapped IPv6 normalized before classification; `WEAVEFLOW_HTTP_BLOCK_PRIVATE=1` also covers 0.0.0.0, CGNAT 100.64/10, 198.18/15), 64MB streamed response cap. **No total/read timeout anywhere — execution timeouts exist ONLY at step level (`timeout_sec`, engine wraps `op.run`); the client never implicitly truncates long-running requests.** 10s connect_timeout is kept as a fast-fail floor for connection establishment only. **Known residual: DNS rebinding TOCTOU** — the pre-check and reqwest's connect each resolve DNS independently, so a low-TTL malicious domain can in principle pass the check and then resolve to a blocked IP (no resolve pinning with the shared client).
- `js` sandbox: no fs/net, 256MB memory limit, 1MB stack; step timeout triggers real interruption via the drop-guard. `__native__.inflate` output is capped at 256MB on the Rust side (decompression bombs can't bypass the sandbox memory limit). **Without `step.timeout_sec`, a `while(1){}` still occupies a blocking thread indefinitely (design decision: timeouts live only at step layer).**

## Tracker / WS flow

- `POST /runs` → `{task_id}` immediately (503 while draining; the draining check runs AFTER `in_flight` increment to close the shutdown TOCTOU); execution in a background `tokio::spawn` watched by a second task that fails the task if the runner panics — the watcher also marks all non-terminal steps `Failed` (`fail_non_terminal_steps`) and unconditionally decrements `in_flight` so `wait_for_drain` always converges.
- WS `/runs/:task_id/ws` pushes `TaskSnapshot` JSON (broadcast channel, capacity 64, Lagged silently skipped).
- `snapshot_and_subscribe()` builds the snapshot and subscribes in **one lock acquisition** — no get-then-subscribe race.
- Terminal tasks are reaped by `cleanup_stale()` (terminal for >10 min) — tracker memory does not grow unboundedly.
- `running_task_ids()` feeds prune so running tasks are never deleted.

## Storage (redb)

- Five tables pre-created at `Database::open`; schema-versioned type names (`::v1`, Snapshot `::v2`). Opening a v0 database auto-migrates: backup to `<file>.v0.bak`, copy PIPELINE/TASK (stripping removed `snapshot_ttl`), drop SNAPSHOT/OBJECT/CACHE.
- Concurrency: `RwLock<redb::Database>` inside `Database` (poison-tolerant); the write lock is taken **only** for `compact()`. No global DB Mutex.
- Prune is two-phase (`prune_scan` read-only → `prune_execute` write txn): skips tasks in `tracker.running_task_ids()` and tasks whose status is still `running` with no snapshots; terminal tasks without snapshots ARE prunable; only deletes snapshots with seq ≤ the scan-time max_seq (snapshots written mid-prune survive), GCs unreferenced OBJECT rows and dangling CACHE entries, then compacts.
- `save_pipeline_upsert` does the name scan + insert in a single redb write transaction (write txns are globally serialized) — concurrent same-name applies cannot double-insert.
- `find_pipeline_by_name` is a full table scan — **intentional** (pipeline count is small).
- `storage.result_ttl` is live: default 3600s, floor 60s, stored in `TaskMeta.result_ttl_secs`. `snapshot_ttl` no longer exists (unknown-field error).

## API endpoints (daemon HTTP)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/runs` | Submit task → `{task_id, ...}` (async; **503** while draining) |
| GET | `/runs/:task_id` | Task status + progress |
| WS | `/runs/:task_id/ws` | Real-time progress push (TaskSnapshot JSON) |
| GET | `/runs/:task_id/snapshots` · `/:seq` | Snapshots |
| POST/GET | `/pipelines` | Create (YAML body) / list pipelines |
| GET/DELETE | `/pipelines/:name` | Inspect/delete pipeline |
| GET | `/tasks` | List tasks |
| POST | `/prune` | Prune tasks (response includes `snapshots_removed`) |
| GET | `/system/operators` · `/system/logs` | Operators / daemon ring-buffer logs (absolute `offset`, `X-Log-Offset` / `X-Log-Truncated` headers) |

Error mapping: `WeaveflowError::BadRequest`/`Parse` → 400, `NotFound` → 404, `Unavailable` (draining) → 503, other 5xx return a fixed message (no internal detail leak).

## Tests

- **186 lib tests** in-module (`#[cfg(test)]`) across dsl/engine/operator/store/tracker/vm; **24 bin tests** under `src/server` + `src/cli` (not in lib).
- **28 integration test binaries** (51 tests) use `tests/common/mod.rs::run_yaml` — parse → validate → in-process `Runner` with a tempfile redb. No daemon, no network, no binary.
- Coverage highlights: cache behavior (`tests/cache_control.rs`), retry/backoff/timeout (`tests/retry.rs`, `tests/step_timeout.rs`), env redaction, array index paths, merge deep, noop envelope (`tests/noop_output.rs`), v0 DB migration, Snapshot v2 layout roundtrip, prune max_seq guard, mark_interrupted, JS syntax sandbox (incl. `while(1){}` watchdog), `effective_max_workers`, command 10MB truncation, http_client split_url/SSRF, file allowlist edge cases, `wait_for_drain`, pidfile binary verification, `encode_segment`, log absolute offsets, `snapshot_and_subscribe` atomicity, `cleanup_stale`.

## Known bugs / open items

The original 72-finding audit (2026-07-17), the 2026-07-18 review + second-audit fix log (incl. the remaining open list: C6 auth, L4/L5/L6/L10/L12/L13/L14, intentional full-scan, JS-without-timeout blocking threads) and the **2026-07-20 third audit (24 fixes, 6 new open items: DNS rebinding TOCTOU, corrupt-row panics, cache two-txn window, unbounded queue, filter eq semantics, file TOCTOU)** are in **TODO.md → "代码审计报告"、"2026-07-18 复审 + 二次审计修复记录"、"三次审计修复记录"**. Check them before touching engine/cache/resolver/daemon code.

## Caveats when editing

- When adding a `StepOp` variant you MUST update `raw.rs` (Raw variant + Inputs + From arm) — missing any = compile error at the `From` match.
- All operator outputs are JSON `Value`; binary data is base64-wrapped (`daemon.rs:get_snapshot_by_seq` has the display fallback).
- JS operator `code` is a `RefValue` — supports literal JS and `{step_id.output}` refs. There is no `timeout` input; use step `timeout_sec`.
- All Raw structs carry `#[serde(deny_unknown_fields)]` — misspelled YAML fields are parse errors, and removed fields (e.g. `snapshot_ttl`, JS `timeout`) now hard-fail.
- `src/cli/display/` is an empty leftover directory.
