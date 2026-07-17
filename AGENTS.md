# weave — DAG batch engine

## Quick reference

```bash
cargo build                    # compile
cargo test --lib               # 37 unit tests (no external deps)
cargo test --test '*'          # 27 integration tests — in-process Runner + temp redb,
                               # NO daemon/binary required (see tests/common/mod.rs)
cargo test --test js_code_template   # single integration file
cargo test <name_substring>    # single test by name
cargo bench --bench '*'        # 5 ETL benches (+ benches/shared.rs helper)
```

No lint/typecheck config beyond `cargo clippy` / `cargo fmt` defaults.

## Architecture

```
src/
├── main.rs              # CLI entry (clap) — all subcommands defined here
├── lib.rs
├── dsl/                 # YAML → PipelineDef parsing & validation
│   ├── parser.rs         YAML parsing (rust-yaml)
│   ├── raw.rs            RawStepDef → StepDef: explicit field-level conversion, no serde_json::from_value
│   ├── step_op.rs        StepOp tagged enum + per-operator Inputs structs (12 operators)
│   ├── step.rs           StepDef (id, after, iterate, retry, cache, timeout, #[serde(flatten)] op)
│   ├── variable.rs       RefValue { Literal | Ref }, VariablePath, parse_string_to_refvalue
│   ├── pipeline.rs       PipelineDef (name, slots, steps, output, rules)
│   ├── validator.rs      compile-time-like checks (step ids, refs, iterate config, JSON Schema)
│   ├── rule.rs           RuleDef
│   ├── storage.rs        StorageDef (TTL strings like "30d" — chrono TimeDelta, overflows panic)
│   └── retry.rs          RetryDef, BackoffStrategy
├── engine/              # DAG executor
│   ├── dag.rs            Dag + Kahn topological sort + implicit deps from input refs
│   ├── runner.rs         Top-level run orchestrator
│   ├── step.rs           Single-step runner (resolve → cache → operator → retry)
│   ├── cache.rs          compute_cache_key (see caveat: NOT SHA256)
│   └── iterate.rs        Iterate expansion (over + as + batch + max_workers)
├── operator/            # Operator trait + registry + builtins
│   ├── types.rs          Operator trait, OperatorSpec { iterate, cache }, OperatorError
│   ├── registry.rs       OperatorRegistry (compile-time registration)
│   └── builtin/          12 builtin operators
├── vm/                  # Variable resolution & scope
│   ├── scope.rs          Scope (HashMap<String, Arc<Value>> backed)
│   └── resolver.rs       resolve_value_tree — recursive RefValue::Ref → Value from scope
├── tracker/             # In-memory runtime state + WS broadcast
│   ├── tracker.rs        TaskTracker (HashMap<TaskId, RunState> + broadcast)
│   ├── state.rs          StepState, TaskStatus state machines
│   ├── snapshot.rs       TaskSnapshot (WS payload)
│   └── meta.rs           TaskMeta
├── store/               # redb embedded KV
│   ├── mod.rs            Database facade: PIPELINE/TASK/SNAPSHOT/OBJECT/CACHE tables + prune
│   ├── database.rs       redb table defs + (de)serialization helpers
│   └── object.rs         ObjectDigest (SHA256) + ObjectValue (all inline, no spill files)
├── quickjs/             # QuickJS sandbox (rquickjs), one Runtime per call
├── server/              # daemon side
│   ├── daemon.rs         Axum HTTP server + daemon lifecycle (start/stop/restart via pidfile)
│   └── logging.rs        ring-buffer log store for `weave daemon log`
└── cli/                 # CLI client side
    ├── client.rs         HTTP/WS client for CLI→daemon
    └── watch.rs          ratatui TUI + --text-output progress rendering
```

## Key design decisions

| Dimension | Decision |
|-----------|----------|
| Operator extension | Compile-time registration, `#[async_trait]` + `OperatorRegistry` |
| Step config | `StepOp` tagged enum (`#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]`) — 12 variants, each with typed Inputs struct |
| Input model | Pipeline-level `slots` (placeholders), step-level `inputs` (per-operator typed structs) |
| Raw → Pipeline conversion | `raw.rs` uses `Raw*Inputs` structs with plain types (no RefValue); `From<RawStepOp> for StepOp` explicitly converts `"{...}"` strings to `RefValue::Ref` |
| Scope | HashMap<String, Arc<Value>> — O(1) get/set, clone = refcount inc |
| Storage | redb — all values inline, no external spill files |
| Cache | `SHA256(DefaultHasher64(op_type + ":" + inputs_json))` — inner 64-bit non-crypto hash caps collision resistance; `DefaultHasher` output is not stable across Rust versions |
| Variables | `{slots.name}` / `{env.KEY}` / `{step_id.output}` / `{step_id.output.field}` |
| Concurrency | DAG layer `join_all` + iterate chunk `join_all` + rayon inside operators |
| Daemon concurrency | `--max-concurrent-tasks` flag or `WEAVE_MAX_CONCURRENT_TASKS` env (default unlimited), semaphore in daemon.rs |

## Commands

```
weave daemon start [--bind 127.0.0.1:9928] [--max-concurrent-tasks N]
weave daemon stop|restart|log [-f]
weave serve --bind ...         # hidden; foreground equivalent of daemon start
weave pipeline apply -f <file.yml> | -d '<yaml string>'   # -f and -d are FLAGS, not positional
weave pipeline ls              # alias: list
weave pipeline inspect <name>
weave pipeline delete <name>
weave run <name> [-i k=v] [-i k=@file.json] [--watch|--text-output]
weave check -f <file.yml>      # local validation, no daemon needed
weave task ls
weave task snapshot list <task_id>
weave task snapshot show <task_id> <seq>
weave system prune [--force] [--dry-run]
weave system operators
```

Global flag: `--daemon <host:port>` (default `127.0.0.1:9928`).
Data dir: `WEAVE_DATA` env var only (default `~/.weave`). There is **no working `--data-dir` flag** — the passthrough code in main.rs is dead (clap rejects the arg).

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
2. **`src/dsl/raw.rs`** — `RawStepOp` variant + `Raw*Inputs` struct + `From<RawStepOp> for StepOp` arm (use `yaml_to_refvalue` for fields that accept `{...}` refs)
3. **`src/operator/builtin/`** — implement `Operator` trait + register in `builtin/mod.rs`

### IterateConfig

```rust
pub struct IterateConfig {
    pub over: VariablePath,          // not a plain String
    #[serde(rename = "as")]          // YAML key is "as", Rust field is as_name
    pub as_name: String,
    pub max_workers: Option<u32>,
    pub batch: Option<BatchConfig>,
}
```

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
`OperatorSpec.cache` declares cacheability — http/command/llm/file set `false`, **but the engine currently ignores it** (see Dead config below).

### Builtin operators (12)

| Operator | DSL type | Feature |
|----------|----------|---------|
| HTTP | `http` | HTTP requests — **bug: reads `inputs.data` but DSL field is `body`, so POST/PUT body is never sent** (TODO.md C1) |
| JS sandbox | `js` | Inline QuickJS, `code` field (RefValue: literal or `{step.output}` ref); ad-hoc 30s timeout that does NOT actually interrupt execution |
| Filter | `filter` | Array filter by field/operator/value (rayon) |
| Sort | `sort` | Array sort by field/order (rayon) |
| Dedup | `dedup` | Array dedup by field |
| Merge | `merge` | Shallow merge two objects (`a` + `b`) |
| Base64 | `base64` | Encode/decode base64 |
| Noop | `noop` | Passthrough (test helper) |
| Var | `var` | Variable placeholder |
| File | `file` | Read local files or URLs — no path validation |
| Command | `command` | `sh -c` execution — inherits all daemon env vars |
| LLM | `llm` | OpenAI-compatible API + images_b64 multimodal |

Detailed input fields: [docs/operators.md](docs/operators.md).

## Raw → Pipeline conversion & RefValue encoding

```
YAML → serde_yaml → RawPipelineDef (Raw*Inputs with plain types)
     → TryFrom → PipelineDef (Inputs with RefValue fields)
```

`yaml_to_refvalue(&Value)` detects whole-string `"{...}"` patterns. Embedded `{...}` inside a longer string stays a **literal** (no interpolation anywhere — parser, resolver, and validator must agree; validator currently disagrees, TODO.md H21).

Top-level `Value::Object`/`Value::Array` literals have nested `"{...}"` strings replaced with `{"Ref": {"parts": [...]}}` **inline tags**, so the resolver finds refs deep inside literal JSON. Validator (`extract_refs`) and DAG (`collect_refs`) pattern-match this exact shape — user data containing single-key `"Ref"` or `"inputs"` objects can be misinterpreted (see Gotchas).

## Resolver

`resolve_value_tree` (vm/resolver.rs) resolves `RefValue::Ref` → Value from scope and merges everything into one `Value::Object` — no data/config split. Path semantics: `{step}` or `{step.output}` = whole step output; `{step.output.field}` = field drill-down; array indices are NOT supported (`{step.output.0.name}` silently → Null).

## Engine behavior — dead config & gotchas

These fields **parse and validate but are not enforced** by the engine (engine/step.rs). Don't rely on them; don't "fix" code assuming they work:

| Field | Status |
|-------|--------|
| `step.timeout` | Never read — no `tokio::time::timeout` anywhere in engine |
| `step.cache` | Never read — cache read/write is unconditional |
| `retry.backoff` (`exponential`) | Never read — retry delay is always fixed `delay_ms` |
| `retry.validator` | Never read |
| `op.spec().cache = false` (http/command/llm/file) | Never read — everything is cached (TODO.md C2) |

Other traps:

- **iterate `over` must include braces** (`over: "{slots.items}"`). `raw.rs:281` uses `.expect()` — missing braces panic the daemon handler (TODO.md C7).
- **iterate `as_name` is not actually bound**: the current element is injected into inputs under the fixed key `"data"`; `{item}`/`{item.field}` refs pass through as literal strings. Only `data: "{item}"` works by accident (TODO.md M-audit).
- **iterate cache key excludes the `over` array** — same op inputs with different `over` data hit the same cache entry (TODO.md C4).
- **`batch.size: 0` is not validated** → usize underflow panic / huge allocation (TODO.md C3).
- **resolver collapses any object containing an `"inputs"` key at any depth** (resolver.rs:42) — literal user data with an `inputs` key loses its sibling keys.
- **iterate steps bypass retry entirely** (step.rs:53 — retry only wraps the non-iterate path).

## Security posture

- **No auth on any endpoint.** Binding `--bind 0.0.0.0` exposes unauthenticated RCE via `command`/`file` operators; even on localhost, browser CSRF (simple POST, no preflight) can create+run pipelines. Treat the daemon as localhost-only (TODO.md C6).
- `command` runs `sh -c` with the daemon's full environment; `file` reads any path the daemon can read; `{env.KEY}` values land in persisted snapshots/cache.
- `js` sandbox: no fs/net, but no memory limit and timeouts don't interrupt `while(1){}` (blocking-thread exhaustion).

## Tracker / WS flow

- `POST /runs` → `{task_id}` immediately; execution in background `tokio::spawn` (no panic guard — a runner panic leaves the task Running forever).
- WS `/runs/:task_id/ws` pushes `TaskSnapshot` JSON (broadcast channel, capacity 64, Lagged silently skipped).
- **Known race**: `get()` then `subscribe()` are two separate lock acquisitions — a fast task can finish in between and the client hangs forever (TODO.md H8).
- `RunState.status` embeds a stale Progress clone made at creation; live progress is in `steps` field of the snapshot (TODO.md C5).
- Completed tasks are never removed from the tracker — memory grows unboundedly in long-lived daemons (TODO.md H9).

## Storage (redb)

- Five tables created lazily — `load_*` on a fresh DB returns 500 instead of empty (TODO.md H12); `list_*` handles it correctly.
- Prune has no coordination with running tasks and never deletes OBJECT/CACHE rows (TODO.md H10/H11).
- Single `Arc<Mutex<Database>>` serializes all DB access; prune holds it across a full table scan.

## API endpoints (daemon HTTP)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/runs` | Submit task → `{task_id, ...}` (async, returns immediately) |
| GET | `/runs/:task_id` | Task status + progress |
| WS | `/runs/:task_id/ws` | Real-time progress push (TaskSnapshot JSON) |
| GET | `/runs/:task_id/snapshots` · `/:seq` | Snapshots |
| POST/GET | `/pipelines` | Create (YAML body) / list pipelines |
| GET/DELETE | `/pipelines/:name` | Inspect/delete pipeline |
| GET | `/tasks` | List tasks |
| POST | `/prune` | Prune tasks |
| GET | `/system/operators` · `/system/logs` | Operators / daemon ring-buffer logs |

Error mapping caveat: `WeaveError::BadRequest`/`Parse` fall into the catch-all → HTTP 500 instead of 400 (error.rs:112, TODO.md H15).

## Tests

- Unit tests live in-module (`#[cfg(test)]`): parser 6, validator 21, dag 9, store 1, daemon 1.
- Integration tests (tests/*.rs) use `tests/common/mod.rs::run_yaml` — parse → validate → in-process `Runner` with a tempfile redb. No daemon, no network, no binary.
- **Zero coverage** today: cache behavior, HTTP error codes, retry/backoff/timeout, failure injection, concurrency, http/command operator failure paths, CLI arg parsing, daemon lifecycle, prune (see TODO.md "测试覆盖缺口").

## Known bugs

A full audit (72 findings, 2026-07-17) with severity, location, cause and fix is in **TODO.md → "代码审计报告"**. Check it before touching engine/cache/resolver/daemon code — several "obvious fixes" interact with documented dead config.

## Caveats when editing

- When adding a `StepOp` variant you MUST update `raw.rs` (Raw variant + Inputs + From arm) — missing any = compile error at the `From` match.
- All operator outputs are JSON `Value`; binary data is base64-wrapped (`daemon.rs:get_snapshot_by_seq` has the display fallback).
- JS operator `code` is a `RefValue` — supports literal JS and `{step_id.output}` refs.
- `#[serde(deny_unknown_fields)]` is NOT set on Raw structs — misspelled YAML fields are silently ignored.
- `src/cli/display/` is an empty leftover directory.
