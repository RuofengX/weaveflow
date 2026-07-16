# weave — DAG batch engine

## Quick reference

```bash
cargo build                    # compile
cargo test --lib               # 36 unit tests (no external deps)
cargo test --test '*'          # 21 integration tests (requires local weave binary)
cargo bench --bench '*'        # ETL benchmarks (5 benches)
```

## Architecture

```
src/
├── main.rs              # CLI entry (clap) — dispatches daemon/client via process
├── lib.rs
├── dsl/                 # YAML → PipelineDef parsing & validation
│   ├── parser.rs         YAML parsing (rust-yaml)
│   ├── raw.rs            RawStepDef → StepDef: explicit field-level conversion, no serde_json::from_value
│   ├── step_op.rs        StepOp tagged enum + per-operator Inputs structs (14 operators)
│   ├── step.rs           StepDef (id, after, iterate, retry, cache, timeout, #[serde(flatten)] op)
│   ├── variable.rs       RefValue { Literal | Ref }, VariablePath, parse_string_to_refvalue (derive Serialize/Deserialize)
│   ├── pipeline.rs       PipelineDef (name, slots, steps, output)
│   ├── validator.rs      compile-time-like checks (step ids, refs, iterate config, JSON Schema)
│   ├── rule.rs           RuleDef
│   ├── storage.rs        StorageDef
│   └── retry.rs          RetryDef, BackoffStrategy
├── engine/              # DAG executor
│   ├── dag.rs            Dag + Kahn topological sort
│   ├── runner.rs         Top-level run orchestrator
│   ├── step.rs           Single-step runner (resolve → operator → snapshot)
│   ├── cache.rs          SHA256 content-addressed cache lookups
│   └── iterate.rs        Iterate expansion (over + as + batch + max_workers)
├── operator/            # Operator trait + registry + builtins
│   ├── types.rs          Operator trait, OperatorSpec, OperatorError
│   ├── registry.rs       OperatorRegistry (compile-time registration)
│   └── builtin/          14 builtin operators
├── vm/                  # Variable resolution & scope
│   ├── scope.rs          Scope (HashMap<String, Arc<Value>> backed)
│   └── resolver.rs       resolve_value_tree — bfs ResolveRefs → replace into cloned JSON
├── tracker/             # In-memory runtime state + WS broadcast
│   ├── tracker.rs        TaskTracker (HashMap<TaskId, RunState> + broadcast)
│   ├── state.rs          StepState, TaskStatus state machines
│   ├── snapshot.rs       Snapshot (per-step output bytes)
│   └── meta.rs           TaskMeta
├── store/               # redb embedded KV
│   ├── database.rs       PIPELINE/TASK/SNAPSHOT/OBJECT/CACHE tables
│   └── object.rs         ObjectDigest + ObjectValue (all inline, no spill files)
├── quickjs/             # QuickJS sandbox (rquickjs)
├── cli/                 # CLI support
│   ├── daemon.rs         Axum HTTP server + daemon lifecycle (start/stop/restart via pidfile)
│   ├── client.rs         HTTP client for CLI→daemon communication
│   └── watch.rs          ratatui TUI + --text-output progress rendering
└── error.rs             # WeaveError
```

## Key design decisions

| Dimension | Decision |
|-----------|----------|
| Operator extension | Compile-time registration, `#[async_trait]` + `OperatorRegistry` |
| Step config | `StepOp` tagged enum (`#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]`) — 14 variants, each with typed Inputs struct |
| Input model | Pipeline-level `slots` (placeholders), step-level `inputs` (per-operator typed structs) |
| Raw → Pipeline conversion | `raw.rs` uses `Raw*Inputs` structs with plain types (no RefValue); `From<RawStepOp> for StepOp` explicitly converts `"{...}"` strings to `RefValue::Ref` |
| Scope | HashMap<String, Arc<Value>> — O(1) get/set, clone = refcount inc |
| Storage | redb — all values inline, no external spill files |
| Cache | SHA256(resolved inputs bytes) content-addressed dedup |
| Variables | `{slots.name}` / `{env.KEY}` / `{step_id.output}` / `{step_id.output.field}` |
| Concurrency | DAG layer `join_all` + iterate chunk `join_all` + rayon inside operators |

## Commands

```
weave serve --bind 0.0.0.0:8080
weave daemon start|stop|restart
weave pipeline apply <file.yml> [--data key=value]
weave pipeline ls
weave pipeline inspect <name>
weave pipeline delete <name>
weave run <name> [-i key=value] [--watch|--text-output]
weave check <file.yml>
weave task ls
weave task snapshot list <task_id>
weave task snapshot show <task_id> <seq>
weave system prune [--force] [--dry-run]
weave system operators
```

## StepOp: tagged enum with per-operator Inputs

```rust
// src/dsl/step_op.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]
pub enum StepOp {
    Http(HttpInputs),
    Js(JsInputs),
    Filter(FilterInputs),
    Sort(SortInputs),
    Dedup(DedupInputs),
    Merge(MergeInputs),
    Base64(Base64Inputs),
    Noop,                            // no inputs
    Var(VarInputs),
    File(FileInputs),
    Command(CommandInputs),
    Llm(LlmInputs),
}
```

`#[serde(rename_all = "lowercase")]` means the variant name IS the DSL `type:` value — no per-variant `rename` needed.

### Adding a new operator — three places

1. **`src/dsl/step_op.rs`** — add variant to `StepOp` + `op_type()` match arm + Inputs struct
2. **`src/dsl/raw.rs`** — add variant to `RawStepOp` + `Raw*Inputs` struct + `From<RawStepOp> for StepOp` arm (use `yaml_to_refvalue` for fields that accept `{...}` refs)
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
    async fn run(&self, inputs: &Value)
        -> Result<Value, OperatorError>;
}

Return `Value` — all operator outputs are JSON. Scope natively stores `Arc<Value>`.

### Builtin operators (12)

| Operator | DSL type | Feature |
|----------|----------|---------|
| HTTP | `http` | HTTP requests |
| JS sandbox | `js` | Inline QuickJS, `code` field in inputs (RefValue: literal string or `{step.output}` ref) |
| Filter | `filter` | Array filter by field/operator/value (rayon parallel) |
| Sort | `sort` | Array sort by field/order (rayon parallel) |
| Dedup | `dedup` | Array deduplication by field |
| Merge | `merge` | Merge two objects (`a` + `b`) |
| Base64 | `base64` | Encode/decode base64 |
| Noop | `noop` | Passthrough (test helper) |
| Var | `var` | Variable placeholder — passes inputs through as-is |
| File | `file` | Read files (path or url) |
| Command | `command` | Execute shell commands |
| LLM | `llm` | LLM API calls |

Detailed input fields, defaults, and examples: [docs/operators.md](docs/operators.md).

## Raw → Pipeline conversion

`raw.rs` uses two-layer deserialization:

```
YAML → serde_yaml → RawPipelineDef (Raw*Inputs with plain types)
     → TryFrom → PipelineDef (Inputs with RefValue fields)
```

The `Raw*Inputs` structs use `Value` for fields that may contain `{...}` refs (url, data, body, etc.) and plain types (String, u64, bool) for scalar fields. The conversion helper `yaml_to_refvalue(&Value) -> RefValue` detects `{...}` patterns and produces the appropriate `RefValue::Ref` or `RefValue::Literal`.

Top-level `Value::Object`/`Value::Array` passed as `RefValue::Literal` have nested `"{...}"` strings replaced with `{"Ref": {"parts": [...]}}` inline tags, so the resolver can find refs deep inside literal JSON structures.

## RefValue

```rust
// src/dsl/variable.rs — derive Serialize + Deserialize only, no custom visitor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefValue {
    Literal(Value),
    Ref(VariablePath),
}
```

`VariablePath` is a simple struct: `{ parts: Vec<String> }` with `VariablePath::parse(&str)` for `"{a.b.c}"` strings.

`parse_string_to_refvalue(&str) -> RefValue` is used for the pipeline `output` field and rule inputs. `to_value() -> Value` converts back for runtime rule config.

No custom `visit_*` deserialization — the `{...}` detection happens in `raw.rs` during conversion, not in the `RefValue` Deserialize.

## Resolver

In `src/vm/resolver.rs`, `resolve_value_tree` resolves `RefValue::Ref` →Value from scope. All resolved fields are merged into a single `Value::Object` — there is no data/config split at the resolver level. Each operator receives the full inputs map and accesses whatever keys it needs.

## Validator: ref detection

The validator serializes each step's op via `serde_json::to_value(&step.op)` and walks the resulting `Value` tree. It recognizes refs by looking for `{"Ref": {"parts": [...]}}` objects (the derive Serialize format of `RefValue::Ref`). For `JsInputs.code` (a plain `String`), the `extract_refs` function scans for `{step_id.field}` patterns.

## API endpoints (daemon HTTP)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/runs` | Submit task → `{task_id, pipeline_name, layers}` (async, returns immediately) |
| GET | `/runs/:task_id` | Task status + progress |
| WS | `/runs/:task_id/ws` | Real-time progress push (TaskSnapshot JSON) |
| GET | `/runs/:task_id/snapshots` | List snapshots |
| GET | `/runs/:task_id/snapshots/:seq` | Get snapshot by sequence |
| POST/GET | `/pipelines` | Create/list pipelines |
| GET/DELETE | `/pipelines/:name` | Inspect/delete pipeline |
| GET | `/tasks` | List tasks |
| POST | `/prune` | Prune completed tasks |
| GET | `/system/operators` | List registered operators |

## Caveats

- All operator outputs are JSON `Value` — Scope natively stores `Arc<Value>`.
- JS operator: `code` field is a `RefValue` — supports literal JS strings and `{step_id.output}` refs. Resolved code is read from inputs at runtime.
- When adding a new step type to `StepOp`, you MUST also add the corresponding `Raw*Inputs` struct + `RawStepOp` variant + `From` conversion arm in `src/dsl/raw.rs`. Missing any of these = compile error at the `From<RawStepOp> for StepOp` match.
