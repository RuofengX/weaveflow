# weave — 实施路线图

## 已完成 ✅

- [x] DSL 解析层：schema / parser / validator
- [x] 语义 Key 类型：PipelineId / TaskId / SnapshotKey / ObjectDigest / CacheKey
- [x] 存储层：Database 门面 + 五表 (PIPELINE/TASK/SNAPSHOT/OBJECT/CACHE) + Prune
- [x] WeaveError 统一错误枚举 (thiserror) + Axum IntoResponse
- [x] FlatBuffer Scope：零拷贝 + memcpy clone
- [x] TaskMeta + Snapshot + Progress/TaskStatus 状态机
- [x] Operator trait + 12 个内置算子（见下）
- [x] DAG：Kahn 拓扑排序 + 环检测 + 隐式依赖 (inputs refs + after)
- [x] Executor：逐层并行 / 缓存 / 重试 / iterate 并行 / 错误传播
- [x] TaskTracker：in-memory 运行时状态 + broadcast 实时推送
- [x] weave daemon (Axum)：REST + WebSocket
- [x] weave CLI：pipeline / run / task / system + `--watch` TUI + `--text-output`
- [x] QuickJS 运行时：rquickjs 内嵌，每次调用独立 Runtime，无 fs/net
- [x] 集成测试 (20 个场景) + ETL benchmark
- [x] `read_file` 算子：本地文件 + URL 读取 (`operator/builtin/file.rs`)
- [x] `command` 算子：Shell 执行 (`operator/builtin/command.rs`)
- [x] `llm` 算子：OpenAI 兼容 API + images_b64 多模态 (`operator/builtin/llm.rs`)
- [x] `pipeline apply` upsert：同名幂等更新
- [x] `pipeline delete` 按名删除
- [x] `-i key=@/path/to/file` 文件输入
- [x] `weave prune` 清理命令
- [x] JS 算子错误栈 (`message` + `stack`)
- [x] 快照二进制数据 base64 fallback 显示 (`daemon.rs:get_snapshot_by_seq`)
- [x] `weave system operators` 算子列表

## 内置算子总览

| 算子 | 文件 | 说明 |
|------|------|------|
| `noop` | `builtin/noop.rs` | 直接透传 |
| `filter` | `builtin/filter.rs` | 数组过滤（rayon 并行） |
| `sort` | `builtin/sort.rs` | 数组排序（rayon 并行） |
| `dedup` | `builtin/dedup.rs` | 数组去重 |
| `merge` | `builtin/merge.rs` | 合并两个对象 |
| `base64` | `builtin/base64.rs` | Base64 编解码 |
| `http` | `builtin/http.rs` | HTTP 请求 |
| `file` | `builtin/file.rs` | 本地文件 / URL 读取 |
| `command` | `builtin/command.rs` | Shell 命令执行 |
| `llm` | `builtin/llm.rs` | OpenAI 兼容 LLM + 多模态图片 |
| `var` | `builtin/var.rs` | 变量占位 |
| `js` | `builtin/js.rs` | 内联 QuickJS 沙箱 |

## 待实现

### 高优先级 — P0 Bug

| 项目 | 文件 | 说明 |
|------|------|------|
| ~~file 算子含路径变量时返回 null~~ ~~| ~~`builtin/file.rs`, `executor.rs:654-713`~~ | **已修复** — v1.0 resolver 重构后变量解析正确。原报告中的 "快照 output=null" 实际根因是最终输出为二进制 bytes，`serde_json::from_slice` 失败后 fallback 为 Null。现 `runner.rs:331` 已改为 base64 包装。 |
| ~~Slot 默认值不生效~~ | **已修复** — `runner.rs:59-69` 正确应用 `schema.get("default")`，并新增 jsonschema 运行时校验。 |

### 中优先级 — P1

| 项目 | 说明 |
|------|------|
| llm 算子模型能力预检优化 | 已添加 warning + `skip_vision_check` + API error hint。可考虑自动 `/v1/models` 检查（低优先级优化，当前方案已可排查问题）。 |
| ~~`--text-output` 步骤产出为空~~ | **已修复** — `watch.rs:95-108` 已改为从 `status.Completed` 取 output 值。 |
| ~~iterate 加速~~ | **已实现** — `iterate.rs:108-112` 每 10 个 chunk 批量更新 tracker，减少 Mutex 锁竞争。 |
| http 算子的 poll_until | `http.rs` 无 poll_until 功能。规划中的 feature，非缺陷。 |

### 低优先级 — P2

| 项目 | 说明 |
|------|------|
| ~~JSON Schema 输入校验~~ | **已实现** — `runner.rs:82-105` 用 jsonschema crate 校验。 |
| ~~`weave check` 命令~~ | **已实现** — `main.rs:69-74` Check 子命令 + `check_pipeline()`. |
| ~~Snapshot null 掩盖根因~~ | **已实现** — `daemon.rs:326-335` 检测 `_anomalous_null` 并标注 raw_size/raw_hex。 |
| ~~自举 validator~~ | **已实现** — `pipeline.rs` RuleDef + `runner.rs` 执行前逐条运行 rules（支持 js 算子），返回 `{valid, error}`。validator.rs 新增 JS 语法 AOT 校验。 |
| bytecode 缓存 | QuickJS 不支持跨 Runtime 字节码复用（字节码 Runtime-tied）。rquickjs 0.12 不暴露 `JS_WriteObject`/`JS_ReadObject`。当前已有 step 级输入缓存（相同输入不重复执行）。若需优化 iterate 场景 JS 重复编译，需升级 rquickjs 版本或引入 Runtime 池。 |

---

# 代码审计报告（2026-07-17）

审计范围：全仓 ~7400 行 Rust（engine / server / cli / store / tracker / dsl / operator / vm / quickjs / main / tests）。
共发现 **72 项问题**：Critical 8 / High 21 / Medium 27 / Low 16。每项含位置、原因、修复方法。

## Critical — 立即修复（P0）

| # | 位置 | 问题与原因 | 修复方法 |
|---|------|-----------|---------|
| C1 | `operator/builtin/http.rs:25` vs `dsl/step_op.rs:59` | **http 算子读 `inputs.get("data")`，但 DSL 字段名是 `body`** → POST/PUT 的 body 永远发送空字节，静默失效 | 改为 `inputs.get("body")`；加集成测试断言服务端收到的 body |
| C2 | `engine/step.rs:41-49, 104, 132` | **算子 `spec().cache == false`（http/command/llm/file）被完全忽略**：缓存查找发生在 `resolve_operator` 之前，写缓存无条件执行 → 陈旧 HTTP 响应、shell 副作用被跳过 | 先 `resolve_operator`，`if op.spec().cache` 才查/写缓存；同时接线 `StepDef.cache` 字段（见 H4） |
| C3 | `engine/iterate.rs:35-36, 76` + `dsl/validator.rs:459-479` | **`batch.size=0` 未校验**：`total_items=0` 时 `0+0-1` usize 下溢（debug panic / release wrap 成 `usize::MAX`）→ `vec![Null; usize::MAX]` OOM abort；非空时每个 chunk 切空数组，数据静默丢失 | validator 拒绝 `size==0`；engine 入口 `(b.size as usize).max(1)` 或返回 BadRequest |
| C4 | `engine/step.rs:34-41` | **iterate 步骤缓存 key 不含 `over` 数组内容**：`{item}` 引用被 resolver 保留为字面字符串，`over` 不同的两次运行命中同一缓存，返回旧数据 | iterate 时将 `resolve_ref(scope, &cfg.over)` 结果并入 key：`compute_cache_key(op_type, &json!({"inputs":…,"over":…}))` |
| C5 | `tracker/tracker.rs:63, 174` | **`RunState.status` 内嵌的 Progress 是创建时的过期克隆**：`build_snapshot` 每次广播 `status: run.status.clone()`，其中 steps 永远全 Pending | 删除冗余克隆，`build_snapshot` 时用 `TaskStatus::Running(run.progress.clone())` 现构造 |
| C6 | `server/daemon.rs:505-525` | **全部端点无鉴权** + `command`/`file` 算子 → 绑 `0.0.0.0` 即未授权 RCE；localhost 下浏览器"简单请求"可 CSRF 本机 RCE（DNS rebinding 同适用） | 非回环绑定要求 `--allow-remote` + bearer token；校验 Host/Origin；`command`/`file`/`js` 加 daemon 级禁用开关 |
| C7 | `dsl/raw.rs:281` | **`iterate.over` 解析失败 `.expect()` panic**：`over: "slots.x"`（忘写花括号）、`"{}"`、`"{a..b}"` 均返回 None → daemon handler / CLI panic | 改 `TryFrom<RawIterateConfig>`，None 映射为 ParseError（带行号） |
| C8 | `dsl/storage.rs:40-43` | **TTL 反序列化溢出 panic**：chrono `TimeDelta::days/hours/...` 溢出即 panic，`snapshot_ttl: "200000000000d"` 可打崩进程 | 改用 `try_days/try_hours/...`，None 时返回 `serde::de::Error::custom` |

## High — 本周修复（P1）

| # | 位置 | 问题与原因 | 修复方法 |
|---|------|-----------|---------|
| H1 | `dsl/step.rs:50` + `engine/step.rs` 全文件 | **`step.timeout` 是死配置**：解析完整但引擎从不读取，失控 http/command/llm 步骤无限挂起整个 layer | `execute_with_retry` 中 `tokio::time::timeout(Duration::from_secs(t), op.run(...))`，超时映射 `OperatorError::Timeout`；iterate chunk future 同样包装 |
| H2 | `dsl/retry.rs:9,12` + `engine/step.rs:96,139` | **`retry.backoff`（exponential）与 `retry.validator` 是死配置**：重试间隔恒为固定 `delay_ms`，指数退避静默不生效 | `let wait = match backoff { Fixed => d, Exponential => d.saturating_mul(1<<attempt.min(20)) }` 并 clamp 上限；validator 字段实现或删除 |
| H3 | `engine/step.rs:95-148` | **`retry.max_attempts=0`**：fast path 不进、`0..0` 循环不执行 → step 一次不跑直接报 "retry exhausted" | `.max(1)` 兜底 + validator 要求 `max_attempts >= 1` |
| H4 | `dsl/step.rs:48` | **`step.cache: Option<bool>` 字段从未被读取**，用户写 `cache: false` 无法禁用缓存 | `let cache_enabled = step.cache.unwrap_or(op.spec().cache);` 包住读写缓存逻辑（与 C2 合并修） |
| H5 | `engine/step.rs:53-78` | **配置 iterate 的 step 完全绕过 retry**：`execute_iterate` 直接 `await?`，任一 chunk 失败整步失败 | chunk future 内按 `step.retry` 包一层重试；或 validator 声明 iterate+retry 互斥 |
| H6 | `cli/client.rs:14-45, 167-175` | **CLI 从不检查 HTTP 状态码**：404/400 的 `{"error":…}` 被当成功结果 pretty-print，**退出码为 0**，CI 误判 | 先读 `resp.status()`，非 2xx 返回 `Err(format!("HTTP {status}: {body}"))` |
| H7 | `server/daemon.rs:462-495` | **WS handler 从不 `socket.recv()`**：无法检测断连、Ping/Close 不处理、任务完成后 `rx.recv()` 永远 pending → 每连接泄漏一个 tokio task + 半开 TCP | `tokio::select!` 同时 poll `socket.recv()`（None/Close/Err 即退出）与 `rx.recv()`；终态后主动发 Close |
| H8 | `server/daemon.rs:448-459` | **WS get/subscribe 竞态**：先 `get` 后 `subscribe`，间隙中的广播（如极短任务的终态）永久丢失 → 客户端无限挂起 | tracker 增加 `snapshot_and_subscribe()`，同一次锁内构建快照 + `tx.subscribe()` |
| H9 | `tracker/tracker.rs:45, 120-139` | **完成的任务永不清理**：`runs` HashMap 只插不删，每个 RunState 持有完整 output Value + broadcast Sender，长跑 daemon 内存无界增长 | 终态后 TTL 清扫（后台任务 `runs.remove`）；完成态不在内存持有完整 output（已落 redb） |
| H10 | `store/mod.rs:547-557` + `server/daemon.rs:380-398` | **prune 可删除运行中的任务**：过滤条件只看"有无 snapshot"，运行超 1h 且已产出 snapshot 的任务被删 → 孤儿 snapshot、seq 从 1 重算、GET /runs/:id 404 | prune 联查 TaskTracker 跳过 Running 任务；删除前校验 tracker 状态 |
| H11 | `store/mod.rs:363-364, 577, 652-658` | **OBJECT/CACHE 表永不回收**：`ref_count` 只增不减、prune 打开 OBJECT 表却一行不删、`PruneReport` 两字段恒为 0 → 磁盘无限累积 | 实现引用计数 GC（prune 时扫描 CACHE 引用集合，删未引用 OBJECT 行并回填 report），或删字段明确"永久保留"设计 |
| H12 | `store/mod.rs:222-225, 322-325, 630-633` | **空表边界 500**：redb 表首次写入前不存在，`load_task`/`load_snapshots`/`has_snapshots` 直接 `?` 传播 → 全新 DB 的 GET /runs/:id、prune 必现 500 | 统一按 `list_*` 模式吞 `TableDoesNotExist` 返回空；或 `Database::open` 时预创建全部 5 表 |
| H13 | `server/daemon.rs:634-652, 586-604, 646` | **`daemon stop` 不校验进程身份**：PID 复用时 SIGTERM 误杀无关进程；pidfile 损坏时 `.expect("parse PID")` panic | kill 前读 `/proc/<pid>/exe`/cmdline 确认是当前二进制；parse 失败删 stale pidfile 按 not running 处理 |
| H14 | `server/daemon.rs:548-560, 613-631, 654-660` | **daemon 启动失败报假成功**：子进程 bind/open panic 但 stderr 置 null，父进程不写健康检查即打印 "started"；restart 只 sleep 200ms → 端口占用时静默死亡 | stderr 重定向日志文件；start 后轮询 `/system/operators`（带超时）再报成功；stop 轮询 `kill(pid,0)` 至 ESRCH 再返回 |
| H15 | `error.rs:112-124` | **`BadRequest`/`Parse` 落入 `_` 分支返回 500**：`/runs/not-a-uuid`、非法 YAML 提交均 500，客户端无法区分 4xx/5xx | 显式分支：`BadRequest \| Parse => StatusCode::BAD_REQUEST` |
| H16 | `operator/builtin/http.rs:24,48,52` + `file.rs:79,94-97` + `llm.rs:111-124` | **http/file(URL)/llm 算子无超时、响应体无大小限制、无 SSRF 防护**：`Client::new()` 默认无超时；`resp.text()/bytes()` 无上限（OOM）；url 可来自 `{slots.x}` 打内网/169.254.169.254 | `Client::builder().timeout().connect_timeout()`；检查 Content-Length + 流式截断（如 64MB）；解析后 IP 做私网/回环拒绝 |
| H17 | `operator/builtin/file.rs:63-74` | **file 算子路径遍历**：`path` 可来自 slot，`tokio::fs::read` 零校验 → 读 `/etc/shadow`、`~/.ssh/id_rsa`，symlink 跟随，大文件 OOM，内容落 snapshot 持久化 | 允许根目录白名单 + `canonicalize` 后前缀校验；`metadata()` 查大小上限；`is_file()` 拒绝设备 |
| H18 | `operator/builtin/command.rs:36-61` | **command 算子无超时、输出无上限、继承 daemon 全部环境变量**：子进程（受 slot 间接控制）可读 `API_KEY` 外带；`{slots.x}` 拼进 `sh -c` 无护栏 | 引擎统一超时（H1）覆盖；`take(n)` 限读字节；`.env_clear()` + 白名单传 env；文档警告 |
| H19 | `operator/builtin/js.rs:25-29` + `quickjs/runtime.rs:14,31-35` | **JS 沙箱无内存上限 + 超时不可中断**：`while(1){}` 占住 blocking 线程（上限 512）耗尽线程池；无 `set_memory_limit` → OOM | `rt.set_memory_limit(256MB)` + `set_max_stack_size`；`set_interrupt_handler` 检查截止时间由 QuickJS 内部中断 |
| H20 | `operator/builtin/llm.rs:129` + `cli/watch.rs:60, 106-108` | **字节切片非 char 边界 panic**：`&s[..500]`/`&s[..200]` 在多字节 UTF-8（中文错误体、中文输出）中间切 → panic | `s.chars().take(n).collect::<String>()` 或 `s.get(..n).unwrap_or(s)` |
| H21 | `dsl/validator.rs:347-372, 401-413` | **validator `extract_refs` 误拒合法 pipeline**：扫描任意字面字符串中的 `{a.b}` 模式 → JS 模板字符串 `` `${user.name}` ``、prompt 示例文本被报 `variable_ref_not_found`，而 parser/resolver 运行时本不做插值，三方语义不一致 | 统一语义：validator 只检整串 ref（`{"Ref":…}` 对象），嵌入模式降级 warning；或 resolver 实现嵌入插值 |

## Medium — 后续迭代（P2）

| # | 位置 | 问题与原因 | 修复方法 |
|---|------|-----------|---------|
| M1 | `engine/cache.rs:6-13` + `store/mod.rs:478` | **缓存 key 用 64 位 `DefaultHasher`**（非文档承诺的 SHA256）：生日界 ~2^32 碰撞 → 返回错误缓存；`DefaultHasher` 跨 Rust 版本不稳定，升级工具链缓存全失效 | 直接 `ObjectDigest::compute(op_type + ":" + json_bytes)` 用完整 SHA256 |
| M2 | `engine/dag.rs:184-187` | **Literal 对象/数组内嵌套的 `{"Ref":…}` 标签不被提取**：`collect_string_refs` 只处理字符串 → DAG 丢隐式依赖边，同 layer 并行时 `step not found in scope` | `Literal` 分支改 `collect_refs(lit, …)` 递归 |
| M3 | `engine/iterate.rs:131-137` | **batched 模式非数组 chunk 结果被静默丢弃**：`.flat_map(\|v\| v.as_array().cloned().unwrap_or_default())` → 数据无声消失，step 状态仍 Completed | 非数组返回错误或 warn + 计入 `IterateProgress.errors` |
| M4 | `engine/runner.rs:250-267` | **pipeline output 为 `{step.field}`（两段且非 "output"）时返回整个 step 输出**：与 `resolver.rs:105-117` 语义不一致的两份逻辑 | 条件改 `len==1 \|\| (len==2 && parts[1]=="output")`；更好是直接复用 `resolve_ref` |
| M5 | `engine/runner.rs:128, 185` | **durable（fsync）快照归属不确定**：`layers.last().and_then(last)` 源自 HashMap 迭代序，崩溃时 output 相关快照可能只在页缓存丢失 | 从 `pipeline.output` 解析目标 step id，对它 durable |
| M6 | `vm/resolver.rs:42-43` | **任意深度的 `"inputs"` 键坍缩**：字面对象含 `inputs` 键时（如 merge 的 `b: {inputs:…, other:…}`）兄弟键被静默丢弃 → 数据损坏 | 只在 op 信封顶层剥离（加 depth 参数）；或换不可冲撞哨兵键 `__weave_ref` |
| M7 | `vm/resolver.rs:114-116` | **数组下标路径静默返回 Null**：`{step.output.0.name}` 的 `0` 不索引数组 | 路径段先试 `parse::<usize>()` 命中数组；找不到时返回错误而非 Null |
| M8 | `vm/resolver.rs:91-97` | **`{env.*}` 机密落 snapshot/cache 持久化**：`{env.AWS_SECRET_ACCESS_KEY}` 可被 `task snapshot show` 读出；缺失变量静默返回 `""` | env 白名单前缀（如 `WEAVE_`）；缺失时 warn；文档提示持久化风险 |
| M9 | `dsl/validator.rs`（缺失） | **validator 不检测 DAG 环**：`A after B, B after A` 通过 check/apply，运行时才 CycleFound | validator 末尾 `Dag::from_pipeline` + `topological_sort` 转 `cycle_detected` 错误 |
| M10 | `dsl/validator.rs:273-289` | **未声明 slot 的引用不报错**：`{slots.urll}` 拼写错误运行时静默得 Null | 收集所有 `slots.*` 引用对照 `def.slots`，未声明报错或 warning |
| M11 | `dsl/validator.rs:94-99` | **inputs/iterate.over 自引用不检测**：`url: "{a.output}"`（step a 自身）运行时才 CycleFound | `check_ref_in_json`/`check_iterate_config` 中 `prefix == step_id` 报错 |
| M12 | `dsl/raw.rs:17-179` | **无 `deny_unknown_fields`**：`methd: post`、`max_worker: 4` 拼写错误静默忽略用默认值 | Raw*Inputs 加 `deny_unknown_fields`；RawStepDef 因 flatten 需手工校验剩余 key |
| M13 | `dsl/storage.rs:38-43` + `dsl/variable.rs:33-50` | **TTL 接受负值**（`"-5m"` 有数据误删风险）；**`VariablePath::parse` 误接受 `"{a.b}{c.d}"`**（inner 拆分出含 `}{` 的 part） | TTL `num < 0` 报错；part 字符集白名单 `[A-Za-z0-9_-]`，含 `{}` 拒绝 |
| M14 | `server/daemon.rs:237-250` | **后台 runner panic 后任务永远 Running**：`tokio::spawn` 无 panic 捕获，tracker 不 fail、DB 不更新、无日志 | `AssertUnwindSafe(...).catch_unwind()` 包裹，panic 时 `tracker.fail(&tid, "internal panic")` |
| M15 | `server/daemon.rs:390-391, 341-342` | **全局 DB Mutex 跨长操作持有**：prune 全表扫描持锁阻塞所有 handler；`get_snapshot_by_seq` 锁内加载全部 snapshot 再 find | redb 读写事务分离；snapshot 按 key 点查 |
| M16 | `server/daemon.rs:226-231` | **POST /runs 在信号量上阻塞响应**：并发打满时 HTTP 请求挂起无界，客户端断开不取消排队 | 立即返回 202 + queued，permit 在后台任务内获取；或 `try_acquire` 失败返回 429 |
| M17 | `server/daemon.rs:509` + `cli/client.rs:74,80` + `dsl/validator.rs:54, 72-89` | **pipeline name / step id 无字符集校验**：含 `/`、`?`、`%` 的 name 创建后 GET/DELETE 永远匹配不到（"删不掉"残留）；含空格/点号的 step id 无法被 `{...}` 引用 | validator 限制 `[A-Za-z0-9_.-]+`（step id `[A-Za-z0-9_-]+`）；CLI 拼路径用 `urlencoding::encode` |
| M18 | `cli/client.rs:16-36, 169, 206-243` + `cli/watch.rs:302-343` | **CLI 全部请求无超时无重试**；**TUI 在 WS 断开后 `try_recv` 永久空转**（state.done 永不 true）；`run_text` 断连静默 exit 0 | `Client::builder().timeout(30s).connect_timeout(5s)`；`TryRecvError::Disconnected` 时置错退出；`run_text` 未见终态收到 None 返回错误 |
| M19 | `operator/builtin/dedup.rs:36-43` | **dedup 字段缺失时整个数组塌缩成单元素**：缺失字段全序列化为 `"null"` 键被判重复，仅留第一个，无告警数据丢失 | 字段缺失跳过该元素并 warn，或返回错误 |
| M20 | `operator/builtin/base64.rs:37` | **base64 解码非 UTF-8 静默变空串**：`from_utf8(...).unwrap_or_default()` → 二进制数据变 `""`，下游无感知 | 失败返回 `OperatorError` 或 `from_utf8_lossy`；需二进制则返回 base64 包装对象 |
| M21 | `operator/builtin/sort.rs:52-60` | **sort 混合类型比较器非全序**：数字 vs 字符串一律 Equal，破坏传递性 → std/rayon 文档明确**可能 panic** | 实现类型分级全序（Null < Number < String < …），同类型内再比较 |
| M22 | `operator/builtin/merge.rs:30-37` | **merge 仅浅合并**：嵌套对象/数组被 b 整体覆盖，用户预期深合并时结果静默错误 | 文档明确浅合并语义，或加 `deep: true` 递归合并选项 |
| M23 | `operator/builtin/filter.rs:16-19` | **filter 大整数经 f64 精度丢失**：`\|n\| > 2^53` 时 `gt/lt` 结果错误，与 `eq` 的精确语义不一致 | 整数先试 `as_i64/as_u64` 精确比较，含小数才回落 f64 |
| M24 | `operator/builtin/command.rs:45-58` | **command stdin 写入与 stdout 读取管道死锁窗口**：stdin > 64KB 且子进程先输出 > 64KB 时双向阻塞（无超时兜底） | `tokio::spawn` 并发写 stdin，主任务同时 `wait_with_output` |
| M25 | `store/mod.rs:528-560, 287-295` | **prune 读-写事务间隙竞态 + seq 复用**；**`save_snapshot_with_durability` 静默吞错**：range/迭代失败算出偏小 max_seq → insert 静默覆盖已有 snapshot | 写事务内重新校验（只删 ≤ 判定时 max_seq）；range 错误 `?` 传播 |
| M26 | `store/database.rs:106-152, 86` | **redb 反序列化 `.expect()` panic + 序列化失败写空字节**：enum/schema 演进后旧数据反序列化失败 → daemon panic；`ObjectDigest::from_bytes` 短数据静默零填充 | key 引入 schema 版本号（`TypeName::new("weave::PipelineDef::v1")`）；`as_bytes` 失败 `expect`；digest 用 `try_into().expect` |
| M27 | `server/daemon.rs:556-561` + `cli/watch.rs:20-24, 335-343` | **无优雅停机**（SIGTERM 硬杀，运行中 task 永久停留 Running）；**TUI Ctrl-C 破坏终端**（raw mode + alternate screen 残留） | `tokio::signal` graceful shutdown + 启动时把 DB 中 Running 孤儿 task 标 Failed(interrupted)；TUI 处理 Ctrl-C 键事件 + panic hook 恢复终端 |

## Low — 择机清理（P3）

| # | 位置 | 问题与原因 | 修复方法 |
|---|------|-----------|---------|
| L1 | `engine/runner.rs:197,219` + `iterate.rs:159` + `engine/step.rs:44-48` | tracker 状态 `attempts: 1`、`cached: false` 硬编码失真；缓存命中不通知 tracker | `execute_step` 返回 `(Value, attempts_used, cached_hit)` 元组 |
| L2 | `engine/runner.rs:227-229` | 失败 layer 之后的 step 永久停留 Pending | layer 失败时遍历剩余 layers 置 Skipped/Cancelled |
| L3 | `engine/dag.rs:38-40` + `iterate.rs:41-45, 80-83` | 重复 step id 静默 `continue`（可能加虚假依赖边）；`max_workers=0` 死循环仅靠 validator 防线 | `Dag::from_pipeline` 返回 `DuplicateStepId` 错误；`max_workers.max(1)` |
| L4 | `engine/runner.rs:140, 177` + `iterate.rs:105, 142` + `vm/scope.rs:9-13` | 大 Value 多级深拷贝 + 每 step 全量 clone Scope HashMap（O(steps²)）；iterate 内 `sc.set_output` 写入即弃 | `set_output` 接收 `Arc<Value>`；并行期只读快照 layer 末合并 |
| L5 | `dsl/variable.rs:47-49, 68` + `dsl/retry.rs:12` | 含空白/空段的疑似模板（`{a..b}`）静默变字面量无提示；`RefValue::to_value` 与 raw.rs 内联编码不一致 | validator 对形似模板但解析失败的 Literal 报 warning；统一编码 |
| L6 | `dsl/variable.rs`（设计缺口） | 字面字符串恰好是 `{foo}` 时无转义手段，必然变 Ref | 提供转义语法（如 `\{foo}`）并文档化 |
| L7 | `main.rs:270-277` + `server/daemon.rs:564-576` | `--data-dir` 透传是死代码（clap 未声明该参数，永远到不了透传逻辑） | clap 正式加 `data_dir` 字段，删手工 args 扫描；或只保留 `WEAVE_DATA` |
| L8 | `cli/client.rs:102-111` + `main.rs:176-179` | `-i k=@broken.json` JSON 损坏静默降级为字符串；`parse_key_val` 接受空 key | `@file` 分支解析失败报错；`k.is_empty()` 返回错误 |
| L9 | `cli/client.rs:206-209` + `main.rs:182-185` | `ws://` 地址推导脆弱（`https://` 前缀出 `ws://https://…`）；`daemon_log` client build unwrap | 入口统一解析 `--daemon` 为 (scheme, host)；改 `Result` 走 `exit_on_err` |
| L10 | `tracker/tracker.rs:64, 90-152, 167` + `server/daemon.rs:492` + `main.rs:181-215` | broadcast 容量 64 Lagged 静默跳消息；`Mutex::lock().unwrap()` 中毒级联；时间戳回拨 `as u64` wrap 成天量 duration；log follow offset 截断后漂移 | Lagged 记 debug 日志 + 客户端断流时 GET 兜底；换 `parking_lot::Mutex`；`.max(0) as u64`；ring 维护单调绝对序号 |
| L11 | `store/mod.rs:386-443, 620-622` | `check_cache`/`set_cache` 死代码且 key 不含 op_type（启用即互相串缓存）；prune dry_run 不回填 `snapshots_removed` | 删除或并入 `compute_cache_key`；dry_run 同样统计 snapshot 数 |
| L12 | `quickjs/runtime.rs:49,64,78` + `operator/builtin/js.rs:21` | QuickJS 原生函数 `.expect()` panic 被 rquickjs 捕获后真实原因丢失；js 算子读 `inputs.timeout` 但 `JsInputs` 无此字段不可达 | 闭包返回 `rquickjs::Result`，`Err(new_into_js(...))` 转 JS 异常；删该读取改用 StepDef.timeout |
| L13 | `vm/resolver.rs:27-60` + `dsl/parser.rs:18` | resolver 递归无深度限制；YAML 无嵌套深度/大小限制（超深嵌套栈溢出） | 递归深度计数 + 上限报错；body 大小限制（axum DefaultBodyLimit 已有 2MB，可显式化） |
| L14 | `operator/builtin/http.rs:22,49,53` + `llm.rs` | 日志/错误信息泄露完整 URL（含 query token），落 task 错误持久化；llm 算子无 `api_key`/headers 字段（功能缺口，无法调需鉴权端点） | 日志只记 host+path，query 脱敏；`LlmInputs` 加 `api_key`（走 `{env.KEY}`）与 headers |
| L15 | `Cargo.toml:9, 22-23` | 未使用依赖 `im`、`bytes`（全仓零引用）；`tokio features=["full"]` 可裁剪 | 删两行依赖，CI 加 `cargo-machete`；tokio 按需裁剪 feature |
| L16 | `error.rs:52-92, 117-119` | redb/DagError 内部错误细节原样返回 HTTP 客户端 | 5xx 返回通用消息，细节仅记 tracing 日志 |

## 测试覆盖缺口（P1 随修复补齐）

现有 20 个集成测试全部走进程内 `Runner`，以下关键路径**零覆盖**：

| 缺口 | 能发现的 bug | 建议测试 |
|------|-------------|---------|
| 缓存行为 | C2/C4/H4/M1 | 同 inputs 跑两次：cacheable 算子第二次命中；`cache:false` 算子两次真执行；iterate 两次不同 `over` 不串缓存 |
| HTTP 错误状态码 | H15/H6 | POST 非法 YAML → 400；GET /runs/bad-uuid → 400；不存在 pipeline → 404；CLI 遇 4xx 退出码非 0 |
| retry/backoff/timeout | H1/H2/H3 | 计数 mock 算子验证重试次数与指数退避间隔；慢算子 + `timeout:1` 验证超时失败 |
| 失败注入/错误传播 | — | 中层 step 失败 → task Failed、后续层不执行、错误含 step id；iterate 单项失败行为 |
| 并发 | M14/M15/M16 | 同 layer 多 step 不饿死；超 max_concurrent_tasks 的 POST /runs 排队语义；并发运行缓存写不冲突 |
| 网络失败 | H16 | 不可达主机、非 2xx、响应超时 → step Failed 且错误可读（本地 mock server） |
| command 算子 | H18/C2 | 非零退出码 → Failed；stdout 捕获；两次运行副作用各执行一次 |
| CLI 参数解析 | L8/L9 | `parse_key_val` 边界（无 `=`、空 key、值含 `=`）；`-i k=@file` 损坏 JSON 报错 |
| daemon 生命周期 | H13/H14/M27 | start→健康检查；stop 后进程确实退出；端口占用时 start 报错而非假成功；stop 误杀防护 |
| 大数据量 | M26 | 数万元素 iterate；大 JSON 输出 snapshot 往返；大响应体截断 |
| prune/snapshot API | H10/H11/M25 | dry-run 不删除；运行中 task 保护；OBJECT 回收回填 report；snapshot list/show 顺序内容 |
| validator 边界 | C3/C7/C8/H21/M9-M13 | `batch.size=0`、`over` 无花括号、超大 TTL、JS 模板字符串、DAG 环、未声明 slot → 均为校验错误而非 panic/500 |

## 修复优先级路线图

- **P0（立即）**：C1 http body 字段名 → C7/C8 解析 panic → C3 batch.size=0 → C2/C4 缓存正确性 → C5 tracker 状态 → C6 鉴权
- **P1（本周）**：H15/H6 HTTP 状态码链路 → H7/H8 WS 生命周期 → H13/H14 daemon 进程管理 → H9/H10/H11/H12 store 泄漏与边界 → H1-H5 引擎死配置 → H16-H19 算子护栏 → H20/H21 panic 与误拒
- **P2（后续迭代）**：M1-M27 + 测试缺口补齐
- **P3（择机）**：L1-L16 清理项

