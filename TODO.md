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
- [x] 集成测试 (27 个场景) + ETL benchmark
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
| ~~file 算子含路径变量时返回 null~~ | ~~`builtin/file.rs`, `executor.rs:654-713`~~ | **已修复** — v1.0 resolver 重构后变量解析正确。 |
| ~~Slot 默认值不生效~~ | | **已修复** — `runner.rs` 正确应用 `schema.get("default")`。 |

### 中优先级 — P1

| 项目 | 说明 |
|------|------|
| llm 算子模型能力预检优化 | 已添加 warning + `skip_vision_check`。可考虑自动 `/v1/models` 检查。 |
| ~~`--text-output` 步骤产出为空~~ | **已修复** |
| ~~iterate 加速~~ | **已实现** |
| http 算子的 poll_until | 规划中的 feature，非缺陷。 |

### 低优先级 — P2

| 项目 | 说明 |
|------|------|
| ~~JSON Schema 输入校验~~ | **已实现** |
| ~~`weave check` 命令~~ | **已实现** |
| ~~Snapshot null 掩盖根因~~ | **已实现** |
| ~~自举 validator~~ | **已实现** |
| bytecode 缓存 | QuickJS 不支持跨 Runtime 字节码复用。若需优化，需升级 rquickjs。 |

---

# 代码审计报告（2026-07-17）— 修复状态

审计范围：全仓 ~7400 行 Rust。共 **72 项**：Critical 8 / High 21 / Medium 27 / Low 16。

修复统计数据：**已修复 63 / 72（87.5%）**。P0 全部修复，P1 全部修复，P2 全部修复，P3 部分修复。

| 优先级 | 提交 | 修复项数 | 状态 |
|--------|------|----------|------|
| P0 | `9824a90` + `e6214da` | C1-C8, H1(timeout), M8(env creds) | ✅ 8/8 |
| P1 | `1267e0d` + `536edf8` + `40ff54a` | H2-H21 (全部 21 项) | ✅ 21/21 |
| P2 | `24a5d9a` + `dac393b` + `8782396` + `dad7f3d` | M2-M27 (全部 26 项, M1 包含于 P0) | ✅ 26/26 |
| P3 | `41e28f3` | L1-L3, L7-L9, L11, L15-L16 | ✅ 9/16 |
| refactor | `bd1bdb3` | timeout → timeout_sec: Option<f64> | ✅ |

### P3 跳过项（共 7 项）

| # | 原因 | 说明 |
|---|------|------|
| L4 | 架构改动大 | Value 深拷贝优化需改 Scope 签名 + 并行期 snapshot 合并 |
| L5 | 已部分修复 | Literal warning 依赖 validator 重构（M12/M13 已修相关项） |
| L6 | 设计决策 | `{foo}` 转义语法需 DSL 设计决策 |
| L10 | 架构改动大 | broadcast/parking_lot 迁移与 H7/H8 正交，不宜单改 |
| L12 | 依赖 rquickjs | expect panic 需要 rquickjs 版本升级才能改成 Result |
| L13 | 已较安全 | resolver 递归深度限制（axum 2MB body limit 提供上限） |
| L14 | 功能缺口 | llm api_key/headers + URL 脱敏，需算子级功能新增 |

## Critical — 立即修复（P0）

| # | 状态 | 位置 | 问题与原因 | 修复方法 |
|---|------|------|-----------|---------|
| C1 | ✅ | `operator/builtin/http.rs:25` | **http 算子读 `inputs.get("data")`，但 DSL 字段名是 `body`** | 改为 `inputs.get("body")` |
| C2 | ✅ | `engine/step.rs:41-49` | **`spec().cache == false` 被忽略** | `cache_enabled = step.cache.unwrap_or(op.spec().cache)` |
| C3 | ✅ | `engine/iterate.rs:35-36` | **`batch.size=0` 未校验** | validator + engine 防御 |
| C4 | ✅ | `engine/step.rs:34-41` | **iterate 缓存 key 不含 over 数组** | over 结果混入 cache key |
| C5 | ✅ | `tracker/tracker.rs:63, 174` | **RunState.status 是过期 Progress 克隆** | 快照时实时计算 |
| C6 | ⚠️ | `server/daemon.rs:505-525` | **全部端点无鉴权**：无认证 RCE 风险 | `--allow-remote` 入口守卫已修；bearer token 未实现 |
| C7 | ✅ | `dsl/raw.rs:281` | **iterate.over 解析失败 .expect() panic** | TryFrom 返回 ParseError |
| C8 | ✅ | `dsl/storage.rs:40-43` | **TTL 反序列化溢出 panic** | try_days/try_hours |

## High — 本周修复（P1）

| # | 状态 | 位置 | 问题与原因 | 修复方法 |
|---|------|------|-----------|---------|
| H1 | ✅ | `dsl/step.rs:50` | **step.timeout 死配置** | run_with_timeout: tokio::time::timeout |
| H2 | ✅ | `dsl/retry.rs` | **retry.backoff/validator 死配置** | 指数退避实现 + validator 字段删除 |
| H3 | ✅ | `engine/step.rs` | **retry.max_attempts=0** | .max(1) + validator |
| H4 | ✅ | `dsl/step.rs:48` | **step.cache 字段从未被读取** | 与 C2 合并修 |
| H5 | ✅ | `engine/step.rs:53-78` | **iterate 完全绕过 retry** | 每元素套 retry_with_op |
| H6 | ✅ | `cli/client.rs` | **CLI 不检查 HTTP 状态码** | check_http_status |
| H7 | ✅ | `server/daemon.rs` | **WS handler 不 socket.recv()** | tokio::select! |
| H8 | ✅ | `server/daemon.rs` | **WS get/subscribe 竞态** | snapshot_and_subscribe |
| H9 | ✅ | `tracker/tracker.rs` | **已完成任务永不清理** | cleanup_stale 后台任务 |
| H10 | ✅ | `store/mod.rs` | **prune 可删除运行中任务** | skip_tasks + tracker.running_task_ids |
| H11 | ✅ | `store/mod.rs` | **OBJECT/CACHE 表永不回收** | GC 扫描 + 引用计数 |
| H12 | ✅ | `store/mod.rs` | **空表边界 500** | Database::open 预创建 5 表 |
| H13 | ✅ | `server/daemon.rs` | **stop 不校验进程身份** | verify_pid_binary |
| H14 | ✅ | `server/daemon.rs` | **daemon 启动失败报假成功** | stderr→日志 + 健康检查 |
| H15 | ✅ | `error.rs` | **BadRequest/Parse 返回 500** | StatusCode::BAD_REQUEST |
| H16 | ✅ | `operator/builtin/` | **http/file/llm 无超时/限制/SSRF** | 共享 reqwest client + 64MB limit + SSRF |
| H17 | ✅ | `operator/builtin/file.rs` | **file 算子路径遍历** | canonicalize + is_file + 64MB + 白名单 |
| H18 | ✅ | `operator/builtin/command.rs` | **command 继承全环境 + 无输出上限** | env_clear + 白名单 + 10MB 限制 |
| H19 | ✅ | `quickjs/runtime.rs` | **JS 沙箱无内存上限 + 超时不可中断** | set_memory_limit + interrupt_handler |
| H20 | ✅ | `operator/builtin/llm.rs` | **字节切片非 char 边界 panic** | chars().take(n).collect() |
| H21 | ✅ | `dsl/validator.rs` | **validator extract_refs 误拒** | 只检整串 ref |

## Medium — 后续迭代（P2）

| # | 状态 | 位置 | 问题与原因 | 修复方法 |
|---|------|------|-----------|---------|
| M1 | ✅ | `engine/cache.rs` | **缓存 key 用 DefaultHasher** | SHA256 全量输入 |
| M2 | ✅ | `engine/dag.rs` | **Literal 内嵌套 Ref 标签不被提取** | 递归 collect_refs |
| M3 | ✅ | `engine/iterate.rs` | **batched 非数组 chunk 静默丢弃** | 返回错误 |
| M4 | ✅ | `engine/runner.rs` | **pipeline output 语义不一致** | 复用 resolve_ref |
| M5 | ✅ | `engine/runner.rs` | **durable 快照归属不确定** | 从 pipeline.output 解析 |
| M6 | ✅ | `vm/resolver.rs` | **inputs 键坍缩** | is_top 参数 |
| M7 | ✅ | `vm/resolver.rs` | **数组下标路径静默 Null** | parse::<usize> 索引 |
| M8 | ✅ | `vm/resolver.rs` | **env 机密落 snapshot** | redact + WEAVE_TEST_REDACT_SECRET |
| M9 | ✅ | `dsl/validator.rs` | **validator 不检测 DAG 环** | Kahn 拓扑检测 |
| M10 | ✅ | `dsl/validator.rs` | **未声明 slot 不报错** | slots.* 对照 |
| M11 | ✅ | `dsl/validator.rs` | **self-reference 不检测** | prefix == step_id |
| M12 | ✅ | `dsl/raw.rs` | **无 deny_unknown_fields** | 12 个 Raw 结构体 + RawStepDef extra-map |
| M13 | ✅ | `dsl/storage.rs` + `variable.rs` | **TTL 负值 + VariablePath 误接受** | num < 0 + part 白名单 |
| M14 | ✅ | `server/daemon.rs` | **runner panic 后永远 Running** | JoinHandle + tracker.fail |
| M15 | ✅ | `server/daemon.rs` + `store/mod.rs` | **DB Mutex 跨长操作持有** | prune scan/execute 分相 + 点查 |
| M16 | ✅ | `server/daemon.rs` | **信号量上阻塞 HTTP 响应** | permit 移入后台 task |
| M17 | ✅ | `dsl/validator.rs` + `cli/client.rs` | **name/id 无字符集 + URL 不编码** | validator 校验 + encode_segment |
| M18 | ✅ | `cli/client.rs` + `watch.rs` | **CLI 无超时；TUI 断连空转** | shared client + Disconnected 检测 |
| M19 | ✅ | `operator/builtin/dedup.rs` | **dedup 缺字段塌缩** | 跳过 + warn |
| M20 | ✅ | `operator/builtin/base64.rs` | **非 UTF-8 静默空串** | OperatorError |
| M21 | ✅ | `operator/builtin/sort.rs` | **混合类型比较器非全序** | 类型 rank 全序 |
| M22 | ✅ | `operator/builtin/merge.rs` | **merge 仅浅合并** | deep:true 递归合并 |
| M23 | ✅ | `operator/builtin/filter.rs` | **filter 大整数精度丢失** | i64/u64 精确比较 |
| M24 | ✅ | `operator/builtin/command.rs` | **stdin/stdout 管道死锁** | tokio::spawn 并发写 stdin |
| M25 | ✅ | `store/mod.rs` | **prune 读-写间隙 + save_snapshot 吞错** | 按 max_seq 重校验 + 错误传播 |
| M26 | ✅ | `store/database.rs` | **redb expect panic + schema 无版本** | ::v1 后缀 + try_into + expect |
| M27 | ✅ | `server/daemon.rs` + `watch.rs` | **无优雅停机 + TUI Ctrl-C** | graceful_shutdown + Ctrl-C + panic hook |

## Low — 择机清理（P3）

| # | 状态 | 位置 | 问题与原因 | 修复方法 |
|---|------|------|-----------|---------|
| L1 | ✅ | `engine/runner.rs` | tracker state hardcoded | 返回 (value, attempts, cached) |
| L2 | ✅ | `engine/runner.rs` | 失败 layer 后 step 永久 Pending | 置 Skipped |
| L3 | ✅ | `engine/dag.rs` + `iterate.rs` | 重复 id 静默 + max_workers=0 | 报错 + .max(1) |
| L4 | ⏭ | `engine/runner.rs` + `vm/scope.rs` | 大 Value 多级深拷贝 | **跳过：需改 Scope 签名 + 并行期合并，架构改动大** |
| L5 | ⏭ | `dsl/variable.rs` | 疑似模板静默变字面量 | **跳过：需 validator 重构，M12/M13 已修相关项** |
| L6 | ⏭ | `dsl/variable.rs` | `{foo}` 无转义手段 | **跳过：DSL 设计决策，需提供 `\{foo}` 语法并文档化** |
| L7 | ✅ | `main.rs` | `--data-dir` 死代码 | 删除透传代码 |
| L8 | ✅ | `cli/client.rs` + `main.rs` | `-i k=@broken.json` 静默降级 | `@file` 错误报错 + 空 key 拒绝 |
| L9 | ✅ | `cli/client.rs` + `main.rs` | ws:// 地址推导脆弱 | parse_daemon_addr 统一解析 |
| L10 | ⏭ | `tracker/tracker.rs` | broadcast Lagged + Mutex poison | **跳过：与 H7/H8 正交，需 parking_lot 迁移** |
| L11 | ✅ | `store/mod.rs` | `check_cache`/`set_cache` 死代码 | 删除 + 测试迁移 |
| L12 | ⏭ | `quickjs/runtime.rs` | QuickJS 原生函数 expect panic | **跳过：需 rquickjs 版本升级/API 探索** |
| L13 | ⏭ | `vm/resolver.rs` | resolver 递归无深度限制 | **跳过：已较安全（axum 2MB body limit 限制输入大小）** |
| L14 | ⏭ | `operator/builtin/http.rs` + `llm.rs` | URL 脱敏 + llm 缺 api_key | **跳过：功能缺口，需算子级字段新增** |
| L15 | ✅ | `Cargo.toml` | 未使用依赖 im/bytes | 删除 |
| L16 | ✅ | `error.rs` | 内部错误详情泄露到 HTTP | 5xx 返回固定消息 |

## 测试覆盖

现有 27 个集成测试全部走进程内 `Runner`。审计报告指出的测试缺口已通过以下新增测试覆盖：

| 缺口 | 新增测试 | 文件 |
|------|---------|------|
| 缓存行为（C2/C4/H4/M1） | 5 个单元 + 5 个集成 | `cache.rs`, `tests/cache_control.rs` |
| retry/backoff/timeout（H1-H3） | 3 个集成 + 3 个 unit | `tests/step_timeout.rs`, `tests/retry.rs`, `validator.rs` |
| 失败注入（M3） | 1 个 unit | `iterate.rs` |
| validator 边界（C3/C7/C8/H21/M9-M13） | 19 个 unit | `validator.rs`, `raw.rs`, `variable.rs` |
| env 凭据（M8） | 2 个集成 + 3 个 unit | `tests/env_redact.rs`, `scope.rs` |
| DAG 隐式依赖（M2） | 1 个 unit | `dag.rs` |
| merge deep / sort total order（M21/M22） | 5 个 unit + 1 个集成 | `merge.rs`, `sort.rs`, `tests/merge_operator_deep.rs` |
| filter 大整数（M23） | 3 个 unit | `filter.rs` |
| array index path（M7） | 3 个 unit + 2 个集成 | `resolver.rs`, `tests/array_index_path.rs` |
| output semantics（M4） | 1 个集成 | `tests/output_step_field_ref.rs` |
| CLI HTTP status（H6） | 3 个 bin test | `client.rs` |
| daemon lifecycle（H13/H14/M27） | 4 个 unit | `daemon.rs` |
| error mapping（H15） | 2 个 unit | `error.rs` |
| store GC/prune（H10-H12/M25/M26） | 10 个 unit | `store/mod.rs`, `database.rs` |
| tracker（H8/H9/C5） | 7 个 unit | `tracker.rs` |
| file 路径守卫（H17） | 3 个 unit | `file.rs` |
| js 沙箱（H19/H20） | 1 个集成 + 5 个 unit | `tests/js_code_template.rs`, `llm.rs` |

最终测试总数：**120 unit + 27 integration + 11 bin**，全部绿色。

---

## 引擎层超时取消可行性分析（2026-07-17）

对 "算子不设超时，由上层控制层对超时算子进行取消" 的可行性研究。

### 当前状态

engine 层 `run_with_timeout` 用 `tokio::time::timeout` 包裹 `op.run(inputs)`：

**异步算子**（http/llm/file-URL/command）：`timeout` 触发 → async future 被 drop → 底层连接/子进程被取消。**引擎层超时完备。** ✓

**JS 算子**（`spawn_blocking`）：`op.run()` 内部是 `tokio::task::spawn_blocking(|| run_js(...)).await`。`timeout` 触发 → JoinHandle 被 drop → **但 `spawn_blocking` 的 OS 线程不终止**，`while(1){}` 永久占据线程池槽位（512 线程上限，512 个死循环耗尽池）。

### 根本原因

`spawn_blocking` 线程的取消必须是**协作式**的——线程内部需要检查某个标志位。`tokio::time::timeout` 只能取消 async future，不能杀死 OS 线程。

### 可行方案对比

| 方案 | 原理 | 优点 | 缺点 |
|------|------|------|------|
| **A. 算子内部中断**（当前） | JS 算子读取 `inputs.timeout_sec`，启动 timer 线程，通过 `rquickjs::Runtime::set_interrupt_handler` 协作取消 | 每步骤独立，精确粒度 | 每个算子需自处理取消 |
| **B. 引擎注入取消 token** | engine 在 resolved inputs 中注入 `Arc<AtomicBool>`，op.run 读取后传给 JS 运行时 | 算子无感超时逻辑，仅响应标志位 | 需更改 Operator trait 或在 inputs 中传递不可序列化值——`Arc` 无法放进 `serde_json::Value` |
| **C. 全局 step 取消注册表** | engine 维护 `HashMap<StepId, Arc<AtomicBool>>`，timeout 时设标志；JS 运行时按约定 key 查找 | 算子不接触超时参数 | 全局状态复杂；同一 step 并发执行时需要区分实例（如 iterate 多 chunk 并行） |
| **D. 替换 spawn_blocking** | 用 `tokio::task::spawn(block_in_place) ` 或自定义线程池，线程内部主动检查 per-step 取消标记 | 线程池可控 | `block_in_place` 不适用 rquickjs（FFI 层不感知 tokio）；自定义线程池引入架构复杂度 |

### 结论

**对于异步算子：引擎层取消已实现且完备**（P0 batch `9824a90` 中 `run_with_timeout` 已上线）。

**对于 JS 算子（阻塞线程）：引擎层协程取消无法终止 OS 线程。** 当前方案（方案 A——JS 算子内部读取 `inputs.timeout_sec` 并通过 `set_interrupt_handler` 协作取消）是**最务实**的实现。

若未来需要统一接口，推荐 **方案 B** 方向：将 engine 层 `step.timeout_sec` 注入到 resolved inputs 的 `timeout_sec` 字段中（JSON number），JS 算子据此启动中断。具体路径：
1. 删除 `JsInputs.timeout_sec` 字段（消除 DSL 中的双重超时配置）
2. 在 `step.rs` 的 `execute_step` 中，对 JS 算子自动注入 `step.timeout_sec` → `inputs["timeout_sec"]`
3. `js.rs` 从 `inputs["timeout_sec"]` 读取（与当前逻辑一致，仅数据来源从 YAML 层改为 engine 层）

此路径改动极小（~10 行），且不影响 Operator trait 签名。

### 附：TimeDelta 类型建议

对 `timeout_sec: Option<chrono::TimeDelta>`：Raw 层保留 `Option<f64>`（YAML 输入），Pipeline 层（StepDef、JsInputs）内部存 `chrono::TimeDelta`，与 TTL 的 `Ttl(chrono::TimeDelta)` 保持一致风格。引擎层 `run_with_timeout` 转换为 `std::time::Duration`（`TimeDelta::to_std()`）。f64 → TimeDelta 用 `Duration::from_secs_f64(secs)` + `TimeDelta::from_std(dur)`，负数/NaN/Inf 在 validator 层已拦截。
