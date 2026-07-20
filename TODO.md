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

> **2026-07-18 更新**：在 63/72 之上完成"复审修复 + 全仓二次审计修复"（三批改动，详见文末
> 「2026-07-18 复审 + 二次审计修复记录」）。本轮新修复 **40+ 项**（含 1 项复审回归），
> C6 鉴权与 L4/L5/L6/L10/L12/L13/L14 维持开放。本轮修复由模型 **kimi-for-coding/k3** 执行。

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

> 2026-07-18 复审 + 二次审计后：**169 lib tests + 28 个集成测试二进制 + 24 bin tests**（见下文测试增量）。

---

# 2026-07-18 复审 + 二次审计修复记录

对 63/72 修复后代码的一轮**复审**（发现 1 项回归 + 若干实现瑕疵）与一轮**全仓二次审计**，
共三批改动（复审批、validator/算子批、store/daemon/cli 批），全部已修复。本轮由模型 **kimi-for-coding/k3** 执行。

## 复审问题修复（批次一）

| 位置 | 问题 | 修法 | 状态 |
|------|------|------|------|
| `engine/iterate.rs:15` | **iterate 默认并发塌缩回归**：未设 `max_workers` 时退化为 1 | 新增 `effective_max_workers()`：显式值 `.max(1)`；缺省 = `available_parallelism()`（fallback 4） | ✅ |
| `dsl/step_op.rs` + `dsl/raw.rs` | JS 算子 inputs 自带 `timeout` 字段，与 step 层 `timeout_sec` 双配置 | `JsInputs`/`RawJsInputs` 删除 timeout 字段；YAML 写 `timeout` 触发 deny_unknown_fields 报错 | ✅ |
| `quickjs/runtime.rs` | step 层 timeout 只 drop future，`spawn_blocking` 线程内 `while(1){}` 仍占槽 | `InterruptGuard` drop-guard：`run_js` future 被 drop 时置位 `AtomicBool`，QuickJS `set_interrupt_handler` 协作中断，真取消 | ✅ |
| `engine/step.rs:159` + `dsl/validator.rs:145` | step timeout 语义不完备 | `Duration::try_from_secs_f64`（NaN/Inf/溢出 → Config 错误）；validator 上限 365 天；超时 `warn!` 并映射 `OperatorError::Timeout`（按 retry 配置重试） | ✅ |
| `engine/step.rs` | 失败路径 attempts 固定为 1，与事实不符 | retry 循环错误携带真实已尝试次数 `(err, attempt+1)` | ✅ |
| `engine/step.rs:53` | 缓存命中 attempts 语义 | 缓存命中返回 `attempts = 0`、`cached = true` | ✅ |
| `engine/step.rs:74,149` | 缓存写失败直接让 step 失败 | 降级为 `warn!`，继续执行不缓存 | ✅ |
| `engine/runner.rs` + `iterate.rs` | iterate 步骤 Completed 状态双写 | `execute_iterate` 不再自行置 Completed，收口到 runner 统一设置（唯一出口） | ✅ |
| `engine/dag.rs:208` | 字符串值内嵌 `{step.output}` 产生幽灵依赖 | `collect_string_refs` 只认整串 `"{...}"`（`VariablePath::parse`），内嵌永不构成依赖，与 parser/resolver/validator 对齐 | ✅ |
| `vm/resolver.rs` | 缺字段静默 Null、数组索引语义摇摆 | 对象缺字段/非标量取段 → `warn!` + Null；数组段非数字/越界 → 硬错误（`resolve_nested` 同步支持数组下标） | ✅ |
| `vm/resolver.rs:43` | noop（无 inputs）输出被 `{"type":"noop"}` 污染 | 顶层 op 信封：无 `inputs` 键时移除 `type` 后以剩余 map 为 inputs；iterate 注入的 `data` 得以存活 | ✅ |
| `quickjs/runtime.rs:28` | JS 异常/正常值共用一个 JSON 通道易混淆 | `__weave_ok__` 信封：`{__weave_ok__: true, value}` / `{__weave_ok__: false, message, stack}` | ✅ |
| `tracker/*` | tracker 状态清理 | `TaskStatus::Pending` 变体删除；`IterateProgress.errors/skip` 字段删除；`StepProgress.timeout_sec` 自 step 配置接线；`TaskTracker::subscribe` 公开方法删除（统一 `snapshot_and_subscribe`）；负时长一律 `.max(0)` | ✅ |
| `vm/scope.rs:34` | env_values Mutex poison 后 panic | `unwrap_or_else(into_inner)` 恢复 | ✅ |

## 二次审计新发现修复（批次二：validator / DSL / 算子）

| 位置 | 问题 | 修法 | 状态 |
|------|------|------|------|
| `dsl/validator.rs:695` | `check_js_syntax` 真实 eval 用户代码，无资源限制 | 沙箱化：64MB `set_memory_limit` + 2s watchdog 线程置 interrupt；仅 `SyntaxError` 判为错误，运行时异常/中断放行 | ✅ |
| `dsl/storage.rs:37` | TTL 单位字符非 ASCII 时 `split_at` 越界 panic | 按 `len_utf8()` 切分，多字节单位返回错误 | ✅ |
| `dsl/validator.rs:509` + `dag.rs:179` + `resolver.rs:30` | 含 `"Ref"` 键的多键对象被误当 ref 标签 | 三方统一守卫 `map.len() == 1`；多键/不可解析按普通对象递归 | ✅ |
| `dsl/validator.rs:522` | slot 收集把内嵌 `{slots.x}` 字符串误报为引用 | `whole_string_ref` 整串守卫（内嵌保持字面量） | ✅ |
| `dsl/raw.rs` 等 | `deny_unknown_fields` 覆盖不全 | 补齐 `RawPipelineDef`/`RetryDef`/`BatchConfig`/`SlotDef`/`StorageDef` 等全部 Raw 结构 | ✅ |
| `builtin/filter.rs` + `sort.rs` | `operator`/`order` 任意字符串运行时静默 | 枚举白名单（8 个 filter operator、asc/desc）：validator 报 `invalid_operator_config` + 运行时 Config 错误兜底 | ✅ |
| `builtin/filter/sort/dedup/base64` | 缺 `data` 字段塌缩成 Null 继续跑 | 缺失或为 null → `OperatorError::Config` | ✅ |
| `builtin/mod.rs:18` | `resolve_nested` 不支持数组下标 | 数字路径段按索引取 Array 元素（与 resolver 语义一致） | ✅ |
| `builtin/command.rs:54` | 子进程泄漏 + 输出超限挂起 | `kill_on_drop(true)`；stdout/stderr 超 10MB 后继续 drain 丢弃（子进程不阻塞），输出带 `truncated: bool` | ✅ |
| `builtin/http_client.rs` | SSRF 预检可绕过 | 全 DNS 地址逐一检查（非只查首个）；`redirect::Policy::none()` 禁 302 跳板；`read_body_limited` 流式 64MB 限长；`reqwest::Url` 解析防 userinfo 伪造 host | ✅ |
| `builtin/file.rs:52` | 白名单解析毛刺 | `WEAVE_FILE_ALLOW_ROOTS` 空段过滤 + 计数 warn；过滤后为空 → 拒绝所有路径；未配置 → `Once` 单次 warn 后放行 | ✅ |
| `operator/registry.rs` | 死代码（HashMap 注册表未被使用） | 删除文件，`get_builtin` 直接 match | ✅ |
| `dsl/validator.rs`、`builtin/llm.rs`、`builtin/command.rs`、`error.rs` | 死代码 | 删 `ValidateOptions`、llm `image_base64` 别名、command `cmd` 别名、`From<serde_json::Error>` | ✅ |
| `dsl/parser.rs:29` | raw 层错误被包装丢失上下文 | `ParseError::Raw(#[from] raw::ParseError)` 变体透传 | ✅ |
| `dsl/validator.rs:395-414` | slot 名/步骤 id/pipeline 名字符集 | 白名单 `[A-Za-z0-9_.-]`（pipeline 可含 `.`）/`[A-Za-z0-9_-]`（slot、step id） | ✅ |
| `dsl/validator.rs:62` | pipeline 名全为 `.` 时 URL 归一化后不可达 | 报 `invalid_pipeline_name` | ✅ |
| `dsl/validator.rs:652` | `iterate.max_workers` 无上限 | ≤ 1024 | ✅ |
| `builtin/sort.rs` + `filter.rs` | sort/filter 整数比较语义不一致 | 共用 `compare_json_numbers`（i64/u64 精确 cmp，混合回落 f64） | ✅ |
| `builtin/dedup.rs:59` | 大量缺字段元素刷 warn | warn 聚合为单条 | ✅ |
| `dsl/validator.rs:647,669` | 文案 | "省缺" → "缺省" | ✅ |

## 二次审计新发现修复（批次三：store / daemon / cli）

| 位置 | 问题 | 修法 | 状态 |
|------|------|------|------|
| `server/daemon.rs:704` | 首启数据目录不存在时启动失败 | `create_dir_all` | ✅ |
| `dsl/storage.rs` + `daemon.rs:329` | `storage.result_ttl` 是死配置 | 接线：`result_ttl_secs()`（缺省 3600s，下限 60s）写入 TaskMeta；`snapshot_ttl` 字段删除（写了报 unknown field） | ✅ |
| `store/mod.rs:54` | v0 数据库（无 ::vN 后缀 schema）打不开 | 自动迁移：旧文件备份 `.v0.bak`，PIPELINE/TASK 逐条拷贝（剥离 `snapshot_ttl`），SNAPSHOT/OBJECT/CACHE 丢弃 | ✅ |
| `store/mod.rs:24` | 全局 `Mutex<Database>` 串行化所有 DB 访问 | 移除外层 Mutex；内部 `std::sync::RwLock<redb::Database>`，仅 `compact()` 取写锁 | ✅ |
| `server/daemon.rs:780` | 无优雅停机 drain | 停机序列：`draining=true` → `/runs` 返回 503 → `wait_for_drain` 最多 30s（`SHUTDOWN_DRAIN_SECS`）→ 超时强制退出 | ✅ |
| `server/daemon.rs:950` | pidfile 竞态 | spawn 成功后立即写 pidfile；失败分支仅当 pidfile 内容是本次 PID 才删除；健康检查失败杀子进程；`is_daemon_running` 校验 `/proc/<pid>/exe` 二进制身份 | ✅ |
| `server/logging.rs` | 日志 offset 相对值，trim 后错乱 | 绝对 offset（自启动起总字节数）；`X-Log-Offset` / `X-Log-Truncated` 响应头 | ✅ |
| `store/database.rs:179` | `count_snapshots`/`list_snapshot_keys` 全量反序列化 | `SnapshotHeader` header-only 视图（同表同类型名），跳过 output 拷贝 | ✅ |
| `store/database.rs:131` | Snapshot serde_json 序列化 Vec<u8> 4 倍膨胀 | 自定义二进制布局 v2：`seq(8B BE) | step_id_len(4B BE) | step_id | output`，类型名 `weave::Snapshot::v2` | ✅ |
| `store/object.rs` | `ObjectValue.ref_count` 字段从不维护 | 删除 | ✅ |
| `cli/client.rs:35` | daemon URL 尾随 `/` 产生双斜杠 | `trim_end_matches('/')` | ✅ |
| `cli/client.rs:210` | prune 全表扫描 + compact 超默认 30s 超时 | prune 请求单独放宽到 300s | ✅ |
| `cli/client.rs:276` | WS connect 无超时 | 10s connect 超时 | ✅ |
| `cli/client.rs:315` | 非 TTY 环境 TUI 无法渲染 | stdout 非 terminal 时自动回落 `--text-output` | ✅ |
| `main.rs:66` | `--watch` 与 `--text-output` 可同时给 | clap `conflicts_with` 互斥 | ✅ |
| `main.rs:193` | daemon log 不支持 https/输出不 flush/出错 0 退出 | `parse_daemon_addr` 统一 scheme；`stdout().flush()`；`exit_on_err` 非零退出 | ✅ |
| `cli/watch.rs:247` | `" (parallel)"` 文案 | 修复拼接 | ✅ |
| `server/daemon.rs:85` | PruneResponse 缺 snapshots 计数 | 新增 `snapshots_removed` 字段 | ✅ |
| `server/daemon.rs` | `weave serve` 参数透传脆弱 | 严格解析（白名单重组 argv） | ✅ |
| `build.rs` | 死代码 | 删除 | ✅ |
| `cli/client.rs`、`store/mod.rs`、`server/daemon.rs` | 边界缺防护测试 | 新增 `encode_segment`、`mark_interrupted_tasks`、prune max_seq 防护（scan 后新 snapshot 不误删）、`wait_for_drain` 等测试 | ✅ |

## 仍开放问题（本轮确认）

| # | 问题 | 状态说明 |
|---|------|---------|
| C6 | bearer token 鉴权未实现 | 仅 `--allow-remote` 入口守卫；daemon 视为 localhost-only |
| L4 | Value 深拷贝优化 | 维持跳过（需改 Scope 签名，架构改动大） |
| L5 | 疑似模板静默变字面量 warning | 维持跳过（validator 重构） |
| L6 | `{foo}` 无转义语法 | 维持跳过（DSL 设计决策） |
| L10 | broadcast Lagged + parking_lot 迁移 | 维持跳过（与 H7/H8 正交） |
| L12 | QuickJS 原生函数 expect panic | 维持跳过（依赖 rquickjs 升级） |
| L13 | resolver 递归无深度限制 | 维持跳过（axum 2MB body limit 兜底） |
| L14 | llm api_key/headers + URL 脱敏 | 维持跳过（功能缺口） |
| — | `find_pipeline_by_name` 全表扫描 | **有意保留**：pipeline 数量小，不加索引 |
| — | JS 无 `timeout_sec` 时死循环仍占 blocking 线程 | **设计决策**：超时只在 step 层，JS 算子不再自带超时 |
| — | `yes` 类无限输出命令 | 跑到 step 超时为止（10MB 截断只保护内存，不终止进程；`kill_on_drop` 保证 step 超时取消时清理） |
| — | `check_js_syntax` 仍会短暂执行用户代码 | 已沙箱（64MB 内存 + 2s 中断 + 无 fs/net），属可接受风险 |

## 本轮测试增量

v0 数据库迁移、Snapshot v2 二进制布局 roundtrip、`encode_segment`、`mark_interrupted_tasks`、
prune max_seq 防护、js 语法沙箱（SyntaxError 拒绝 / ReferenceError 放行 / `while(1){}` watchdog 终止）、
`effective_max_workers` 三态、command 10MB 截断、http_client split_url/SSRF、file 白名单空段、
noop 输出信封（`tests/noop_output.rs`）、`wait_for_drain` 三态、pidfile 二进制校验、
TTL 多字节单位、日志绝对 offset、`snapshot_and_subscribe` 原子性等。

复审 + 二次审计后总数：**169 lib tests + 28 个集成测试二进制（51 个集成测试函数）+ 24 bin tests**，`cargo clippy --all-targets` 0 警告。

---

## 引擎层超时取消可行性分析（2026-07-17）

> **已被 2026-07-18 复审批超越**：JS 算子 inputs 的 `timeout` 字段已删除，方案 A 不再适用。
> 最终实现为"方案 B 的零成本变体"——step 层 `tokio::time::timeout` drop `run_js` future 时，
> `InterruptGuard` drop-guard 置位 `AtomicBool` 触发 QuickJS `set_interrupt_handler` 协作中断
> （`quickjs/runtime.rs`）。无需改 Operator trait，也无需向 inputs 注入不可序列化值。
> 以下分析保留作历史参考。

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

## 三次审计修复记录（2026-07-20）

四轮并行只读 subagent 审计（engine/vm/tracker、dsl、store/server、operator/quickjs/cli），
去重后 30 项，修复 24 项，余 6 项记入开放清单。分三批提交。

### 批次 A — DSL / resolver / DAG 一致性

| # | 级别 | 位置 | 问题 | 修法 |
|---|------|------|------|------|
| D1 | H | vm/resolver.rs | 单键 `{"Ref": <不可解析>}` 用户数据运行时硬错误，validator 却放行 | 解析失败回退为普通对象递归（三方对齐） |
| D2 | M | vm/resolver.rs | Literal 负载内单键 `{"Literal": X}` 用户数据被静默拆包 | Literal 标签只在字段位置识别，负载内不递归识别 |
| D3 | H | validator/dag | 裸 String 字段整串 `"{...}"`：validator/DAG 当 ref，resolver 当字面量（cycle 假阳性/误拒/静默错值） | validator/DAG 移除裸字符串 ref 识别，三方一致按字面量 |
| D4 | M | validator | iterate.as 无校验，`as: slots` 等静默劫持 ref 解析 | 拒绝保留前缀/步骤 id 冲突/非法字符 |
| D5 | M | validator | `{as_name.x}` 被误拒，与 resolver 透传语义相反 | as_name 前缀跳过存在性检查 |
| D6 | M | validator | retry 无上限（u32::MAX 次 × u64::MAX ms） | max_attempts ≤ 100，delay_ms ≤ 1h |
| D7 | L | validator | llm.temperature NaN/Inf 触发 to_value 失败被 unwrap_or(Null) 静默吞 | 有限数值校验 |
| D8 | L | validator | base64.mode 无白名单（filter/sort 已有） | 补 mode + http.method 白名单 |
| E1 | M | resolver | slots 路径不支持数组下标，静默 Null（step 路径是硬错误） | slots 与 step 分支对齐：数组严格、对象缺失 Null |
| E4 | L | resolver | slot 解析失败 warn 把整层 slot 值写日志（大/敏感） | 日志只留路径与缺失段 |
| E5 | L | validator | collect_slots_used 全路径比较，unused_slot 误报 | 按首段匹配 |

### 批次 B — daemon / store

| # | 级别 | 位置 | 问题 | 修法 |
|---|------|------|------|------|
| S3 | M | daemon.rs | draining 检查 → in_flight+1 之间 TOCTOU，停机窗口任务被中止 | 先占计数再复查 draining |
| E2/S6 | M | daemon.rs | runner panic 跳过 in_flight 回收，drain 永远等满 30s | 回收移至外层 watcher 无条件执行 |
| E3 | L | daemon.rs | panic 路径步骤永远 Running | tracker.fail_non_terminal_steps 收口 |
| S1 | M | daemon.rs | stop() 10s SIGKILL < 30s drain，优雅停机被架空 | 超时改为 SHUTDOWN_DRAIN_SECS+5 |
| S2 | M | daemon.rs | 健康检查不校验子进程存活，可误报启动成功（孤儿 daemon） | 循环内 try_wait |
| S9 | L | daemon.rs | start 失败退出码 0 | exit(1) |
| S10 | L | daemon.rs | kill(pid,0)=EPERM 误删他人 pidfile | EPERM 视为存活 |
| S4 | M | store | save_pipeline_upsert check-then-act 并发同名双插 | 名称扫描并入写事务 |
| S7 | L | store | 无 snapshot 的终态 task 永不进 prune plan | 非 running 且无 snapshot 也纳入 |

### 批次 C — operator / quickjs / cli

| # | 级别 | 位置 | 问题 | 修法 |
|---|------|------|------|------|
| O1 | H | http_client | SSRF 不识别 v4-mapped v6（`[::ffff:169.254.169.254]` 绕过）；0.0.0.0/CGNAT/198.18 未覆盖 | normalize_ip 归一化 + 补段 |
| O3 | M | quickjs | inflate 解压炸弹：Rust 侧无界 Vec 绕过 256MB 沙箱限制，expect 可 panic | take(256MB+1) 有界读 + JS 异常 |
| O4 | M | cli | 任务 Failed 时 CLI 退出码仍 0，CI 无法感知 | run_text/run_tui 返回 Err |
| O6 | M | cli | TUI 同步阻塞循环直接跑在 tokio worker 上，单核饿死 WS reader | spawn_blocking |
| O5 | M | llm | 共享 client 60s 总超时截断长生成 | llm 专用 client 600s |
| O7 | L | sort/filter | 混合 int/float f64 比较在 ≥2^53 破坏全序 | cmp_i64_f64/cmp_u64_f64 精确比较 |
| O9 | L | file | allowlist root 未 canonicalize，符号链接/相对 root 静默全拒 | root 先 canonicalize |
| O11 | L | cli | daemon log 不读 X-Log-Truncated，缺口无提示 | 检测并提示 |

### 仍开放（三次审计新增）

| # | 说明 | 处置 |
|---|------|------|
| O2 | DNS rebinding TOCTOU：SSRF 检查与 reqwest 连接各自解析 DNS | 共享 OnceLock client 架构下难钉 resolve，记为已知残余风险（低 TTL 恶意域名场景） |
| S5 | redb from_bytes 对损坏行 assert/expect panic | trait 限制无法返回 Result；单条坏行可 abort 请求/启动，待评估 catch_unwind |
| S8 | set_cache_bytes OBJECT+CACHE 两事务，与 prune scan 存在窗口 | 后果仅 cache miss 自愈，低优先 |
| S11 | semaphore 排队任务无上限（M16 有意设计的副作用） | 设计权衡，待需求 |
| O8 | filter eq/ne/in 是严格 JSON 相等（1 ≠ 1.0），与 gt/lt 数值语义不一致 | 语义文档化，不改行为 |
| O10 | file canonicalize→read TOCTOU（本地符号链接替换） | 需 openat2 级防护，本地威胁模型下接受 |
| — | RawStepOp::Noop 带 inputs 键是否被 serde 静默吞掉 | 待验证 |
| — | VariablePath::parse 的 trim() 使带空格字面量被意外当 ref | 设计取舍，待文档化 |

本轮修复由模型 **kimi-for-coding/k3** 执行。测试：186 lib + 51 集成全绿，clippy 0 警告。
