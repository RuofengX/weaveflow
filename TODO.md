# weaveflow TODO — v1.0 台账

> 本文件只记两件事：**待修改的开放项**（上半）与 **v1.0 已完成的修改**（下半）。
> 行为与用法文档在 [docs/](docs/README.md)；工程约定在 [AGENTS.md](AGENTS.md)。
> 质量基线：244 lib tests + 74 集成测试（32 个二进制）+ 48 bin tests 全绿，`cargo clippy --all-targets` 0 警告。

---

# 待修改（开放项）

## A. 值得优先处理

（暂无）

## B. 低优先 / 已接受的残余风险

| # | 位置 | 问题 | 处置 |
|---|------|------|------|
| S8 | `store/mod.rs` | `set_cache_bytes` OBJECT+CACHE 两事务，与 prune scan 存在窗口 | 后果仅 cache miss 自愈，低优先 |
| S11 | `server/daemon.rs` | semaphore 排队任务无上限（M16 把 permit 移入后台任务的副作用） | 设计权衡，待需求 |
| L10 | `tracker/tracker.rs` | broadcast Lagged 静默跳过 + std Mutex poison | 与 H7/H8 正交，需 parking_lot 迁移，不宜单改 |
| L12 | `quickjs/runtime.rs` | QuickJS 原生函数 expect panic | 依赖 rquickjs 版本升级才能改成 Result |
| L13 | `vm/resolver.rs` | resolver 递归无深度限制 | 已较安全（axum 2MB body limit 限制输入大小） |
| O2 | `operator/builtin/http_client.rs` | DNS rebinding TOCTOU：SSRF 预检与 reqwest 建连各自解析 DNS | 共享 OnceLock client 架构下难钉 resolve；低 TTL 恶意域名场景的已知残余 |
| O10 | `operator/builtin/file.rs` | canonicalize→read 之间 TOCTOU（本地符号链接替换） | 需 openat2 级防护；本地威胁模型下接受 |

## C. 语义 / 设计决策（多数"文档化，不改行为"）

| # | 位置 | 问题 | 处置 |
|---|------|------|------|
| L4 | `engine/runner.rs` + `vm/scope.rs` | 大 Value 多级深拷贝 | 需改 Scope 签名 + 并行期合并，架构改动大 |
| L5 | `dsl/variable.rs` | 疑似模板（嵌在长串中的 `{...}`）静默变字面量，无 warning | 需 validator 重构；M12/M13 已修相关项。2026-07-21 起可用显式 `f"..."` 模板（opt-in，非法即 parse error） |
| L6 | `dsl/variable.rs` | `{foo}` 无转义手段（想要字面量 `{foo}` 无法表达） | DSL 设计决策：普通串维持无转义（兼容）；`f"..."` 模板内提供 `\{`/`\}` 转义（2026-07-21） |
| O8 | `operator/builtin/filter.rs` | eq/ne/in 是严格 JSON 相等（`1 ≠ 1.0`），与 gt/lt 的数值语义不一致 | 语义文档化，不改行为 |
| — | `dsl/variable.rs` | `VariablePath::parse` 的 trim() 使首尾带空格的字面量被意外当 ref | 设计取舍，待文档化 |

## D. 子代理深度测试发现（2026-07-22）— 待确认的设计项

33 项发现中 29 项已修复（见下半"v1.0 已完成 → 子代理深度测试修复"），以下 4 项改动大或涉及设计理念，待确认后处理：

| # | 位置 | 问题 | 待定方向 |
|---|------|------|----------|
| D12 | `server/routine.rs:533-544` | stream permit 释放依赖 1s 轮询 DB，快任务吞吐被钳制在 ~7 批/s/routine | 改 tracker 终态回调（架构改动） |
| D24 | `dsl/parser.rs` | 解析错误泄露内部 Rust 类型名（`RawStepOp::Noop` 等） | 需错误消息映射层 |
| D29 | `dsl/validator.rs` | `variable_ref_not_found`/`slot_not_found` 不定位到具体输入字段，多条错误按字段名字典序排列 | 需 refs 提取携带字段路径（validator 重构） |
| D32 | cron 调度 | cron 无并发上限：`interval:1s` + 慢 pipeline 任务无限叠加（stream 有 max_in_flight，cron 无等价物） | 加 cron max_in_flight？或文档化 |

## 有意保留（非缺陷，勿"修复"）


| 行为 | 理由 |
|------|------|
| **全部端点无鉴权（C6）** | 定位 localhost-only 工具，鉴权是网关/反代层职责；`--allow-remote` 绑非 loopback 时启动打印醒目警告。开放此能力是为便于 hack，非开发者场景 |
| **redb 坏行 `from_bytes` panic（S5）** | 任何时刻都假定数据库文件完好；文件系统级腐败不在防御范围（redb 自带 checksum）。catch_unwind 包裹无实际收益 |
| `find_pipeline_by_name` 全表扫描 | pipeline 数量小，不加索引 |
| JS step 无 `timeout_sec` 时死循环占 blocking 线程 | 超时只在 step 层（2026-07-20 用户决策：任何隐式超时都不得截断 step 执行） |
| `yes` 类无限输出命令跑到 step 超时为止 | 10MB 截断只保护内存；`kill_on_drop` 保证 step 超时取消时回收子进程 |
| `check_js_syntax` 短暂执行用户代码 | 已沙箱化（64MB 内存 + 2s 中断 + 无 fs/net），可接受风险 |

---

# v1.0 已完成

## 功能清单

- DSL 层：parser / validator（30+ 校验规则）/ `deny_unknown_fields` 全覆盖 / JSON Schema slot 校验
- 语义 Key 类型：PipelineId / TaskId / SnapshotKey / ObjectDigest / CacheKey
- 存储层：redb 五表（PIPELINE/TASK/SNAPSHOT/OBJECT/CACHE）+ Snapshot v2 二进制布局 + 两阶段 prune + v0 自动迁移
- 引擎：Kahn 拓扑分层 / 层内 join_all 并行 / iterate 逐元素并行（默认并发 = CPU 核数）/ 缓存（SHA256 key）/ retry+backoff / step 级 timeout_sec（JS 经 drop-guard 真中断）
- 变量系统：`{slots.*}` / `{env.*}`（快照脱敏）/ `{step.output.*}` 含数组下标 / 整串匹配 + 内联 Ref tag
- TaskTracker：in-memory 状态机 + `snapshot_and_subscribe` 原子订阅 + WS broadcast + cleanup_stale
- daemon：REST + WS + 优雅停机 drain + 并发信号量 + pidfile 二进制校验 + 环形日志（绝对 offset）
- CLI：pipeline/run/task/system/check 全命令 + `--watch` TUI + `--text-output` + 非 TTY 自动回退 + 统一配置层（flag > env > 默认）+ `--output json` / JSONL 快照流
- 12 个内置算子：http / js / filter / sort / dedup / merge / base64 / noop / var / file / command / llm（`api_key` Bearer 注入，validator 拒明文）
- 安全加固：共享 HTTP client（不跟 redirect、全 DNS SSRF 检查、64MB 流式限长、无隐式总超时）/ command env_clear+白名单+10MB 截断 / file 白名单+64MB / QuickJS 沙箱 256MB+1MB 栈+inflate 限长
- `pipeline apply` 同名 upsert（单写事务原子）/ `run -i k=@file.json` 文件输入 / 任务失败退出码 1
- 文档体系：docs/ 八篇分册 + agent SKILL

## MCP 试用修复（2026-07-22）

试用 17 个 MCP 工具发现 5 个问题，全部处置：

| # | 问题 | 修复 |
|---|------|------|
| M1 | MCP `Value` 参数（run_pipeline.inputs / push_routine.items / upsert_routine.def）被客户端序列化成 JSON 字符串时不解析——push 场景把数组当单个字符串元素静默入库 | `mcp.rs::parse_maybe_json_string`：字符串且可解析为 JSON 则用之，否则原样保留（纯字符串元素仍可 push） |
| M2 | iterate `over` 解析为 Null（slot 未传）时静默按 1 个元素迭代，产出垃圾输出且任务"成功" | `iterate.rs`：Null → BadRequest 硬错误；非数组非标量 → warn 日志按单元素迭代 |
| M3 | `get_task_result` 描述说返回 final output，实际返回整个 task JSON（含 inputs 明文） | 改为提取 `progress.status`：Completed → `{status, output}`，Failed → `{status, error}` |
| M4 | command 非零退出码步骤仍 Completed | 确认为有意（管道可消费预期非零退出，如 grep 无匹配），已在 docs/operators.md 文档化 |
| M5 | validate 顶层类型错误行号恒为 1:1 | parser 错误消息后追加"顶层字段 X 应为…"提示 |

验证：229 lib + 44 bin + 71 集成测试全绿，clippy 0 警告；MCP stdio 端到端复测 5 项全部通过。

## 子代理深度测试修复（2026-07-22）

5 个 QA 子代理并行深测发现 33 项（D1-D33），修复 29 项（D12/D24/D29/D32 待设计确认，留在开放项）；另修用户报告的 daemon 版本升级后无法 stop 的 bug：

| # | 问题 | 修复 |
|---|------|------|
| D1 | 终态快照 >16MiB 时 `run` 误报失败（WS 单帧超 tungstenite 16MiB 默认上限，协议错误被吞成"连接丢失"） | daemon axum `max_frame_size`/`max_message_size` 与 CLI tungstenite 配置均放宽到 256MiB；WS 协议错误拼进渲染层错误消息（`（WebSocket: ...）`） |
| D2+D33 | env 脱敏不覆盖最终 output：tracker/WS/get_task_result/routine `output_preview` 均带明文，且 preview 明文落盘 ROUTINE_EVENT | runner 在 `tracker.complete` 前对最终 output 走同一 `redact_env_values`，下游全部消费脱敏值 |
| D3 | 缓存无法通过 prune 清除（env 明文 OBJECT 永久残留） | 用户决策：缓存明文可接受（用户不可直接访问，后续可做端到端加密存储）；`PruneOptions.include_cache` + CLI `prune --include-cache` + HTTP `include_cache`：清空 CACHE 表及其独占 OBJECT，dry-run 含字节预估 |
| D4 | tracker 回收（10min）后 `get_task_result` 把终态任务误报 `running`（与 `get_task_status` 矛盾） | 非 summary 响应补 DB `status` 字段；MCP 在 progress 为 null 时回退 DB 终态 |
| D5 | stream `flush_interval` 空闲期后失效（过期 tick Burst，首元素单飞） | `MissedTickBehavior::Delay` + 每批首元素 `reset()`：批次窗口从首元素到达起算 |
| D6 | DELETE/热更新丢弃 channel 中已 accepted 元素 | 取消分支先 `rx.try_recv()` drain channel 再 flush 全部缓冲 |
| D7 | 必填 slot（无 default）缺失静默变 null、跳过 schema 校验 | runner 启动即 `BadRequest("缺少必填 slot...")`（validator 是静态 YAML 校验，run inputs 只能运行时判） |
| D8 | `with_type_hint` 把合法 `output`（任意 JSON）误判"应为字符串" | 移除 output 检查项 |
| D9 | webhook payload `seq` 恒为 0（与收件箱不一致） | `emit` 返回分配 seq 后的记录，webhook 投递该记录 |
| D10 | cron interval 锚点为 last_fired_at，逐周期漂移 | 锚点固定 created_at 网格；`missed_fire` 改为"最近网格点晚于 last_fired 即错过" |
| D11 | `buffer_cap` 不含阻塞在信号量上的批次（429 偏松） | `buffered.fetch_sub` 移到 `acquire` 成功之后 |
| D13 | `__native__.atob` 非法 base64 触发 Rust panic | 比照 `inflate` 返回 JS 异常 |
| D14 | dedup 把"字段值为 null"误判"字段缺失"不去重 | 新增 `resolve_nested_opt` 区分缺失/显式 null；null 作为正常 key 参与去重 |
| D15 | filter `in`/`contains` 操作数类型错静默返回空数组 | 运行前置 Config 错误（in 要求 value 数组、contains 要求 value 字符串） |
| D16 | `check --output json` 解析失败不输出 JSON + 前缀重复 | 解析失败也输出结构化 JSON；不再重复包裹"YAML 解析失败:"前缀 |
| D17 | 非 TTY 自动 `--text-output` 文档声称但未实现 | `run` 无 flag 且 stdout 非 TTY 时自动进入 watch 流式模式（失败 exit 1） |
| D18 | base64 掩码按解码公式报告错误字节数 | 改为报告原始字符数（启发式本身保留，纯文本长串仍会命中） |
| D19 | `prune --dry-run` 的 `snapshots_removed` 恒 0 | 按各 task max_seq 预估 |
| D20 | `?max_bytes` 非法值静默忽略 | 非正整数 → 400 |
| D21 | 大 YAML apply >2MiB 被 axum 默认 2MiB 限 413 | `DefaultBodyLimit::max(64MB)`（413 纯文本改结构化错误未做，影响小） |
| D22 | `prune --force` 无 help 说明 | 补 doc comment（"忽略 result_ttl，删除所有终态任务"） |
| D23 | `{env.KEY.多余段}` 静默忽略多余段 | validator 新增 `invalid_env_ref`：env 引用必须恰好两段 |
| D25 | slot `schema.default` 不做静态校验 | validator 用编译后的 schema 校验 default（`invalid_slot_default`） |
| D26 | filter/sort/dedup/base64 缺 `data`、file 无 `path`/`url` 运行时必炸但 check 不报 | validator 新增 `missing_required_input` 静态检查 |
| D27 | `no_upstream_deps` 对只引用 as 名的 iterate 步骤假阴性 | 依赖收集排除 as_name 前缀 |
| D28 | `cycle_detected` 不指出环成员 | Kahn 剩余节点列入消息 |
| D30 | `pipeline apply` 静默丢弃 warnings | apply 响应带 `warnings`，CLI 回显 |
| D31 | `iterate.batch.size` 无上限、`as` 允许纯数字 | batch.size ≤ 1048576；as 首字符禁数字 |
| — | **新 CLI 无法 stop 旧 daemon**（升级后 `verify_pid_binary` 全路径不等 → 误判 PID 复用，拒绝 kill 还误删 pidfile，只能手动 sudo kill） | `verify_pid_binary` 改为比较 exe **文件名**（剥离 " (deleted)" 后缀），升级场景同名即视为同一 daemon，异名仍挡 PID 复用 |

验证：244 lib + 48 bin + 74 集成测试全绿（新增 17 个测试覆盖上述修复），clippy 0 警告；daemon 实机复测 D1（17MB 输出 run 成功）/D7（缺 slot 明确报错）/D17（非 TTY 自动流式）/D20（400）/D30（warnings 回显）/prune --include-cache（dry-run 字节预估、缓存清除后不再 ♻）全部通过。

## 三轮审计汇总

| 轮次 | 日期 | 范围 | 发现 | 修复 | 转入开放 |
|------|------|------|------|------|---------|
| 一次审计 | 2026-07-17 | 全仓 ~7400 行 | 72（C8/H21/M27/L16） | 65（P0-P2 全清，P3 修 9/16） | C6 + L4/L5/L6/L10/L12/L13/L14 |
| 复审 + 二次审计 | 2026-07-18 | 一轮复审（1 项回归）+ 全仓二次审计 | 40+ | 全部 | 维持上轮开放项 |
| 三次审计 | 2026-07-20 | 四轮并行 subagent（engine/vm/tracker、dsl、store/server、operator/quickjs/cli） | 30 | 24（三批：A DSL/resolver/DAG 一致性、B daemon/store、C operator/quickjs/cli） | O2/S5/S8/S11/O8/O10 + 2 项待验证 |

关键提交：P0 `9824a90`+`e6214da`；P1 `1267e0d`+`536edf8`+`40ff54a`；P2 `24a5d9a`+`dac393b`+`8782396`+`dad7f3d`；P3 `41e28f3`；refactor `bd1bdb3`（timeout → timeout_sec）。

### 2026-07-20 用户决策（覆盖三次审计批次 C 的 O5）

任何隐式超时都不得截断 step 执行：共享 HTTP client 的 60s 总超时与 llm 的 600s 专用超时一并移除。
超时只在 step 层（`timeout_sec`）显式配置；未配置时请求可无限等待。connect_timeout 10s 保留为建连快速失败下限。

### 2026-07-20 开放项处置决议（A 组清零）

- **C6（无鉴权）→ 有意保留**：weaveflow 只做 localhost 开放服务，鉴权属网关层职责；`--allow-remote` 保留启动警告。
- **S5（redb 坏行 panic）→ 有意保留（wontfix）**：假定数据库完好；文件系统腐败不是防御目标。
- **Noop inputs 吞没 → 验证为误报**：serde adjacent-tag 对 unit variant 带 `inputs` 直接 parse 报错
  （`invalid type: map, expected unit variant RawStepOp::Noop`），不存在静默吞没，无需修复（报错定位信息较差，可接受）。
- **L14（llm api_key）→ 已修复**：`LlmInputs` 新增 `api_key` 字段（发送 `Authorization: Bearer`）；
  validator 拒绝明文字面量（`insecure_api_key`），仅接受 `{env.*}` / var / file 步骤引用（`{env.*}` 自动进快照脱敏集）；
  新增 2 个 validator 测试；docs/operators.md 已同步。

---

# 归档：审计明细

<details>
<summary><b>一次审计（2026-07-17）72 项明细</b> — 65 项已修复，7 项转开放</summary>

修复统计：P0 8/8（C1-C8，提交 `9824a90`+`e6214da`）、P1 21/21（H1-H21）、P2 26/26（M1-M27）、P3 9/16（L 系列）。

**Critical（P0）**：C1 http 算子读 `data` 而 DSL 字段是 `body`；C2 `spec().cache==false` 被忽略；
C3 `batch.size=0` 未校验；C4 iterate 缓存 key 不含 over 数组；C5 RunState.status 是过期 Progress 克隆；
C7 iterate.over 解析失败 expect panic；C8 TTL 反序列化溢出 panic。（C6 转开放）

**High（P1）**：H1 step.timeout 死配置（实现 run_with_timeout）；H2 backoff 死配置（实现指数退避）；
H3 max_attempts=0；H4 step.cache 未读取；H5 iterate 绕过 retry（每元素套 retry）；
H6 CLI 不检查 HTTP 状态码；H7 WS handler 不 recv；H8 WS get/subscribe 竞态（snapshot_and_subscribe）；
H9 已完成任务永不清理（cleanup_stale）；H10 prune 可删运行中任务；H11 OBJECT/CACHE 永不回收；
H12 空表边界 500；H13 stop 不校验进程身份；H14 daemon 启动失败报假成功；H15 BadRequest/Parse 返回 500；
H16 http/file/llm 无超时/限制/SSRF（共享 client）；H17 file 路径遍历；H18 command 继承全环境+无输出上限；
H19 JS 沙箱无内存上限+超时不可中断；H20 llm 字节切片非 char 边界 panic；H21 validator extract_refs 误拒。

**Medium（P2）**：M1 缓存 key DefaultHasher → SHA256；M2 Literal 内嵌 Ref 不提取；M3 batched 非数组静默丢弃；
M4/M5 pipeline output 语义；M6 inputs 键坍缩；M7 数组下标静默 Null → 严格；M8 env 机密落 snapshot（脱敏）；
M9-M11 validator 不检环/未声明 slot/自引用；M12 无 deny_unknown_fields；M13 TTL 负值 + VariablePath 误接受；
M14 runner panic 永远 Running；M15 DB Mutex 跨长操作；M16 信号量阻塞 HTTP 响应；M17 名字符集 + URL 编码；
M18 CLI 无超时/TUI 断连空转；M19 dedup 缺字段塌缩；M20 base64 非 UTF-8 静默空串；M21 sort 非全序；
M22 merge 仅浅合并；M23 filter 大整数精度；M24 command 管道死锁；M25 prune 读写间隙 + save_snapshot 吞错；
M26 redb expect panic + schema 无版本（::v1）；M27 无优雅停机 + TUI Ctrl-C。

**Low（P3）已修**：L1 tracker state hardcoded；L2 失败 layer 后 step 永久 Pending → Skipped；
L3 重复 id 静默 + max_workers=0；L7 `--data-dir` 死代码删除；L8 `-i k=@broken.json` 静默降级 → 报错；
L9 ws:// 地址推导；L11 check_cache/set_cache 死代码；L15 未使用依赖；L16 内部错误详情泄露 → 5xx 固定文案。
（L4/L5/L6/L10/L12/L13/L14 转开放）

测试增量（本伦）：120 unit + 27 integration + 11 bin → 缓存/retry/timeout/env 脱敏/数组下标/merge deep/
filter 大整数/store GC/tracker/file 守卫/js 沙箱等缺口全部补测。
</details>

<details>
<summary><b>复审 + 二次审计（2026-07-18）40+ 项明细</b> — 全部修复</summary>

**复审批（含 1 项回归）**：iterate 默认并发塌缩为 1 → `effective_max_workers()`（缺省 = available_parallelism）；
JS inputs 自带 timeout 双配置 → 删除（step 层 timeout_sec 统一）；step timeout 只 drop future 而 spawn_blocking
线程仍占槽 → InterruptGuard drop-guard 协作中断 `while(1){}`；timeout NaN/Inf/溢出 → try_from_secs_f64 +
validator 上限 365 天；失败路径 attempts 固定 1 → 真实次数；缓存命中 attempts=0/cached=true；缓存写失败
降级 warn；iterate Completed 双写 → 收口 runner；字符串内嵌 `{step.output}` 幽灵依赖 → 整串守卫；
resolver 数组下标硬错误/缺字段 Null+warn；noop 输出被 `{"type":"noop"}` 信封污染 → 顶层信封剥离；
JS 异常/正常值共通道 → `__weaveflow_ok__` 信封；TaskStatus::Pending 变体删除；env_values Mutex poison 恢复。

**二次批次二（validator/DSL/算子）**：check_js_syntax 真实 eval 无限制 → 沙箱化（64MB + 2s watchdog +
仅 SyntaxError 判错）；TTL 非 ASCII 单位 split_at panic；含 `"Ref"` 键多键对象误当 ref → 三方 `len()==1` 守卫；
slot 收集误报内嵌字符串；deny_unknown_fields 补齐全部 Raw 结构；filter/sort 枚举白名单（validator+运行时双层）；
filter/sort/dedup/base64 缺 data → Config 错误；resolve_nested 支持数组下标；command 子进程泄漏 → kill_on_drop +
10MB 后继续 drain + truncated 标记；http_client SSRF 绕过 → 全 DNS 检查 + 禁 redirect + 流式 64MB + Url 解析防
userinfo 伪造；file 白名单空段处理；registry.rs 死代码删除；ValidateOptions/别名死代码删除；raw 层错误透传
ParseError::Raw；名字符集白名单；pipeline 名全 `.` 不可达；iterate.max_workers ≤ 1024；sort/filter 共用
compare_json_numbers；dedup warn 聚合。

**二次批次三（store/daemon/cli）**：首启数据目录 create_dir_all；storage.result_ttl 死配置接线（snapshot_ttl
删除）；v0 数据库自动迁移（.v0.bak 备份）；全局 DB Mutex → RwLock 仅 compact 取写锁；优雅停机 drain 30s；
pidfile 竞态 + /proc 二进制校验；日志绝对 offset + X-Log-Offset/X-Log-Truncated；count/list snapshots
全量反序列化 → SnapshotHeader 视图；Snapshot serde_json 4 倍膨胀 → 自定义二进制 v2；ObjectValue.ref_count
从不维护 → 删除；daemon URL 尾斜杠；prune 超时放宽 300s；WS connect 10s 超时；非 TTY 自动回退 text-output；
--watch/--text-output 互斥；daemon log https/flush/退出码；TUI "(parallel)" 文案；PruneResponse 补
snapshots_removed；serve 参数白名单重组；build.rs 死代码删除。

本伦后总数：169 lib tests + 28 个集成测试二进制（51 个集成测试）+ 24 bin tests。
</details>

<details>
<summary><b>三次审计（2026-07-20）30 项明细</b> — 修复 24，开放 6</summary>

**批次 A（DSL/resolver/DAG 一致性）**：D1 单键 `{"Ref": 不可解析}` 运行时硬错误 vs validator 放行 → 回退普通对象；
D2 Literal 负载内单键 `{"Literal": X}` 被静默拆包 → 只在字段位置识别；D3 裸 String 字段整串 `"{...}"` 三方语义
分裂（cycle 假阳性/误拒/静默错值）→ 统一按字面量；D4 iterate.as 无校验 → 保留前缀/id 冲突/非法字符拒绝；
D5 `{as_name.x}` 误拒 → 透传对齐；D6 retry 无上限 → attempts ≤ 100 / delay ≤ 1h；D7 llm.temperature NaN/Inf 静默吞；
D8 base64.mode/http.method 补白名单；E1 slots 路径不支持数组下标 → 与 step 对齐；E4 slot 解析 warn 泄露整层值；
E5 unused_slot 误报 → 按首段匹配。

**批次 B（daemon/store）**：S3 draining 检查与 in_flight+1 之间 TOCTOU → 先占计数再复查；E2/S6 runner panic
跳过 in_flight 回收 → watcher 无条件回收；E3 panic 路径步骤永远 Running → fail_non_terminal_steps；
S1 stop() 10s SIGKILL < 30s drain → drain+5s；S2 健康检查不校验子进程存活 → try_wait；S9 start 失败退出码 0；
S10 kill EPERM 误删他人 pidfile；S4 save_pipeline_upsert check-then-act 双插 → 并入写事务；
S7 无 snapshot 的终态 task 永不进 prune plan → 纳入。

**批次 C（operator/quickjs/cli）**：O1 SSRF 不识别 v4-mapped v6 + 0.0.0.0/CGNAT/198.18 → normalize_ip 补段；
O3 inflate 解压炸弹 → take(256MB+1) 有界读；O4 任务 Failed 退出码仍 0 → exit 1；O6 TUI 同步阻塞跑在 worker
上 → spawn_blocking；O7 混合 int/float ≥2^53 破坏全序 → cmp_i64_f64/cmp_u64_f64；O9 file allowlist root
未 canonicalize；O11 daemon log 不读 X-Log-Truncated → 检测提示。（O5 llm 专用 600s 超时被用户决策否决，
见上半"用户决策"节）

开放 6 项：O2（DNS rebinding TOCTOU）、S5（redb 坏行 panic）、S8（缓存两事务窗口）、S11（排队无上限）、
O8（filter eq 严格相等）、O10（file TOCTOU），另 2 项待验证（Noop inputs 吞没、VariablePath trim）。
</details>

<details>
<summary><b>引擎层超时取消可行性分析（2026-07-17，历史参考）</b></summary>

已被 2026-07-18 复审批超越：最终实现为"方案 B 的零成本变体"——step 层 `tokio::time::timeout` drop
`run_js` future 时，InterruptGuard drop-guard 置位 AtomicBool 触发 QuickJS `set_interrupt_handler`
协作中断（`quickjs/runtime.rs`）。无需改 Operator trait。

原始结论：异步算子（http/llm/file-URL/command）的引擎层超时取消完备（future drop 即取消底层连接/子进程）；
JS 算子（spawn_blocking）的协程取消无法终止 OS 线程，必须协作式中断。候选方案：A 算子内部读 timeout
自中断（当时的实现）；B 引擎注入取消标志（Arc 无法放进 serde_json::Value）；C 全局 step 取消注册表
（iterate 并发实例难区分）；D 替换 spawn_blocking（rquickjs FFI 不感知 tokio）。
</details>
