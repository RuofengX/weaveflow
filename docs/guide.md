# weaveflow 使用手册

> YAML 声明式 DAG 批处理引擎：daemon + CLI + 内嵌 redb 存储 + QuickJS 沙箱。
> 本文是使用手册主入口。字段级细节：[dsl.md](dsl.md)（DSL）、[operators.md](operators.md)（算子）；编程调用：[api.md](api.md)（HTTP/WS）、[agent.md](agent.md)（Agent 集成）；部署边界：[security.md](security.md)；内部设计：[architecture.md](architecture.md)。

## 目录

1. [构建与安装](#构建与安装)
2. [快速开始](#快速开始)
3. [CLI 完整参考](#cli-完整参考)
4. [Daemon 生命周期](#daemon-生命周期)
5. [Pipeline 与变量模型](#pipeline-与变量模型)
6. [HTTP / WebSocket API 速览](#http--websocket-api-速览)
7. [面向 Agent 的集成速览](#面向-agent-的集成速览)
8. [存储与数据目录](#存储与数据目录)
9. [安全注意事项](#安全注意事项)
10. [故障排查](#故障排查)

---

## 构建与安装

```bash
cargo build --release          # 产出 target/release/weaveflow
cargo test --lib               # 186 单元测试（无外部依赖）
cargo test --test '*'          # 28 个集成测试二进制（in-process，无需 daemon）
```

无系统级安装命令，直接把二进制放入 PATH 或绝对路径调用即可。

## 快速开始

```bash
# 1. 启动 daemon（默认 127.0.0.1:9928，数据在 ~/.weaveflow）
weaveflow daemon start

# 2. 本地校验 YAML（不需要 daemon）
weaveflow check -f pipeline.yml

# 3. 注册 pipeline
weaveflow pipeline apply -f pipeline.yml

# 4. 运行并观测（TTY 下是 TUI；非 TTY 自动回退到文本进度）
weaveflow run etl_demo -i source_url=https://example.com/data.json --watch

# 5. 拿结构化结果（agent/脚本场景）
weaveflow run etl_demo -i source_url=@input.json --output json --text-output

# 6. 停止 daemon
weaveflow daemon stop
```

`-i k=v` 注入 slot 值；`-i k=@file.json` 从文件读取 JSON 注入。任务失败时 `run` 以退出码 1 结束。

## CLI 完整参考

```
weaveflow daemon start [--bind 127.0.0.1:9928] [--max-concurrent-tasks N] [--allow-remote] [--shutdown-drain 30s]
weaveflow daemon stop [--timeout 35s]
weaveflow daemon restart [...start opts] [--stop-timeout 35s]
weaveflow daemon log [-f]
weaveflow serve --bind ...                        # hidden；daemon start 的前台等价物
weaveflow mcp                                     # MCP stdio server（AI agent 接入）
weaveflow pipeline apply -f <file.yml> | -d '<yaml>'   # -f / -d 是 flag，不是位置参数
weaveflow pipeline ls                             # alias: list
weaveflow pipeline inspect <name>
weaveflow pipeline delete <name>
weaveflow routine apply -f <file.toml>            # TOML（CLI 本地解析）→ PUT JSON；幂等热更新
weaveflow routine ls                              # alias: list；含 total_fired/next_fire_at 等运行时状态
weaveflow routine inspect <name>
weaveflow routine delete <name>
weaveflow routine push <name> -d '[...]'          # stream 型；单对象自动包一层数组
weaveflow routine events <name> [--after N]       # 事件收件箱增量回查（智能体值班回查通道）
weaveflow run <name> [-i k=v] [-i k=@file.json] [--watch | --text-output]   # 两者互斥
weaveflow check -f <file.yml> [--output json]     # 纯本地校验，无需 daemon
weaveflow task ls
weaveflow task show <task_id> [--full]            # 默认 summary（省 token）
weaveflow task snapshot list <task_id>
weaveflow task snapshot show <task_id> <seq> [--max-bytes N] [--full]
weaveflow system prune [--force] [--dry-run]
weaveflow system operators
```

### 全局 flag 与环境变量

优先级统一为 **CLI flag > `WEAVEFLOW_*` env > 默认值**；时长参数接受 `500ms/30s/5m/1h`。

| Flag | Env | 默认 | 说明 |
|------|-----|------|------|
| `--daemon <host:port>` | `WEAVEFLOW_DAEMON` | `127.0.0.1:9928` | 可带 `http(s)://` 前缀，尾部 `/` 自动去除 |
| `--output text\|json` | `WEAVEFLOW_OUTPUT` | `text` | json = 单行紧凑 JSON，适合 agent / jq |
| `--http-timeout` / `--connect-timeout` | `WEAVEFLOW_HTTP_TIMEOUT` / `WEAVEFLOW_CONNECT_TIMEOUT` | `30s` / `5s` | CLI→daemon HTTP |
| `--ws-timeout` | `WEAVEFLOW_WS_TIMEOUT` | `10s` | `run --watch` 的 WS 连接 |
| `--prune-timeout` | `WEAVEFLOW_PRUNE_TIMEOUT` | `300s` | |
| `--log-timeout` / `--log-poll` | `WEAVEFLOW_LOG_TIMEOUT` / `WEAVEFLOW_LOG_POLL` | `2s` / `500ms` | `daemon log` |

Daemon 侧 env：`WEAVEFLOW_BIND`、`WEAVEFLOW_MAX_CONCURRENT_TASKS`、`WEAVEFLOW_SHUTDOWN_DRAIN`、`WEAVEFLOW_STOP_TIMEOUT`。

数据目录：仅 `WEAVEFLOW_DATA` env（默认 `~/.weaveflow`），**没有可用的 `--data-dir` flag**。

`weaveflow run` 在 stdout 非 TTY 时自动回退到 `--text-output`。

### 常用命令细节

- **`pipeline apply`**：同名 pipeline 走 upsert（单个 redb 写事务内扫描+插入，并发 apply 不会双插）。
- **`run -i`**：slot 注入。`-i k=v` 按字符串；`-i k=@file.json` 读取文件并解析为 JSON。pipeline 声明的 slot 有 JSON Schema 校验。
- **`run --output json --text-output`**：JSONL 快照流（见 [面向 Agent 的集成速览](#面向-agent-的集成速览)）。
- **`task snapshot`**：每个 step 完成时写一条 snapshot（seq 递增），`show` 可回放任意 step 的输出；二进制输出以 `[binary N bytes]` 占位显示。
- **`system prune`**：清理过期任务/快照/孤儿对象/悬空缓存；响应含 `snapshots_removed`；运行中任务永不被清理。
- **`check --output json`**：输出结构化校验报告（errors / warnings 列表，含 code），适合 CI 或 agent 预检。

## Daemon 生命周期

- `daemon start` 以后台进程运行（pidfile 管理），`serve` 是前台等价物（hidden，调试用）。
- **优雅排空**：收到信号后 `/runs` 立即返回 503，在途任务最多继续运行 `--shutdown-drain`（默认 30s）；`daemon stop --timeout`（默认 35s，应 ≥ drain）超时后 SIGKILL。
- **并发限制**：`--max-concurrent-tasks` / `WEAVEFLOW_MAX_CONCURRENT_TASKS`（默认不限），信号量在 daemon 内实现。
- `daemon log [-f]` 读取 daemon 环形缓冲日志，支持绝对 offset 续读（响应头 `X-Log-Offset` / `X-Log-Truncated`）。

## Pipeline 与变量模型

一个 YAML 文件 = 一个可执行 DAG。字段级参考见 [dsl.md](dsl.md)，这里只列最容易踩坑的规则：

- **DAG 边**来自两处：`inputs` 里的 `{step_id.output}` 引用（数据+顺序依赖）和 `after`（仅顺序依赖）。环在 `Dag::from_pipeline()` 的 Kahn 排序时检测。
- **引用语法**：`{slots.name}` / `{env.KEY}` / `{step_id.output}` / `{step_id.output.field}` / `{step_id.output.0.field}`（数组下标严格：非数字段或越界是硬错误）。
- **整串匹配**：只有**整个字符串**恰好是 `"{...}"` 才被当作引用；嵌在长字符串里的 `{...}` 一律是字面量。
- **内嵌 tag**：顶层 object/array 字面量内部的 `"{...}"` 字符串会被替换为单键 `{"Ref": ...}` 内联 tag，resolver 可深入解析；用户数据里带 `"Ref"` 键但还有其他键的对象不受影响。
- **纯 String 字段永远字面量**：`http.method`、`filter.field/operator`、`sort.field/order`、`dedup.field`、`base64.mode`、`command.shell`、`llm.model` 即使写成 `"{...}"` 也不会解析成引用。
- **iterate**：`over` 必须带花括号；当前元素绑定 `as` 名，`{item...}` 在 inputs 任意字段解析；重试/超时按元素独立；缓存粒度为整个 step，key 混入 resolved `over` 数组。
- **未知字段即错误**：所有 Raw 结构带 `deny_unknown_fields`，拼错的 YAML key 会直接报解析错误（已移除的 `snapshot_ttl`、JS inputs 的 `timeout` 同样硬失败）。
- **超时只在 step 层**：`timeout_sec` 包裹算子 future，JS 死循环会被真中断；HTTP client 自身没有任何隐式总超时。
- **缓存**：`cache_enabled = step.cache ?? 算子默认`；http/command/llm/file 默认不缓存；key = `SHA256(op_type + ":" + inputs_json)`；缓存命中报 `attempts=0, cached=true`。
- **`{env.KEY}`** 的值会被记录并在持久化快照中脱敏。

## HTTP / WebSocket API 速览

> 完整的请求/响应 schema、curl 示例与 TaskSnapshot 结构见 [api.md](api.md)。

| Method | Path | 说明 |
|--------|------|------|
| POST | `/runs` | 提交任务 → `{task_id, ...}`（异步；draining 时 **503**） |
| GET | `/runs/:task_id` | 任务状态 + 进度（`?summary=1` = token 友好模式：不带 inputs、不内嵌最终输出） |
| WS | `/runs/:task_id/ws` | 实时进度推送（TaskSnapshot JSON） |
| GET | `/runs/:task_id/snapshots` · `/:seq` | 快照列表 / 单条（`?max_bytes=N` 截断超长输出） |
| POST/GET | `/pipelines` | 创建（YAML body）/ 列表 |
| GET/DELETE | `/pipelines/:name` | 查看 / 删除 |
| PUT | `/routines/:name` | 创建/更新 routine（JSON body；幂等热更新 worker） |
| GET | `/routines` · `/routines/:name` | routine 列表（含运行时状态）/ 详情（含 buffered） |
| DELETE | `/routines/:name` | 删除 routine（停 worker + 清空收件箱） |
| POST | `/routines/:name/push` | stream 入口；缓冲满 → 429 |
| GET | `/routines/:name/events?after=&limit=` | 事件收件箱增量回查（智能体值班回查通道） |
| WS | `/routines/:name/ws` | routine 实时事件流 |
| GET | `/tasks` | 任务列表 |
| POST | `/prune` | 清理（响应含 `snapshots_removed`） |
| GET | `/system/operators` · `/system/logs` · `/system/version` | 算子清单 / daemon 日志（绝对 offset） / 版本信息 `{version, build_code}`（CLI 构建码不一致时告警，识别旧版 daemon 残留） |

错误：400 参数/解析/校验，404 不存在，503 draining，5xx 固定文案。终态 `TaskSnapshot.status.Completed` 直接携带 pipeline 最终结果。

## 面向 Agent 的集成速览

> 完整模式、MCP 接入、routine 值班范式与写 YAML 的常见坑见 [agent.md](agent.md)。

**首选 MCP**：`weaveflow mcp` 是标准 MCP stdio server（17 个 tools），在 Claude Code / opencode 注册即可：

```json
{ "mcpServers": { "weaveflow": { "command": "weaveflow", "args": ["mcp"] } } }
```

CLI 等价路径：

```bash
weaveflow check -f p.yml --output json                       # 预检（无需 daemon）
weaveflow pipeline apply -f p.yml --output json              # 注册
weaveflow run p -i k=v --text-output --output json           # JSONL 快照流
#   每行一个 TaskSnapshot，最后一行 status.Completed 即结果；Failed → 退出码 1
weaveflow task show <task_id> --output json                  # summary（省 token）
weaveflow task snapshot show <task_id> <seq> --max-bytes 2000 --output json
```

要点：全程 `--output json`（单行紧凑，jq 友好）；非 TTY 自动回退 `--text-output`，不会卡 TUI；`run` 退出码即成败；task/snapshot 默认走省 token 模式。

## 存储与数据目录

- 内嵌 redb，位置 `WEAVEFLOW_DATA`（默认 `~/.weaveflow`），含 PIPELINE / TASK / SNAPSHOT / OBJECT / CACHE / ROUTINE / ROUTINE_EVENT 七张表（旧 trigger 表打开时自动迁移为 routine 表）。
- 所有值内联存储，无外部 spill 文件；schema 以类型名版本化（`::v1`、Snapshot `::v2`），v0 库打开时自动迁移（先备份 `.v0.bak`）。
- `storage.result_ttl` 控制任务结果保留时长（默认 3600s，下限 60s）；过期由 `system prune` 清理。
- 快照编码：`seq(8B BE) | step_id_len(4B BE) | step_id | output`。

## 安全注意事项

> 完整边界与加固清单见 [security.md](security.md)，这里只列最关键的三条。

- **所有端点无鉴权**：绑定 `0.0.0.0` 等于未授权 RCE（`command`/`file` 算子）；localhost 下也有浏览器 CSRF 风险。**daemon 只应在 localhost 使用。**
- `command` 等价于 shell、`file` 可读本地文件——能提交任务的人等价于有代码执行权，这是设计前提。
- 密钥走 `{env.KEY}` 引用（持久化快照会脱敏），不要写死在 YAML 或 slot 值里。

## 故障排查

| 现象 | 排查 |
|------|------|
| `connection refused` | `weaveflow daemon start`；确认 `--daemon` / `WEAVEFLOW_DAEMON` 指向正确地址 |
| apply 报 unknown field | Raw 结构 deny_unknown_fields——检查 key 拼写；`snapshot_ttl`、JS `timeout` 已移除 |
| 校验 `cycle_detected` | `{step.output}` 引用构成环；用 `after` 表达纯顺序依赖 |
| `variable_ref_not_found` | 引用必须是整串 `"{...}"`；纯 String 字段（method/field/operator 等）不支持引用 |
| iterate 不生效 | `over` 必须带花括号（`"{slots.items}"`）；元素用 `{item}` 引用（`item` = `as` 名），如 `data: "{item}"` |
| 任务一直 Running | 无 `timeout_sec` 的 js/command/http step 没有隐式超时；`daemon log -f` 看日志 |
| 结果消失 | `result_ttl` 过期被 prune；`weaveflow task ls` 确认任务是否还在 |
| `503` on `/runs` | daemon 正在 draining（收到停止信号），等排空结束或重启 |
