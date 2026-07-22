# weaveflow Agent 集成指南

> 让 LLM Agent 可靠地驱动 weaveflow：声明 pipeline、跑批量数据、委托长驻任务、跨会话拿回反馈。接口细节见 [api.md](api.md)。

weaveflow 的定位是**智能体的上下文外数据引擎**：bulk 数据在引擎内流过确定性算子，不占上下文窗口；每个 step 输出持久化为快照，可随时取证；routine 让 agent 能把「盯着做」的事委托给 daemon。

## 接入方式一：MCP（首选）

`weaveflow mcp` 是标准 MCP stdio server（基于 rmcp），在 Claude Code / opencode 等客户端注册：

```json
{ "mcpServers": { "weaveflow": { "command": "weaveflow", "args": ["mcp"] } } }
```

17 个 tools 覆盖完整闭环：

```
validate_pipeline → apply_pipeline → run_pipeline → list_snapshots / get_snapshot
upsert_routine → push_routine → get_routine_events（seq 游标增量回查）
```

设计取向（也是给 agent 的使用建议）：

- **默认省 token**：`run_pipeline` / `get_task_status` 返回 summary（不带 inputs、不内嵌最终输出）；`get_snapshot` 应始终传 `max_bytes`（如 2000）。
- **先校验后注册**：`validate_pipeline` 本地跑，不接触 daemon，errors 为空再 apply。
- **长任务用异步**：`run_pipeline(wait=false)` 拿 task_id，轮询 `get_task_status`；或干脆注册 routine。

## 接入方式二：CLI（脚本/无 MCP 客户端时）

核心原则：

1. **全程 `--output json`**：所有命令输出单行紧凑 JSON，直接 jq 解析，不要解析 text 格式。
2. **先 `check` 后 `apply`**：本地校验零成本，避免把校验错误留到 daemon。
3. **JSONL 流拿终态**：`run --text-output --output json` 每行一个 TaskSnapshot，**最后一行即终态，`status.Completed` 直接携带结果**，无需再查接口。
4. **退出码即成败**：任务 Failed → exit 1；CLI/网络错误也非 0。

```bash
# 0. 探测 daemon（不在线则启动）
weaveflow pipeline ls --output json >/dev/null 2>&1 || weaveflow daemon start

# 1. 本地校验（无需 daemon；errors 为空才算过）
weaveflow check -f pipeline.yml --output json

# 2. 注册（同名 upsert；也可 -d '<yaml 字符串>' 不落盘）
weaveflow pipeline apply -f pipeline.yml --output json

# 3. 运行 + 观测（tee 留存进度，便于事后排查）
weaveflow run <name> -i key=value -i data=@input.json \
  --text-output --output json | tee run.jsonl

# 4. 取结果
tail -1 run.jsonl | jq '.status'
# {"Completed": <pipeline 输出>}  → 成功，值即结果
# {"Failed": "<错误串>"}          → 失败（进程退出码 1）
```

### 进度观测（可选）

```bash
jq -r 'select(.status.Running) | .steps[] | "\(.step_id): \(.state|keys[0])"' run.jsonl
```

- iterate 步骤：`state.Iterating.progress` 给出 `{total, done}`。
- 重试中：`state.Running.attempts` > 1。
- 缓存命中：`state.Completed.cached == true` 且 `attempts == 0`。

### 任务状态与中间步骤输出（省 token 路径）

```bash
weaveflow task show <task_id> --output json                 # summary：不带 inputs/内嵌输出
weaveflow task show <task_id> --full --output json          # 完整（大）
weaveflow task snapshot list <task_id> --output json        # [{seq, step_id}]
weaveflow task snapshot show <task_id> 2 --max-bytes 2000 --output json
```

`output` 可能是二进制包装（`{"_binary": true, "_base64": ...}`）或截断预览（`"…[truncated, N bytes total]"`），解析时判一下。

## Routine：智能体值班范式

agent 的会话是短暂的，但有些工作是持续的——这正是 routine 的用途：**把「盯着做」委托给 daemon，daemon 替你值班，你下次醒来查收件箱**。

```
注册：upsert_routine（cron 定时 / stream 微批）
  ↓ daemon 持续触发 pipeline run（agent 会话可以结束）
反馈：每个终态落事件到收件箱（每 routine 保留 100 条）
  ↓
回查：get_routine_events(after=<上次见过的最大 seq>) → 增量拿到跨会话历史
唤醒（可选）：notify.webhook_url → daemon 主动 POST 终态事件给 harness，harness 唤醒 agent
```

CLI 等价路径：

```bash
weaveflow routine apply -f watch.toml        # TOML → PUT JSON（也可直接 PUT HTTP API）
weaveflow routine ls --output json
weaveflow routine events watch --after 12 --output json
```

routine TOML 示例：

```toml
name = "watch-feed"
pipeline = "etl_demo"
type = "cron"

[cron]
interval = "5m"
inputs = { source_url = "https://example.com/d.json" }

[notify]
webhook_url = "http://127.0.0.1:8800/wake"   # 可选；SSRF 策略同 http 算子
preview_bytes = 2048
```

事件 `kind`：`fired` / `failed` / `dropped` / `task_completed` / `task_failed` / `notify_failed`。终态事件附 `output_preview`（pipeline 最终输出的截断版，默认 2KB）。

## 标准流程（HTTP API）

不用 CLI/MCP 时的等价路径：

```
POST /pipelines        (YAML body)         → 200 {id, name, steps, slots}
POST /runs             {"pipeline","inputs"} → 200 {task_id, layers}
WS  /runs/:task_id/ws                       → 首帧当前快照，随后增量推送
  或 GET /runs/:task_id?summary=1（1~2s 轮询，读 status 字符串）
终态: status.Completed → 结果；status.Failed → 错误串
GET /runs/:task_id/snapshots/:seq?max_bytes=N → 任意 step 的输出（截断）
```

错误处理：400（YAML/校验/参数问题，error 字段含 code 和说明，**把错误反馈给 LLM 让它修 YAML 重试**）/ 404（名字打错）/ 503（daemon 正在停机，稍后重试）/ 500（看 `GET /system/logs` 或 `weaveflow daemon log`）。

## 写 pipeline 时 Agent 最常踩的坑

| 坑 | 正确写法 |
|----|---------|
| 引用没包整串 | `url: "{slots.u}"` 整串才是引用；`"prefix {slots.u}"` 是字面量 |
| 给纯 String 字段写引用 | `method`/`field`/`operator`/`mode`/`shell`/`model` 永远字面量，不支持 `{...}` |
| iterate 取元素 | 当前元素绑定 `as` 名：`{item}` / `{item.field}` 在 inputs 任意字段解析 |
| iterate `over` 没花括号 | `over: "{slots.items}"`，少了 `{}` 直接解析错误 |
| js 死循环 | js/command/http step 一律配 `timeout_sec`，引擎没有兜底超时 |
| 拼错 YAML key | 所有字段 deny_unknown_fields——报错信息会指出非法 key，照改即可 |
| 已删除字段 | `snapshot_ttl`、JS inputs 的 `timeout` 已移除，写了硬报错 |
| 字符串拼接 | 用 `f"..."` 模板（`\{` `\}` 转义字面花括号），或在 js 算子里拼 |

## 与 LLM 算子组合的模式

weaveflow 的 `llm` 算子是 OpenAI 兼容协议。Agent 编排 LLM 链的典型结构：

```yaml
steps:
  - id: draft
    type: llm
    timeout_sec: 120
    retry: { max_attempts: 3, backoff: exponential, delay_ms: 2000 }
    inputs:
      url: "https://api.openai.com/v1/chat/completions"
      model: "gpt-4o"
      prompt: "{slots.topic}"

  - id: critique
    type: llm
    timeout_sec: 120
    inputs:
      url: "https://api.openai.com/v1/chat/completions"
      model: "gpt-4o"
      prompt: "{draft.output}"        # 上一步文本直接作为下一步输入

output: "{critique.output}"
```

要点：llm/http 默认**不缓存**，重跑即重新计费——开发期可对 step 显式 `cache: true`；`Authorization` 头用 `{env.KEY}` 注入（快照会脱敏），不要写进 YAML。**批量 prompt 评测**是杀手场景：`iterate` 把数据集逐条喂给 llm step，js 算子打分，快照逐条对比。

## 故障定位顺序

1. `run` 非 0 退出 → 看 JSONL 最后一行 `status.Failed` 的错误串。
2. 错误串指向某 step → `task snapshot list/show` 看它上游的输出是否符合预期。
3. 任务卡住不动 → 该 step 多半没配 `timeout_sec` 且算子在等 IO；`weaveflow daemon log -f` 看实时日志。
4. daemon 行为异常 → `GET /system/logs`（带 offset 续读）或 `weaveflow daemon log`。
5. routine 没触发 → `routine inspect` 看 `next_fire_at`/`total_failed`；`routine events` 看 `failed`/`dropped` 事件。
