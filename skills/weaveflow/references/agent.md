# weaveflow Agent 集成指南

> 让 LLM Agent / 脚本可靠地驱动 weaveflow：提交任务、观测过程、拿结果。接口细节见 [api.md](api.md)。

## 核心原则

1. **全程 `--output json`**：所有命令输出单行紧凑 JSON，直接 jq 解析，不要解析 text 格式。
2. **先 `check` 后 `apply`**：本地校验零成本，避免把校验错误留到 daemon。
3. **JSONL 流拿终态**：`run --text-output --output json` 每行一个 TaskSnapshot，**最后一行即终态，`status.Completed` 直接携带结果**，无需再查接口。
4. **退出码即成败**：任务 Failed → exit 1；CLI/网络错误也非 0。

## 标准流程（CLI）

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

JSONL 中间行可渲染进度：

```bash
jq -r 'select(.status.Running) | .steps[] | "\(.step_id): \(.state|keys[0])"' run.jsonl
```

- iterate 步骤：`state.Iterating.progress` 给出 `{total, done}`。
- 重试中：`state.Running.attempts` > 1。
- 缓存命中：`state.Completed.cached == true` 且 `attempts == 0`。

### 中间步骤输出

```bash
weaveflow task snapshot list <task_id> --output json     # [{seq, step_id}]
weaveflow task snapshot show <task_id> 2 --output json   # {seq, step_id, output}
```

`output` 可能是二进制包装（`{"_binary": true, "_base64": ...}`），解析时判一下。

## 标准流程（HTTP API）

不用 CLI 时的等价路径：

```
POST /pipelines        (YAML body)         → 200 {id, name, steps, slots}
POST /runs             {"pipeline","inputs"} → 200 {task_id, layers}
WS  /runs/:task_id/ws                       → 首帧当前快照，随后增量推送
  或 GET /runs/:task_id（1~2s 轮询，读 progress.status）
终态: status.Completed → 结果；status.Failed → 错误串
GET /runs/:task_id/snapshots/:seq           → 任意 step 的输出
```

错误处理：400（YAML/校验/参数问题，error 字段含 code 和说明，**把错误反馈给 LLM 让它修 YAML 重试**）/ 404（名字打错）/ 503（daemon 正在停机，稍后重试）/ 500（看 `GET /system/logs` 或 `weaveflow daemon log`）。

## 写 pipeline 时 Agent 最常踩的坑

| 坑 | 正确写法 |
|----|---------|
| 引用没包整串 | `url: "{slots.u}"` 整串才是引用；`"prefix {slots.u}"` 是字面量 |
| 给纯 String 字段写引用 | `method`/`field`/`operator`/`mode`/`shell`/`model` 永远字面量，不支持 `{...}` |
| iterate 取元素 | 元素固定注入 inputs 的 `data` 键；`as` 的名字**不会绑定**，`{item}` 是字面量 |
| iterate `over` 没花括号 | `over: "{slots.items}"`，少了 `{}` 直接解析错误 |
| js 死循环 | js/command/http step 一律配 `timeout_sec`，引擎没有兜底超时 |
| 拼错 YAML key | 所有字段 deny_unknown_fields——报错信息会指出非法 key，照改即可 |
| 已删除字段 | `snapshot_ttl`、JS inputs 的 `timeout` 已移除，写了硬报错 |
| 中间字符串插值 | 需要拼接时在 js 算子里做，YAML 层不支持嵌入式插值 |

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

要点：llm/http 默认**不缓存**，重跑即重新计费——开发期可对 step 显式 `cache: true`；`Authorization` 头用 `{env.KEY}` 注入（快照会脱敏），不要写进 YAML。

## 故障定位顺序

1. `run` 非 0 退出 → 看 JSONL 最后一行 `status.Failed` 的错误串。
2. 错误串指向某 step → `task snapshot list/show` 看它上游的输出是否符合预期。
3. 任务卡住不动 → 该 step 多半没配 `timeout_sec` 且算子在等 IO；`weaveflow daemon log -f` 看实时日志。
4. daemon 行为异常 → `GET /system/logs`（带 offset 续读）或 `weaveflow daemon log`。
