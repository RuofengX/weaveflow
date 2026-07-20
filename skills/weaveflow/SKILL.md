---
name: weaveflow
description: >
  Use when the task involves weaveflow — running YAML DAG pipelines, submitting
  batch/ETL jobs to the weaveflow daemon, watching pipeline progress, fetching
  step outputs/snapshots, or calling its HTTP/WS API. Triggers on weaveflow CLI
  usage, pipeline YAML files (slots/steps/output), 'weaveflow run/apply/daemon'
  commands, or requests to orchestrate multi-step data workflows and observe them.
---

# weaveflow — DAG 批处理引擎操作技能

weaveflow 是一个 YAML 声明式 DAG 批处理引擎（daemon + CLI + HTTP/WS API）。用它编排多步任务（http/filter/sort/js/command/llm 等 12 种算子）、实时观测执行、获取每步结构化结果。

本 skill 自包含：字段级细节见同目录 `references/`：

- `references/dsl.md` — DSL 完整参考（slots/steps/iterate/retry/校验错误码）
- `references/operators.md` — 12 个算子的输入字段表 + 示例
- `references/guide.md` — CLI 完整参考、daemon 生命周期、存储
- `references/agent.md` — Agent 集成模式、常见坑、故障定位顺序
- `references/api.md` — HTTP/WS API 请求/响应 schema

## Setup

```bash
which weaveflow || cargo build --release   # 二进制在 target/release/weaveflow
weaveflow pipeline ls --output json        # 探测 daemon 是否在线
# 不在线则：weaveflow daemon start
```

默认 daemon 地址 `127.0.0.1:9928`（`--daemon` flag / `WEAVEFLOW_DAEMON` env 覆盖）。**daemon 无鉴权，只在 localhost 使用，不要 --allow-remote 绑定 0.0.0.0。**

## 标准工作流（agent 模式）

全程加 `--output json`：单行紧凑 JSON，可直接 jq 解析。

```bash
# 1. 本地校验（无需 daemon；--output json 输出结构化 errors/warnings 报告）
weaveflow check -f pipeline.yml --output json

# 2. 注册 pipeline（同名 upsert；-d '<yaml>' 可直接传字符串）
weaveflow pipeline apply -f pipeline.yml --output json

# 3. 运行并观测：JSONL 快照流，每行一个 TaskSnapshot，最后一行即终态
weaveflow run <name> -i key=value -i data=@input.json \
  --text-output --output json | tee run.jsonl

# 4. 取结果：最后一行 status 为 {"Completed": <pipeline output>} 即成功
tail -1 run.jsonl | jq '.status'
#    {"Failed": "..."} → 失败，退出码 1，错误串在 status.Failed

# 5. 需要中间步骤输出时回放快照
weaveflow task snapshot list <task_id> --output json
weaveflow task snapshot show <task_id> <seq> --output json
```

要点：

- 非 TTY 下 `run` 自动回退 `--text-output`，不会卡在 TUI；`--watch` 与 `--text-output` 互斥。
- TaskSnapshot 关键字段：`status`（Running/Completed/Failed）、`steps[].state`（Pending/Running{attempts}/Iterating{progress}/Completed/Failed）、`total_duration_ms`。**Completed 终态快照直接携带最终结果，无需另查。**
- slot 注入：`-i k=v`（字符串）或 `-i k=@file.json`（JSON 文件）；pipeline 的 slot schema 会校验。

## YAML 编写

### 最小骨架

```yaml
name: my_pipeline            # 必填，唯一
slots:                       # 可选，调用方注入的占位符
  - name: source_url
    schema: { type: string, pattern: "^https?://" }
steps:                       # 必填，至少 1 步
  - id: fetch
    type: http
    inputs:
      url: "{slots.source_url}"
      method: GET
  - id: filter_adults
    type: filter
    inputs:
      data: "{fetch.output.body}"   # 引用上游输出 → 自动建立 DAG 依赖
      field: "age"
      operator: "gte"
      value: 18
output: "{filter_adults.output}"    # 必填
```

step 通用字段：`id` `type` `inputs` `after: [step_a]`（纯顺序依赖）`iterate` `retry: {max_attempts, backoff: fixed|exponential, delay_ms}` `cache: true|false` `timeout_sec`（支持小数）。字段级参考：`references/dsl.md`。

### 变量引用（最容易踩坑的部分）

- 支持：`{slots.name}` `{env.KEY}` `{step_id.output}` `{step_id.output.field}` `{step_id.output.0.field}`（数组下标严格：越界/非数字段是硬错误）。
- **整串匹配**：只有整个字符串恰好是 `"{...}"` 才是引用；`"prefix {slots.u}"` 是字面量。需要拼接时在 js 算子里做。
- **纯 String 字段永远字面量**：`http.method`、`filter.field/operator`、`sort.field/order`、`dedup.field`、`base64.mode`、`command.shell`、`llm.model/image_type` 写 `"{...}"` 也不会解析。
- 顶层 object/array 字面量里的 `"{...}"` 字符串可以深层解析（内联 Ref tag）。
- `{env.KEY}` 值会在持久化快照中脱敏——密钥一律走 env 引用，不要写死在 YAML。

### iterate（逐元素并行）

```yaml
- id: batch_step
  type: js
  iterate:
    over: "{slots.items}"    # 必须带花括号，指向数组
    as: "item"               # 必填但名字不绑定（见下）
    max_workers: 4           # 缺省 = CPU 核数
  timeout_sec: 30            # 每个元素独立计时/重试
  inputs:
    code: |
      function run(data) { return data * 2; }
```

- 当前元素固定注入 inputs 的 **`data` 键**；`as` 声明的名字**不会绑定**，`{item}` 是字面量。
- `batch: {size: 100}` 时按批切分，每批（数组）注入 `data`。

### 算子速查（12 种）

| type | 用途 | 关键 inputs |
|------|------|------------|
| `http` | HTTP 请求 | `url` `method` `headers` `body` |
| `js` | QuickJS 沙箱 | `code`（顶层须定义 `function run(data)`）`data` |
| `filter` | 数组过滤 | `data` `field` `operator`(eq/ne/gt/gte/lt/lte/in/contains) `value` |
| `sort` | 数组排序 | `data` `field` `order`(asc/desc) |
| `dedup` | 按字段去重 | `data` `field` |
| `merge` | 合并对象 | `a` `b` `deep` |
| `base64` | 编解码 | `data` `mode`(encode/decode) |
| `noop` | 透传（测试） | 无 |
| `var` | 变量占位 | `value`（任意 JSON） |
| `file` | 读文件/URL | `path` 或 `url` → 输出 `{content(base64), mimetype, size}` |
| `command` | sh -c | `command` `shell` `stdin` → 输出 `{stdout, stderr, exit_code, success, truncated}` |
| `llm` | OpenAI 兼容 | `url` `model` `prompt` `api_key`（必须 `{env.XXX}` 引用）等 |

各算子完整字段表：`references/operators.md`。注意：http/command/llm/file **默认不缓存**，开发期可显式 `cache: true`。

### 写 YAML 时的硬性规则

- 所有字段 `deny_unknown_fields`：拼错 key 直接解析错误，报错会指出非法 key，照改即可。
- 已删除字段写了硬报错：`snapshot_ttl`、JS inputs 的 `timeout`。
- js/command/http/llm step 一律配 `timeout_sec`——引擎没有隐式超时，死循环/慢 IO 会永久卡住。
- llm 的 `api_key` 禁止明文（validator 拒绝），必须 `{env.KEY}` 引用。

## Debug 排错

定位顺序：

1. `run` 非 0 退出 → 看 JSONL 最后一行 `status.Failed` 的错误串。
2. 错误指向某 step → `weaveflow task snapshot list/show <task_id> <seq> --output json` 回放它上游各步输出，确认哪一步数据不符合预期。
3. 任务卡住不动 → 多半是 js/command/http step 没配 `timeout_sec` 在等 IO；`weaveflow daemon log -f` 看实时日志。
4. apply/check 报错 → 错误 JSON 里有 `code`（如 `variable_ref_not_found`、`cycle_detected`、`invalid_iterate_config`），按码修 YAML 重试；错误码全表在 `references/dsl.md`。
5. daemon 行为异常 → `weaveflow daemon log` 或 `GET /system/logs`。

常见问题速查：

| 现象 | 原因 / 解法 |
|------|------------|
| `connection refused` | daemon 没启动：`weaveflow daemon start` |
| apply 报 unknown field | YAML key 拼错（deny_unknown_fields），报错里有非法 key 名 |
| 引用不生效 | 只有整串 `"{...}"` 才是引用；`method`/`field`/`operator` 等纯 String 字段永远字面量 |
| `variable_ref_not_found` | 引用的 step_id / slot 不存在，或写在纯 String 字段里 |
| `cycle_detected` | `{step.output}` 引用构成环；纯顺序依赖改用 `after` |
| iterate 不生效 / 解析错 | `over` 必须带花括号；元素从 `data` 键取，不是 `{item}` |
| 任务一直 Running | step 没配 `timeout_sec`；给 js/command/http/llm 加超时 |
| 结果/任务消失 | `storage.result_ttl` 过期被 prune（默认 3600s）；`weaveflow task ls` 确认 |
| `/runs` 返回 503 | daemon 正在 draining（收到停止信号），等排空结束或重启 |
| 缓存命中但想要重跑 | 该 step 数据没变（key = SHA256(op_type+inputs)）；改 inputs 或删 CACHE |

## HTTP / WS API（程序化集成）

| Method | Path | 用途 |
|--------|------|------|
| POST | `/pipelines`（YAML body） | 注册 pipeline |
| POST | `/runs` | 提交任务 → `{task_id}`（draining 时 503） |
| GET | `/runs/:task_id` | 轮询状态 |
| WS | `/runs/:task_id/ws` | 实时推送 TaskSnapshot |
| GET | `/runs/:task_id/snapshots` · `/:seq` | 步骤输出快照 |
| GET | `/tasks` · `/system/operators` · `/system/logs` | 任务列表 / 算子清单 / daemon 日志 |

流程：POST `/runs` 拿 task_id → WS 订阅（或轮询 GET）→ 收到 `status.Completed` 取结果。错误码：400 参数/解析错，404 不存在，503 draining，5xx 固定文案。完整 schema：`references/api.md`。
