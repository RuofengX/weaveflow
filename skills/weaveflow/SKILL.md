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

weaveflow 是一个 YAML 声明式 DAG 批处理引擎（daemon + CLI + HTTP/WS API）。你可以用它编排多步任务（http/filter/sort/js/command/llm 等 12 种算子）、实时观测执行过程、获取每一步的结构化结果。完整文档在仓库 `docs/guide.md`，DSL 细节在 `docs/dsl.md`，算子字段在 `docs/operators.md`。

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

## HTTP / WS API（程序化集成）

| Method | Path | 用途 |
|--------|------|------|
| POST | `/pipelines`（YAML body） | 注册 pipeline |
| POST | `/runs` | 提交任务 → `{task_id}`（draining 时 503） |
| GET | `/runs/:task_id` | 轮询状态 |
| WS | `/runs/:task_id/ws` | 实时推送 TaskSnapshot |
| GET | `/runs/:task_id/snapshots` · `/:seq` | 步骤输出快照 |
| GET | `/tasks` · `/system/operators` · `/system/logs` | 任务列表 / 算子清单 / daemon 日志 |

流程：POST `/runs` 拿 task_id → WS 订阅（或轮询 GET）→ 收到 `status.Completed` 取结果。错误码：400 参数/解析错，404 不存在，503 draining，5xx 固定文案。

## 排错速查

- `connection refused` → `weaveflow daemon start`。
- apply 报 unknown field → YAML key 拼写错误（deny_unknown_fields）；`snapshot_ttl`、JS inputs 的 `timeout` 已移除。
- 引用不生效 → 只有整串 `"{...}"` 才是引用；`method`/`field`/`operator` 等纯 String 字段永远字面量。
- iterate → `over` 必须带花括号（`over: "{slots.items}"`）；元素固定注入 inputs 的 `data` 键，`as` 名字不绑定。
- 任务不结束 → 给 js/command/http step 配 `timeout_sec`（引擎没有隐式超时）；`weaveflow daemon log -f` 看日志。
- 详细手册：`docs/guide.md`；审计遗留问题：`TODO.md`。
