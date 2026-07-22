# weaveflow HTTP / WebSocket API 参考

> daemon 默认监听 `127.0.0.1:9928`。**所有端点无鉴权**——只允许 localhost 使用，详见 [security.md](security.md)。
> 请求/响应字段与 `src/server/daemon.rs` 的 handler 一一对应。

## 通用约定

- 请求/响应 body 均为 JSON（`POST /pipelines` 除外，body 是 YAML 文本）。
- 错误响应统一为 `{"error": "<message>"}`：

| HTTP 状态 | 触发 |
|-----------|------|
| 400 | 参数错误、YAML 解析错误、DSL 校验失败（body 含具体信息） |
| 404 | pipeline / task / snapshot 不存在 |
| 503 | daemon 正在停机排空（draining），`POST /runs` 拒绝新任务 |
| 500 | 固定文案 `"internal server error"`（内部细节只进 daemon 日志，不外泄） |

- `POST /runs` 是**异步**的：立即返回 `task_id`，执行在后台进行。

---

## Pipelines

### `POST /pipelines` — 注册 pipeline

Body：`Content-Type: text/plain`（或任意），内容为 pipeline YAML 全文。同名 pipeline 走 upsert。

```bash
curl -X POST http://127.0.0.1:9928/pipelines --data-binary @pipeline.yml
```

响应 `200`：

```json
{
  "id": "3f8a...uuid",
  "name": "etl_demo",
  "steps": 4,
  "slots": [{ "name": "source_url", "schema": { "type": "string" } }]
}
```

校验失败返回 `400`，`error` 字段为 `[code] message; [code] message` 形式的多条拼接。未注册的 step `type` 同样 400。

### `GET /pipelines` — 列表

```json
[{ "id": "3f8a...uuid", "name": "etl_demo" }]
```

### `GET /pipelines/:name_or_id` — 查看

按 name 或 UUID 查询，响应为完整的 `PipelineDef` JSON（name/slots/steps/output 等，字段与 DSL 对应，引用以 `{"Ref": ...}` tag 形式出现）。

### `DELETE /pipelines/:name` — 删除

```json
{ "deleted": "etl_demo" }
```

---

## Runs（任务）

### `POST /runs` — 提交任务

```bash
curl -X POST http://127.0.0.1:9928/runs \
  -H 'Content-Type: application/json' \
  -d '{"pipeline": "etl_demo", "inputs": {"source_url": "https://example.com/d.json"}}'
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `pipeline` | string | ✓ | pipeline 名 |
| `inputs` | object | — | slot 注入值（key = slot 名，值为任意 JSON） |

响应 `200`（立即返回，不等执行完）：

```json
{
  "task_id": "b21c...uuid",
  "pipeline_name": "etl_demo",
  "status": { "Running": { "steps": [ ... ] } },
  "layers": [{ "index": 0, "step_ids": ["fetch"] }]
}
```

- pipeline 不存在 → `404`；draining → `503`。
- 并发上限（`--max-concurrent-tasks`）的信号量在后台任务内获取，本接口不会被并发打满而挂起。

### `GET /runs/:task_id` — 查询状态

```json
{
  "task_id": "b21c...",
  "pipeline_name": "etl_demo",
  "inputs": { "source_url": "..." },
  "created_at": "2026-07-20T08:00:00+00:00",
  "snapshot_count": 3,
  "progress": { /* TaskSnapshot，任务还在 tracker 内存中才有；终态任务被回收后此字段缺失 */ }
}
```

结果取值：`progress.status.Completed` 即 pipeline 最终输出；任务被回收（终态 10 分钟后）后，用 snapshots 接口取各 step 输出。

**`?summary=1`（token 友好模式，面向 LLM 消费者）**：不返回 `inputs`；`progress.status.Completed` 内嵌的完整输出替换为 `true`；额外返回持久化的 `status` 字符串（`running`/`completed`/`failed`）。结果细节按需走 snapshots 端点。

### `WS /runs/:task_id/ws` — 实时进度推送

连接后立即推送一帧当前 `TaskSnapshot`（`snapshot_and_subscribe` 单次加锁，无 get-then-subscribe 竞态），之后每次状态变化推一帧。任务终态后连接由服务端关闭。task 不存在 → 升级前返回 404。

### `GET /runs/:task_id/snapshots` — 快照列表

```json
[{ "seq": 1, "step_id": "fetch" }, { "seq": 2, "step_id": "filter_adults" }]
```

每个完成的 step 写一条快照，seq 单调递增。

### `GET /runs/:task_id/snapshots/:seq` — 单条快照

```json
{ "seq": 1, "step_id": "fetch", "output": { "status": 200, "body": [ ... ] } }
```

`output` 特殊形态：

- 正常：step 输出的 JSON 值。
- 输出不是合法 JSON（二进制）：`{"_binary": true, "_size": N, "_base64": "..."}`。
- 输出是 JSON `null` 但原始 bytes 非空（异常信号）：`{"_anomalous_null": true, "_notice": "...", "_raw_size": N, "_raw_hex": "..."}`。

**`?max_bytes=N`（token 友好截断）**：`output` 紧凑序列化超过 N 字节时，替换为头部预览字符串（`"…[truncated, M bytes total]"`，M 为总字节数）。http/file/command 步骤的输出可能几十 KB，agent 应始终带此参数。

---

## Routines（智能体委托的长驻任务岗）

routine 引用已注册的 pipeline，把「定时」（cron）与「流式微批」（stream）两类触发转化为普通 pipeline run；任务终态事件写入**持久化收件箱**并可选 webhook 推送——agent 无需长连接守候，下次会话用 seq 游标增量回查（智能体值班范式）。

### `PUT /routines/:name` — 创建/更新（幂等，热更新 worker）

```json
{
  "name": "watch-feed",
  "pipeline": "etl_demo",
  "type": "cron",
  "cron": { "interval": "5m", "inputs": { "source_url": "https://example.com/d.json" }, "misfire": "skip" },
  "notify": { "webhook_url": "http://127.0.0.1:8800/wake", "preview_bytes": 2048 }
}
```

| 字段 | 说明 |
|------|------|
| `type` | `"cron"` 或 `"stream"`，与 `cron`/`stream` 配置段二选一对应 |
| `cron.schedule` | 5/6/7 段 cron 表达式（与 `interval` 二选一） |
| `cron.interval` | 固定间隔 `"30s"/"5m"/"1h"`（≥ 1s，以 created_at 为锚点，重启不漂移） |
| `cron.misfire` | `catch_up`（补最近一次）/ `skip`（默认） |
| `cron.inputs` | 每次触发作为 slots 提交的静态输入 |
| `stream.batch_size` | 凑够 N 条 flush 一批（默认 100） |
| `stream.flush_interval` | 距上一条到达超过该时长即 flush（可选） |
| `stream.slot` | 微批数组写入的 slot 名（默认 `"items"`） |
| `stream.max_in_flight` | 同时运行的微批 task 上限（默认 4） |
| `stream.buffer_cap` | 缓冲元素上限，到顶 push 返回 429（默认 100000） |
| `notify.webhook_url` | 终态事件 POST 到该 URL（可选；SSRF 策略同 http 算子；3 次指数退避，4xx 不重试；投递失败落 `notify_failed` 事件） |
| `notify.preview_bytes` | 终态事件 `output_preview` 字节上限（默认 2048，最大 65536） |

校验失败（`deny_unknown_fields`、段不匹配、pipeline 不存在等）→ 400。daemon 只接收 JSON；TOML 是 CLI 侧的本地载体（`weaveflow routine apply -f x.toml`）。

### `GET /routines` / `GET /routines/:name` — 列表 / 详情

列表项：`{name, pipeline, type, created_at, last_fired_at, next_fire_at, total_fired, total_failed}`。详情为完整 `RoutineRow`（含 def 与运行时状态）外加实时 `buffered`（stream 缓冲中的元素数）。

### `DELETE /routines/:name` — 删除

停 worker（stream worker 先 flush 剩余缓冲再退出），并清空该 routine 的事件收件箱。

### `POST /routines/:name/push` — stream 入口

body 为 JSON 数组（单值自动包一层）。响应 `{"accepted": N, "buffered": M}`；缓冲满 → 429。缓冲纯内存，重启丢弃未 flush 数据。

### `GET /routines/:name/events?after=<seq>&limit=<n>` — 事件收件箱（智能体回查）

返回 `seq > after` 的事件（升序，limit 默认全部，建议 ≤ 50）。每个 routine 保留最近 100 条。**用法：agent 持久化已读到的最大 seq，下次会话作为 `after` 传入即拿到跨会话的完整反馈历史。**

```json
[{
  "seq": 7,
  "routine": "watch-feed",
  "kind": "task_completed",
  "task_id": "b21c...",
  "output_preview": { "...": "截断到 preview_bytes 的 pipeline 最终输出" },
  "at": "2026-07-21T10:00:00Z"
}]
```

`kind` 取值：`fired`（产生 task）/ `failed`（提交失败）/ `dropped`（draining 丢弃）/ `task_completed` / `task_failed`（附 `error`）/ `notify_failed`（webhook 投递失败）。

### `WS /routines/:name/ws` — 实时事件流

与收件箱同源的事件（JSON 帧），按 routine 名过滤，适合实时观察；可靠回查请用收件箱端点。

---

## Tasks

### `GET /tasks` — 任务列表（持久层）

```json
[{
  "task_id": "b21c...",
  "pipeline_name": "etl_demo",
  "created_at": "2026-07-20T08:00:00+00:00",
  "status": "completed"
}]
```

`status` 为持久化字符串（`running` / `completed` / `failed` / `failed_interrupted`），来自 redb，daemon 重启后仍可查。

---

## Prune

### `POST /prune` — 清理过期数据

```bash
curl -X POST http://127.0.0.1:9928/prune -H 'Content-Type: application/json' \
  -d '{"force": true, "dry_run": false}'
```

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `pipeline` | string | — | 只清理指定 pipeline 的任务 |
| `force` | bool | `false` | 忽略 `result_ttl`，清理所有终态任务 |
| `dry_run` | bool | `false` | 只扫描不删除 |
| `include_cache` | bool | `false` | 同时清空全部步骤缓存（CACHE 表 + 其独占 OBJECT） |

响应：

```json
{
  "tasks_removed": 12,
  "snapshots_removed": 48,
  "objects_removed": 30,
  "cache_entries_removed": 5,
  "bytes_freed": 1048576,
  "dry_run": false
}
```

保证：运行中的任务永不被清理；prune 扫描期间新写的快照（seq > 扫描时 max_seq）不会被删。

---

## System

### `GET /system/version` — 版本信息

```json
{ "version": "1.0.2", "build_code": "a1b2c3" }
```

`build_code` 是构建时的 git 提交短哈希（6 位，非 git 构建为 `"unknown"`）。CLI 每次访问 daemon 前会比对自身构建码，不一致时 stderr 告警（识别旧版本 daemon 残留）。

### `GET /system/operators` — 算子清单

```json
[{ "type_name": "http", "description": "...", "iterate": true, "cache": false }]
```

`cache` 是该算子的默认缓存开关（可被 step 级 `cache:` 覆盖）。

### `GET /system/logs?offset=N` — daemon 日志

响应 `text/plain`，从绝对 offset 读取环形缓冲日志：

| 响应头 | 说明 |
|--------|------|
| `X-Log-Offset` | 下次续读应传的 offset（绝对、单调） |
| `X-Log-Truncated` | 为 `1` 时表示 ring buffer 已覆盖旧内容，日志有缺失 |

---

## TaskSnapshot 结构

`POST /runs` 响应的 `status`、WS 推送帧、`GET /runs/:task_id` 的 `progress`、以及 CLI `--text-output --output json` 的 JSONL 每一行，都是同一个结构：

```json
{
  "task_id": "b21c...",
  "pipeline_name": "etl_demo",
  "status": { "Running": { "steps": [ /* StepProgress */ ] } },
  "layers": [{ "index": 0, "step_ids": ["fetch", "fetch2"] }],
  "steps": [
    {
      "step_id": "fetch",
      "timeout_sec": 60.0,
      "state": { "Running": { "started_at": "...", "attempts": 1 } }
    }
  ],
  "started_at": "2026-07-20T08:00:00Z",
  "completed_at": null,
  "total_duration_ms": null
}
```

### `status`（externally tagged）

| 形态 | 含义 |
|------|------|
| `{"Running": {"steps": [...]}}` | 执行中 |
| `{"Completed": <任意 JSON>}` | 成功，值即 pipeline `output:` 解析结果——**终态快照直接携带最终结果** |
| `{"Failed": "<error string>"}` | 失败 |

### `steps[].state`（externally tagged）

| 形态 | 附加字段 |
|------|---------|
| `"Pending"` / `"Skipped"` | 无 |
| `{"Running": {...}}` | `started_at`, `attempts` |
| `{"Iterating": {...}}` | `started_at`, `progress: {total, done}` |
| `{"Completed": {...}}` | `started_at`, `completed_at`, `attempts`, `cached`, `duration_ms` |
| `{"Failed": {...}}` | `started_at`(可空), `completed_at`, `error`, `attempts` |

`layers` 是 DAG 拓扑分层（同层 step 并行执行），可用于渲染并行分组。

## 客户端时序建议

```
POST /runs ──► task_id
     │
     ├─ 实时：WS /runs/:task_id/ws，收到 status.Completed/Failed 后关闭
     └─ 轮询：GET /runs/:task_id（1~2s 间隔），progress.status 出现终态即停
     │
     ▼
需要中间步骤输出？GET /runs/:task_id/snapshots → 按 seq 逐条取
```
