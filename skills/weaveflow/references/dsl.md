# weaveflow DSL 参考

> YAML 声明式管道定义语言。一个 YAML 文件 = 一个可执行的 DAG。

## Pipeline 结构

```yaml
name: my_pipeline          # 必填，唯一标识
description: ...            # 可选

storage:                    # 可选
  result_ttl: 30d           # 任务结果保留时长（snapshot_ttl 字段已移除，写了会报 unknown field 错误）

slots:                      # Pipeline 级占位符（调用方注入）
  - name: source_url
    schema: { type: string, pattern: "^https?://" }

steps:                      # 必填，至少 1 步
  - id: step_id
    type: operator_name     # 内置算子名，或 "js" 表示内联 JS
    after: [step_a]          # 可选，显式顺序依赖
    inputs:                  # 算子输入（JS 的 code 字段也在内）
    iterate:                 # 可选，step 根级，对所有算子生效
      over: "{upstream.output}"
      as: "item"
      max_workers: 4
      batch:
        size: 100
    cache: true
    retry:
      max_attempts: 3
      backoff: exponential
      delay_ms: 1000
    timeout_sec: 60

output: "{step_id.output}"  # 必填
```

### storage

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `result_ttl` | string | `3600s` | 任务结果保留时长（如 `"30d"`/`"2h"`），实际生效；下限 60 秒（更小的值按 60s 计） |

`snapshot_ttl` 字段已删除：所有 Raw 结构带 `deny_unknown_fields`，写了会直接报解析错误。

## Slots（Pipeline 级占位符）

调用方在 `POST /runs` 时注入值：

```yaml
slots:
  - name: source_url
    schema: { type: string, pattern: "^https?://" }
  - name: api_key
    schema: { type: string }
```

引用：`{slots.source_url}` 或 `{slots.api_key}`。

## 变量引用

| 模式 | 解析 |
|------|------|
| `{slots.name}` | Pipeline 占位符 |
| `{env.HOME}` | 环境变量 |
| `{step_id.output}` | 步骤完整输出 |
| `{step_id.output.field}` | 步骤输出字段访问（数字段可索引数组，如 `{step.output.0.name}`；非数字/越界为硬错误） |

iterate 模式注意：`as` 声明的元素变量名**当前不会被绑定**——当前元素固定注入到算子 inputs 的 `data` 键；写成 `{item}` 的引用会按字面量字符串透传，不会解析。

## 步骤定义

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `id` | string | ✓ | — | 唯一标识 |
| `type` | string | ✓ | — | 算子类型（内置名 或 `"js"`） |
| `after` | string[] | — | — | 顺序依赖（不需要数据） |
| `iterate` | object | — | — | 迭代配置（step 根级，所有算子均支持） |
| `inputs` | object | — | — | 算子输入（JS 算子的 `code` 字段也在此） |
| `cache` | bool | — | 算子默认 | 是否启用缓存 |
| `retry` | object | — | — | 重试策略 |
| `timeout_sec` | f64 | — | — | 超时秒数（支持小数，如 `0.5`；旧名 `timeout` 仍兼容；上限 365 天，超出校验报错） |

超时语义：`timeout_sec` 用 `tokio::time::timeout` 包裹算子 future——超时即取消该次执行（JS 算子的 QuickJS 运行时经 interrupt handler 真中断，包括 `while(1){}` 死循环），记录 `warn` 日志并按 `retry` 配置重试（每次尝试都重新计时）；重试耗尽后该 layer 标记失败。

### retry

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `max_attempts` | u32 | 1 | 最大尝试次数（1 = 不重试） |
| `backoff` | string | fixed | `fixed` / `exponential` |
| `delay_ms` | u64 | 1000 | 基础间隔（毫秒） |

### iterate

`step.iterate` 存在即启用逐元素并行展开，所有算子类型均支持：

```yaml
- id: batch_step
  type: js
  iterate:
    over: "{upstream.output}"  # 变量引用，指向数组
    as: "item"                 # 元素变量名（见下注：当前不会绑定）
    max_workers: 4             # 缺省 → 按 CPU 核数（available_parallelism）
    batch:                     # 可选：按数组批次传给算子
      size: 100
  inputs:
    code: |
      function run(data) { return process(data); }
    # 当前元素（或 batch 数组）固定以 `data` 键注入 inputs，
    # 会覆盖 inputs 中原有的 data 字段
```

| 字段 | 说明 |
|------|------|
| `over` | 变量引用，指向数组（必须带花括号，如 `"{slots.items}"`） |
| `as` | 元素变量名。**当前实现不会把该名字绑定进作用域**：元素固定注入 inputs 的 `data` 键，`{item}` 引用按字面量透传 |
| `max_workers` | 并发数（缺省 = `available_parallelism`，即 CPU 核数；不允许 0，上限 1024） |
| `batch` | 批量模式，`batch.size` 个元素打包传给算子（不允许 0） |

- 无 `batch`：每个元素作为裸对象注入 `data`
- 有 `batch`：按 `size` 切分数组，每批作为数组注入 `data`
- 重试按元素独立进行（每个 chunk 单独套 retry + timeout）

### 内联 JS（`type: js`）

无需预注册，直接在 DSL 中写 JS 源码。`code` 字段写在 `inputs` 内部：

```yaml
  - id: custom_filter
    type: js
    inputs:
      code: |
        function run(data) {
          return data.filter(function(o) {
            return o.status === 'paid';
          });
        }
      data: "{slots.orders}"
```

- `code` 字段包含 JS 源码
- 必须在顶层定义 `function run(data) { ... }`
- `data` 为输入数据（来自 `data` 字段）
- QuickJS 沙箱执行，无 fs/net 访问
- **inputs 没有 `timeout` 字段**（写了会报 unknown field 错误）：超时由 step 根级 `timeout_sec` 统一控制，超时会真中断 QuickJS 执行（含 `while(1){}` 死循环）

## 内置算子

| 算子 | 功能 | 状态 |
|------|------|------|
| `http` | HTTP 请求（GET/POST/PUT/DELETE） | ✅ |
| `filter` | 按条件过滤数组 (eq/ne/gt/gte/lt/lte/in/contains) | ✅ |
| `sort` | 按字段排序 (asc/desc) | ✅ |
| `dedup` | 按字段去重 | ✅ |
| `merge` | 合并两个对象 (b 覆盖 a) | ✅ |
| `base64` | Base64 编解码 | ✅ |
| `noop` | 直接返回输入（测试用） | ✅ |
| `var` | 变量占位——接受任意 inputs 原样输出 | ✅ |
| `file` | 读取本地文件或远程 URL | ✅ |
| `command` | 执行 Shell 命令 | ✅ |
| `llm` | OpenAI 兼容的 LLM 推理 | ✅ |
| `js` | 内联 QuickJS 沙箱（`code` 字段写入 `inputs` 内） | ✅ |

各算子的输入字段、默认值、配置示例详见 [operators.md](operators.md)。

## 校验

| 级别 | 说明 | 能否忽略 |
|------|------|---------|
| error | ID 重复、引用不存在、iterate/retry/timeout 配置无效、filter/sort 枚举越界、JS 语法错误、Schema 非法、依赖环 | ❌ |
| warning | unused_slot、orphan_step、no_upstream_deps | 仅提示，不阻断 |

### Error codes

`empty_pipeline_name`, `invalid_pipeline_name`, `no_steps`, `empty_step_id`,
`reserved_step_id`, `duplicate_step_id`, `empty_slot_name`, `duplicate_slot_name`,
`invalid_name_charset`, `after_self_ref`, `duplicate_after_entry`,
`after_ref_not_found`, `variable_ref_not_found`, `output_ref_not_found`,
`slot_not_found`, `self_reference`, `invalid_iterate_config`,
`invalid_retry_config`, `invalid_timeout`, `invalid_operator_config`,
`invalid_json_schema`, `cycle_detected`, `js_syntax_error`, `js_runtime_error`

### Warning codes

`unused_slot`, `orphan_step`, `no_upstream_deps`

## DAG 推断

执行器从两个来源构建 DAG 边：

1. **`inputs` 中的 `{step_id.output}` 引用** → 数据 + 顺序依赖
2. **`after` 字段** → 仅顺序依赖

环检测在 `Dag::from_pipeline()` 时完成（Kahn 排序失败即环存在）。

## 完整示例

```yaml
name: etl_demo

slots:
  - name: source_url
    schema: { type: string }

steps:
  - id: fetch
    type: http
    inputs:
      url: "{slots.source_url}"
      method: GET

  - id: filter_adults
    type: filter
    inputs:
      data: "{fetch.output.body}"
      field: "age"
      operator: "gte"
      value: 18

  - id: enrich
    type: js
    inputs:
      code: |
        function run(data) {
          return data.map(function(record) {
            record.processed = true;
            return record;
          });
        }
      data: "{filter_adults.output}"

  - id: upload
    type: http
    inputs:
      url: "https://api.example.com/ingest"
      method: POST
      body: "{enrich.output}"

output: "{upload.output.status}"
```
