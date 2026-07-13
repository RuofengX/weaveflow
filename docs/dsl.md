# weave DSL 参考

> YAML 声明式管道定义语言。一个 YAML 文件 = 一个可执行的 DAG。

## Pipeline 结构

```yaml
name: my_pipeline          # 必填，唯一标识
description: ...            # 可选

storage:                    # 可选
  snapshot_ttl: 7d
  result_ttl: 30d

slots:                      # Pipeline 级占位符（调用方注入）
  - name: source_url
    schema: { type: string, pattern: "^https?://" }

steps:                      # 必填，至少 1 步
  - id: step_id
    type: operator_name     # 内置算子名，或 "js" 表示内联 JS
    after: [step_a]          # 可选，显式顺序依赖
    iterate:                 # 可选，step 根级
      over: "{upstream.output}"
      as: "item"
      max_workers: 4
      batch:
        size: 100
    inputs:                  # 算子输入
      key: value
    code: |                  # 仅 type="js" 时可用，内联 JS 源码
      function run(input) { ... }
    cache: true
    retry:
      max_attempts: 3
      backoff: exponential
      delay_ms: 1000
    timeout: 60

output: "{step_id.output}"  # 必填
```

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
| `{step_id.output.field}` | 步骤输出字段访问 |
| `{item}` | iterate 元素变量 (scope 内) |

## 步骤定义

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `id` | string | ✓ | — | 唯一标识 |
| `type` | string | ✓ | — | 算子类型（内置名 或 `"js"`） |
| `after` | string[] | — | — | 顺序依赖（不需要数据） |
| `iterate` | object | — | — | 迭代配置（step 根级） |
| `inputs` | object | — | — | 算子输入 |
| `code` | string | — | — | 内联 JS 源码（`type="js"` 时） |
| `cache` | bool | — | 算子默认 | 是否启用缓存 |
| `retry` | object | — | — | 重试策略 |
| `timeout` | u64 | — | — | 超时秒数 |

### retry

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `max_attempts` | u32 | 1 | 最大尝试次数（1 = 不重试） |
| `backoff` | string | fixed | `fixed` / `exponential` |
| `delay_ms` | u64 | 1000 | 基础间隔（毫秒） |

### iterate

`step.iterate` 存在即启用逐元素并行展开：

```yaml
- id: batch_step
  type: http
  iterate:
    over: "{split.output}"   # 变量引用，指向数组
    as: "item"               # 元素变量名
    max_workers: 4           # 省缺 → rayon 自动
    batch:                   # 可选：按数组批次传给算子
      size: 100
  inputs:
    url: "https://api.example.com/ingest"
    body: "{item}"
```

| 字段 | 说明 |
|------|------|
| `over` | 变量引用，指向数组 |
| `as` | 元素变量名 |
| `max_workers` | 并发数（省缺 = rayon 自动） |
| `batch` | 批量模式，`batch.size` 个元素打包传给算子 |

- 无 `batch`：每个元素作为裸对象传给算子（`batch=None`）
- 有 `batch`：按 `size` 切分数组，每批作为数组传给算子

### 内联 JS（`type: js`）

无需预注册，直接在 DSL 中写 JS 源码：

```yaml
- id: custom_filter
  type: js
  code: |
    function run(input) {
      return input.data.filter(function(o) {
        return o.status === 'paid';
      });
    }
  inputs:
    data: "{slots.orders}"
```

- `code` 字段包含 JS 源码
- 必须在顶层定义 `function run(input) { ... }`
- `input` 为 `{ data: ..., ...其他 inputs }`
- QuickJS 沙箱执行，无 fs/net 访问

## 内置算子

| 算子 | 功能 | 状态 |
|------|------|------|
| `http` | HTTP 请求（GET/POST/PUT/DELETE/PATCH） | ✅ |
| `filter` | 按条件过滤数组 (eq/ne/gt/gte/lt/lte/in/contains) | ✅ |
| `sort` | 按字段排序 (asc/desc) | ✅ |
| `dedup` | 按字段去重 | ✅ |
| `merge` | 合并两个对象 (b 覆盖 a) | ✅ |
| `split` | 数组切分为等长 chunks | ✅ |
| `base64` | Base64 编解码 | ✅ |
| `noop` | 直接返回输入（测试用） | ✅ |
| `var` | 变量占位——接受任意 inputs 原样输出 | ✅ |
| `js` | 内联 QuickJS 沙箱（`code` 字段） | ✅ |

## 校验

| 级别 | 说明 | 能否忽略 |
|------|------|---------|
| error | ID 重复、引用不存在、iterate 配置无效、Schema 非法 | ❌ |
| warning | unused_slot、orphan_step | ✅ allow_warnings |

### Error codes

`empty_pipeline_name`, `no_steps`, `empty_step_id`, `duplicate_step_id`,
`empty_slot_name`, `duplicate_slot_name`, `after_self_ref`, `duplicate_after_entry`,
`after_ref_not_found`, `variable_ref_not_found`, `output_ref_not_found`,
`invalid_iterate_config`, `invalid_json_schema`

### Warning codes

`unused_slot`, `orphan_step`

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
    code: |
      function run(input) {
        return input.data.map(function(record) {
          record.processed = true;
          return record;
        });
      }
    inputs:
      data: "{filter_adults.output}"

  - id: upload
    type: http
    inputs:
      url: "https://api.example.com/ingest"
      method: POST
      body: "{enrich.output}"

output: "{upload.output.status}"
```
