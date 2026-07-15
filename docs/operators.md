# weave 算子配置参考

## http — HTTP 请求

发送 HTTP 请求，fetch + 返回响应体。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `url` | RefValue | ✓ | — | 请求 URL，支持 `{...}` 变量引用 |
| `method` | string | — | `"GET"` | HTTP 方法：`GET`/`POST`/`PUT`/`DELETE`/`PATCH` |
| `headers` | `{string: RefValue}` | — | — | 请求头键值对 |
| `body` | RefValue | — | — | 请求体（字符串、对象或 `{...}` 引用） |

```yaml
- id: fetch_data
  type: http
  inputs:
    url: "{slots.source_url}"
    method: GET
    headers:
      Authorization: "Bearer {env.API_KEY}"
```

---

## js — 内联 QuickJS 沙箱

在 QuickJS 沙箱中执行内联 JS 代码。不注册即可写自定义逻辑。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `code` | string | ✓ | — | JS 源码，须顶层定义 `function run(input)` |
| `data` | RefValue | — | — | 传给 JS 的输入数据 `input.data` |

JS 运行时规范：
- 闭包顶层必须定义 `function run(input) { ... }`，返回可 JSON 序列化的值
- `input.data` = `data` 输入，`input.<其他inputs>` = 其他的输入字段
- 支持 `{{step_id.output}}` 双花括号模板（运行时 resolve）
- 无 `fs`、`net`、`process` 等 Node.js API

```yaml
- id: custom_sort
  type: js
  inputs:
    code: |
      function run(input) {
        return input.data.sort(function(a, b) {
          return a.score - b.score;
        });
      }
    data: "{upstream.output}"
```

---

## filter — 按条件过滤数组

对数组按字段条件过滤，rayon 并行。

支持的 `operator`：`eq` `ne` `gt` `gte` `lt` `lte` `in` `contains`

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入数组，支持 `{...}` 引用 |
| `field` | string | — | `""` | 比较的字段名（空 = 直接比较元素自身） |
| `operator` | string | — | `"eq"` | 比较运算符 |
| `value` | RefValue | — | — | 比较值 |

```yaml
- id: filter_adults
  type: filter
  inputs:
    data: "{fetch.output.body}"
    field: "age"
    operator: "gte"
    value: 18
```

---

## sort — 按字段排序数组

对数组排序，rayon 并行。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入数组 |
| `field` | string | — | `""` | 排序字段名 |
| `order` | string | — | `"asc"` | `asc` / `desc` |

```yaml
- id: sort_by_score
  type: sort
  inputs:
    data: "{filter.output}"
    field: "score"
    order: "desc"
```

---

## dedup — 按字段去重数组

对数组按字段去重，保留先出现的元素。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入数组 |
| `field` | string | — | `""` | 去重字段名（空 = 直接比较元素自身） |

```yaml
- id: dedup_by_id
  type: dedup
  inputs:
    data: "{sort.output}"
    field: "id"
```

---

## merge — 合并两个对象

将两个对象合并，`b` 的字段覆盖 `a` 的同名字段。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `a` | RefValue | — | — | 基础对象 |
| `b` | RefValue | ✓ | — | 覆盖对象 |

```yaml
- id: merge_data
  type: merge
  inputs:
    a: "{step1.output}"
    b: "{step2.output}"
```

---

## split — 数组切分为 chunks

将数组等分切块，用于后续 batch 处理。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入数组 |
| `size` | u32 | — | `100` | 每 chunk 最大元素数 |

```yaml
- id: split_batches
  type: split
  inputs:
    data: "{source.output}"
    size: 50
```

---

## base64 — 编解码

字符串 ↔ Base64 转换。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入字符串或 bytes |
| `mode` | string | — | `"encode"` | `encode` / `decode` |

```yaml
- id: encode_pdf
  type: base64
  inputs:
    data: "{read_pdf.output}"
    mode: "encode"
```

---

## noop — 直接透传

不做任何处理，输入原样返回。测试和占位用，无 `inputs`。

```yaml
- id: placeholder
  type: noop
```

---

## var — 变量占位

将任意 inputs 直接输出，不做加工。用于在 DAG 中"声明"一个变量供下游引用。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `value` | RefValue | — | — | 任意 JSON 值或 `{...}` 引用 |

```yaml
- id: config
  type: var
  inputs:
    value:
      timeout: 30
      retries: 3
```

---

## file — 读取文件

读本地文件或远程 URL，产出原始 bytes。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `path` | RefValue | — | — | 本地文件路径（file:// 前缀） |
| `url` | RefValue | — | — | 远程 URL（http/https） |

> 二选一：`path` 或 `url`，不可同时为空。

> 二进制文件 >10MB 在 JS 算子中返回 null（quickjs 限制），请先用 `base64` 算子 encode。

```yaml
- id: read_config
  type: file
  inputs:
    path: "file:///etc/config.json"
```

---

## command — 执行 Shell 命令

执行 shell 命令并捕获 stdout。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `command` | RefValue | ✓ | — | 要执行的命令 |
| `shell` | string | — | `"sh"` | shell 解释器：`sh` / `bash` |
| `stdin` | RefValue | — | — | 传入 stdin 的数据 |

```yaml
- id: run_script
  type: command
  inputs:
    command: "python3 transform.py"
    shell: "bash"
    stdin: "{upstream.output}"
```

---

## llm — LLM 推理

调用 OpenAI 兼容的 LLM API，返回文本。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `url` | RefValue | ✓ | — | API endpoint |
| `model` | string | ✓ | — | 模型名 |
| `prompt` | RefValue | ✓ | — | 用户 prompt |
| `system` | RefValue | — | — | 系统 prompt |
| `images_b64` | RefValue | — | — | Base64 图片数组（多模态） |
| `image_type` | string | — | `"png"` | 图片 MIME 类型 |
| `max_tokens` | u64 | — | `4096` | 最大输出 token |
| `temperature` | f64 | — | — | 采样温度 |
| `skip_vision_check` | bool | — | — | 跳过视觉模型检测 |

```yaml
- id: summarize
  type: llm
  inputs:
    url: "https://api.openai.com/v1/chat/completions"
    model: "gpt-4o"
    prompt: "{extract.output.text}"
    system: "你是专业文档分析师"
    max_tokens: 2048
    temperature: 0.3
```

---

## fork — 并行多路分发

将一个步骤的输出分发到多个并行分支，各分支独立执行后聚合结果。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `branches` | `[{id, type, inputs}]` | ✓ | — | 分支配置数组 |
| `join` | string | — | `"object"` | 聚合模式：`object`（按分支 id 合并）/ `array`（拼接为数组） |

### 分支 (branch) 结构

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | — | 分支标识（省缺 = `branch_N`） |
| `type` | string | ✓ | 算子类型 |
| `inputs` | object | — | 算子输入（支持 `{...}` 引用） |

```yaml
- id: process
  type: fork
  inputs:
    branches:
      - id: stats
        type: js
        inputs:
          code: |
            function run(input) { return { count: input.data.length }; }
          data: "{source.output}"
      - id: sample
        type: js
        inputs:
          code: |
            function run(input) { return { first_10: input.data.slice(0, 10) }; }
          data: "{source.output}"
    join: "object"
```

`join: "object"` 输出：
```json
{ "stats": ..., "sample": ... }
```

`join: "array"` 输出：
```json
[{...}, {...}]
```
