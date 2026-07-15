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
| `code` | string | ✓ | — | JS 源码，须顶层定义 `function run(data)` |
| `data` | RefValue | — | — | 传给 JS 的输入数据 |

JS 运行时规范：
- 闭包顶层必须定义 `function run(data) { ... }`，返回可 JSON 序列化的值
- `data` = 输入数据（来自 `data` 字段），`data.data_base64` / `data.data_utf8` = 二进制辅助字段
- 支持 `{{step_id.output}}` 双花括号模板（运行时 resolve）
- 无 `fs`、`net`、`process` 等 Node.js API

```yaml
- id: custom_sort
  type: js
  inputs:
    code: |
      function run(data) {
        return data.sort(function(a, b) {
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

读本地文件或远程 URL，产出 JSON 对象。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `path` | RefValue | — | — | 本地文件路径 |
| `url` | RefValue | — | — | 远程 URL（http/https） |

> 二选一：`path` 或 `url`，不可同时为空。

**输出格式：**
```json
{
  "content": "<base64 编码内容>",
  "mimetype": "text/plain",
  "size": 12345
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `content` | string | 文件内容的 base64 编码 |
| `mimetype` | string | MIME 类型，URL 模式取 Content-Type header，path 模式按扩展名检测，默认 `application/octet-stream` |
| `size` | number | 文件大小（字节） |

```yaml
- id: read_config
  type: file
  inputs:
    path: "./config.json"
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

```
