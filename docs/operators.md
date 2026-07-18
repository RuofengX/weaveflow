# weave 算子配置参考

## http — HTTP 请求

发送 HTTP 请求，fetch + 返回响应体。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `url` | RefValue | ✓ | — | 请求 URL，支持 `{...}` 变量引用 |
| `method` | string | — | `"GET"` | HTTP 方法：`GET`/`POST`/`PUT`/`DELETE`，其他值报 Config 错误 |
| `headers` | `{string: RefValue}` | — | — | 请求头键值对 |
| `body` | RefValue | — | — | 请求体（字符串、对象或 `{...}` 引用；GET/DELETE 忽略 body） |

共享 HTTP client 的固定行为：**不跟随 redirect**（3xx 响应原样返回 `status`/`body`，不会跳转）；SSRF 预检对 DNS 解析出的**全部** IP 逐一检查（169.254.169.254 始终拒绝，私网地址需 `WEAVE_HTTP_BLOCK_PRIVATE=1` 才拒绝）；响应体边读边累计，超过 **64MB** 即中断报错；总超时 60s、连接超时 10s。

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

在 QuickJS 沙箱中执行 JS 代码。不注册即可写自定义逻辑。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `code` | RefValue | ✓ | — | JS 源码字符串或 `{step_id.output}` 引用 |
| `data` | RefValue | — | — | 传给 JS 的输入数据 |

JS 超时由 step 层 `timeout_sec` 统一控制（inputs **没有** timeout 字段，写了会报 unknown field 错误）；step 超时 drop 执行 future，经 drop-guard 触发 QuickJS interrupt handler 强制终止执行（包括 `while(1){}` 死循环）。不设置 `timeout_sec` 时，死循环会一直占用一个 blocking 线程——请始终为 js 步骤配置 `timeout_sec`。

JS 运行时规范：
- 顶层必须定义 `function run(data) { ... }`，返回可 JSON 序列化的值
- `data` = 输入数据（来自 `data` 字段）
- 无 `fs`、`net`、`process` 等 Node.js API

```yaml
# 内联 JS 代码
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

# 从上游步骤引用 JS 代码
- id: code_builder
  type: base64
  inputs:
    data: "{file_read.output.content}"
    mode: decode
- id: run_code
  type: js
  inputs:
    code: "{code_builder.output}"
    data: "{data_step.output}"
```

---

## filter — 按条件过滤数组

对数组按字段条件过滤，rayon 并行。

支持的 `operator`：`eq` `ne` `gt` `gte` `lt` `lte` `in` `contains`。其他值在 validator 校验阶段报错（`invalid_operator_config`），绕过校验运行时返回 Config 错误。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入数组，支持 `{...}` 引用；缺失或为 null 时报 Config 错误 |
| `field` | string | — | `""` | 比较的字段名（空 = 直接比较元素自身）；数字路径段可索引数组 |
| `operator` | string | — | `"eq"` | 比较运算符，枚举值见上 |
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

`order` 仅支持 `asc` / `desc`；其他值在 validator 校验阶段报错（`invalid_operator_config`），绕过校验运行时返回 Config 错误。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入数组；缺失或为 null 时报 Config 错误 |
| `field` | string | — | `""` | 排序字段名；数字路径段可索引数组 |
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
| `data` | RefValue | — | — | 输入数组；缺失或为 null 时报 Config 错误 |
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
| `deep` | bool | — | `false` | 深合并：递归合并嵌套对象；默认浅合并（b 的同名字段整体覆盖 a）。数组与标量不做合并，始终由 b 覆盖 |

```yaml
- id: merge_data
  type: merge
  inputs:
    a: "{step1.output}"
    b: "{step2.output}"
    deep: true
```

---

## base64 — 编解码

字符串 ↔ Base64 转换。

| 输入 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `data` | RefValue | — | — | 输入字符串或 bytes；缺失或为 null 时报 Config 错误 |
| `mode` | string | — | `"encode"` | `encode` / `decode`，其他值报 Config 错误 |

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

本地路径经 canonicalize 后按 `WEAVE_FILE_ALLOW_ROOTS` 白名单（冒号分隔的根目录列表）校验：

- **未配置**：放行所有路径，且进程内只打一次 warn（`Once`）建议配置白名单
- **含空段**（如 `/tmp::/var` 或首尾冒号）：空段被忽略并打 warn 计数
- **过滤空段后为空**（如 `:::`）：视为配置有误，拒绝所有路径并打 warn
- 路径不在任一白名单根下 → Runtime 错误

本地文件超过 64MB 报错；URL 模式走共享 HTTP client（SSRF 检查、不跟 redirect、64MB 流式限长）。

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

stdout / stderr 各有 10MB 上限：超过上限的输出会被截断（仅保留前 10MB，超出部分读取后丢弃，子进程可正常退出），对应流追加 `[weave: ... truncated at 10MB]` 标记，且输出 JSON 的 `truncated` 字段为 `true`。

子进程以 `env_clear` + 最小环境白名单（`PATH`/`HOME`/`LANG`/`LC_ALL`/`TZ`）启动，并启用 `kill_on_drop`：step 超时取消算子 future 时子进程会被回收。注意 `yes` 这类无限输出命令不会自行停止，会一直运行到 step `timeout_sec` 触发为止。

输出 JSON：`stdout` / `stderr` / `exit_code` / `success` / `truncated`。

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
