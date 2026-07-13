# weave — 实施路线图

## 已完成 ✅

- [x] DSL 解析层：schema / parser / validator
- [x] 语义 Key 类型：PipelineId / TaskId / SnapshotKey / ObjectDigest / CacheKey
- [x] 存储层：Database 门面 + 五表 (PIPELINE/TASK/SNAPSHOT/OBJECT/CACHE) + Prune
- [x] WeaveError 统一错误枚举 (thiserror) + Axum IntoResponse
- [x] FlatBuffer Scope：零拷贝 + memcpy clone
- [x] TaskMeta + Snapshot + Progress/TaskStatus 状态机
- [x] Operator trait + 12 个内置算子（见下）
- [x] DAG：Kahn 拓扑排序 + 环检测 + 隐式依赖 (inputs refs + after)
- [x] Executor：逐层并行 / 缓存 / 重试 / iterate 并行 / 错误传播
- [x] TaskTracker：in-memory 运行时状态 + broadcast 实时推送
- [x] weave daemon (Axum)：REST + WebSocket
- [x] weave CLI：pipeline / run / task / system + `--watch` TUI + `--text-output`
- [x] QuickJS 运行时：rquickjs 内嵌，每次调用独立 Runtime，无 fs/net
- [x] 集成测试 (20 个场景) + ETL benchmark
- [x] `read_file` 算子：本地文件 + URL 读取 (`operator/builtin/file.rs`)
- [x] `command` 算子：Shell 执行 (`operator/builtin/command.rs`)
- [x] `llm` 算子：OpenAI 兼容 API + images_b64 多模态 (`operator/builtin/llm.rs`)
- [x] `pipeline apply` upsert：同名幂等更新
- [x] `pipeline delete` 按名删除
- [x] `-i key=@/path/to/file` 文件输入
- [x] `weave prune` 清理命令
- [x] JS 算子错误栈 (`message` + `stack`)
- [x] 快照二进制数据 base64 fallback 显示 (`daemon.rs:get_snapshot_by_seq`)
- [x] `weave system operators` 算子列表

## 内置算子总览

| 算子 | 文件 | 说明 |
|------|------|------|
| `noop` | `builtin/noop.rs` | 直接透传 |
| `filter` | `builtin/filter.rs` | 数组过滤（rayon 并行） |
| `sort` | `builtin/sort.rs` | 数组排序（rayon 并行） |
| `dedup` | `builtin/dedup.rs` | 数组去重 |
| `merge` | `builtin/merge.rs` | 合并两个对象 |
| `split` | `builtin/split.rs` | 数组切分 |
| `base64` | `builtin/base64.rs` | Base64 编解码 |
| `http` | `builtin/http.rs` | HTTP 请求 |
| `file` | `builtin/file.rs` | 本地文件 / URL 读取 |
| `command` | `builtin/command.rs` | Shell 命令执行 |
| `llm` | `builtin/llm.rs` | OpenAI 兼容 LLM + 多模态图片 |
| `var` | `builtin/var.rs` | 变量占位 |
| `js` | `builtin/js.rs` | 内联 QuickJS 沙箱 |

## 待实现

### 高优先级 — P0 Bug

| 项目 | 文件 | 说明 |
|------|------|------|
| file 算子含路径变量时返回 null | `builtin/file.rs`, `executor.rs:654-713` | 实际测试：10MB PDF，快照 seq=1 output=null。根因为 `resolve_inputs` 中 `k=="path"`（非 `data` key）走 JSON parse 分支，UTF-8 fallback 可能丢失路径；或 `{slots.pdf_path}` 未解析直接作为字面量路径传给 file 算子 |
| Slot 默认值不生效 | `executor.rs:58-65` | `weave run -i pdf_path=...` 不传其他有 default 的 slot 时，llm 算子报"缺少 url/model"。`schema.get("default")` 逻辑需加 trace 日志验证 |

### 中优先级 — P1

| 项目 | 说明 |
|------|------|
| llm 算子缺少模型能力预检 | 对纯文本模型（如 Gemma-4）发送 `images_b64` 时无声失败（LLM 回复"无法访问文件"）。应调 `/v1/models` 或接收端检查是否支持 vision，不支持时显式报错 |
| `--text-output` 步骤产出为空 | `watch.rs:run_text` 走 `data.get("steps").and_then(...)` 取 output 字段，但 TaskSnapshot 中 steps 不含 output（output 在 snapshot 中）。需从 tracker 中取最终 output 值 |
| iterate 加速 | `batch.size=1` 场景 Mutex 锁开销大，考虑 `std::sync::Mutex` 或连接池 |
| http 算子的 poll_until 测试 | 轮询模式需 mock HTTP server 测试 |

### 低优先级 — P2

| 项目 | 说明 |
|------|------|
| JSON Schema 输入校验 | 运行时校验 slots 值是否符合 schema |
| `weave check` 命令 | 本地 YAML 校验（无需 daemon） |
| fork / join 算子 | 多路分发 + 汇总 |
| 自举 validator | 用 weave DSL 表达自定义校验规则 |
| bytecode 缓存 | JS 算子字节码缓存到 Object 表 |
| Snapshot null 掩盖根因 | `daemon.rs:324-333` 基 base64 fallback 已存在，但实际快照为 JSON `null` 说明存的就不是二进制数据，可能是 fail 后未清理的空快照。应在 snapshots 为 JSON null 时标记为异常状态 |

## 2026-07-13 PDF OCR 实战踩坑记录

使用 weave + spark:8000 (Gemma-4) 对 10MB 判决书 PDF 做 OCR 时发现的问题，已映射到上表：

1. **file 算子返回 null**（P0）→ 快照 output=null，后续 llm 算子收到 null base64 → LLM 回复"无法访问文件"
2. **Slot 默认值不生效**（P0）→ 只传 pdf_path 时报"缺少 url/model"
3. **Gemma-4 无视觉能力**（P1）→ 该端点只有一个文本模型，不支持 image_url 多模态输入
4. **`--text-output` 无步骤产出**（P1）→ 只显示 `completed in 8581ms`，无任何 output
