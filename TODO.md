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
| ~~file 算子含路径变量时返回 null~~ ~~| ~~`builtin/file.rs`, `executor.rs:654-713`~~ | **已修复** — v1.0 resolver 重构后变量解析正确。原报告中的 "快照 output=null" 实际根因是最终输出为二进制 bytes，`serde_json::from_slice` 失败后 fallback 为 Null。现 `runner.rs:331` 已改为 base64 包装。 |
| ~~Slot 默认值不生效~~ | **已修复** — `runner.rs:59-69` 正确应用 `schema.get("default")`，并新增 jsonschema 运行时校验。 |

### 中优先级 — P1

| 项目 | 说明 |
|------|------|
| llm 算子模型能力预检优化 | 已添加 warning + `skip_vision_check` + API error hint。可考虑自动 `/v1/models` 检查（低优先级优化，当前方案已可排查问题）。 |
| ~~`--text-output` 步骤产出为空~~ | **已修复** — `watch.rs:95-108` 已改为从 `status.Completed` 取 output 值。 |
| ~~iterate 加速~~ | **已实现** — `iterate.rs:108-112` 每 10 个 chunk 批量更新 tracker，减少 Mutex 锁竞争。 |
| http 算子的 poll_until | `http.rs` 无 poll_until 功能。规划中的 feature，非缺陷。 |

### 低优先级 — P2

| 项目 | 说明 |
|------|------|
| ~~JSON Schema 输入校验~~ | **已实现** — `runner.rs:82-105` 用 jsonschema crate 校验。 |
| ~~`weave check` 命令~~ | **已实现** — `main.rs:69-74` Check 子命令 + `check_pipeline()`. |
| ~~Snapshot null 掩盖根因~~ | **已实现** — `daemon.rs:326-335` 检测 `_anomalous_null` 并标注 raw_size/raw_hex。 |
| ~~自举 validator~~ | **已实现** — `pipeline.rs` RuleDef + `runner.rs` 执行前逐条运行 rules（支持 js 算子），返回 `{valid, error}`。validator.rs 新增 JS 语法 AOT 校验。 |
| bytecode 缓存 | QuickJS 不支持跨 Runtime 字节码复用（字节码 Runtime-tied）。rquickjs 0.12 不暴露 `JS_WriteObject`/`JS_ReadObject`。当前已有 step 级输入缓存（相同输入不重复执行）。若需优化 iterate 场景 JS 重复编译，需升级 rquickjs 版本或引入 Runtime 池。 |

