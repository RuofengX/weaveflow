# weaveflow

**智能体的上下文外数据引擎**（out-of-context data engine for AI agents）——YAML 声明 DAG，Rust 单二进制确定性执行，MCP 原生接入。

智能体的短板是：上下文窗口装不下批量数据、一次性 tool call 链不可复现、会话结束后工作就断了。weaveflow 补足这三块：

- **上下文外计算**：agent 声明意图（YAML pipeline），bulk 数据在引擎内流过 filter/sort/dedup/merge/js/llm 等 12 个算子，不回上下文
- **可复现、可观测**：每个 step 的输出持久化为快照，重跑命中缓存，agent 按需用 seq 游标逐条取证
- **Routine（智能体值班）**：agent 把「盯着做」的事委托给 daemon（cron 或 stream 微批），终态事件写入持久化收件箱 + 可选 webhook 唤醒——下次会话从 seq 游标增量回查，工作跨会话连续

## 接入方式：MCP（推荐）

```bash
cargo install weaveflow
weaveflow daemon start          # 127.0.0.1:9928
```

在 Claude Code / opencode 等 MCP 客户端注册：

```json
{
  "mcpServers": {
    "weaveflow": { "command": "weaveflow", "args": ["mcp"] }
  }
}
```

17 个 tools：`validate_pipeline` → `apply_pipeline` → `run_pipeline` → `list_snapshots`/`get_snapshot`（默认 token 友好的 summary/截断模式），以及 routine 全家桶（`upsert_routine` / `push_routine` / `get_routine_events` …）。

## Routine：智能体值班范式

```bash
# agent 委托：每 5 分钟跑一遍 pipeline
weaveflow routine apply -f watch.toml     # type = "cron" / "stream"
# …agent 会话结束，daemon 继续值班…
# 下次醒来：增量回查收件箱
weaveflow routine events watch --after 12 --output json
```

routine 产生的每个任务终态都会落成事件（`task_completed`/`task_failed`，附输出截断预览）。配置 `notify.webhook_url` 后还会主动 POST 事件——harness 类软件收到 webhook 即可唤醒对应智能体。

## Pipeline 示例

```yaml
name: etl_demo

slots:
  - name: source_url
    schema: { type: string }

steps:
  - id: fetch
    type: http
    timeout_sec: 30
    inputs:
      url: "{slots.source_url}"
      method: GET

  - id: adults
    type: filter
    inputs:
      data: "{fetch.output.body}"
      field: "age"
      operator: "gte"
      value: 18

output: "{adults.output}"
```

- **steps 组成 DAG**：`{step_id.output}` 引用即依赖，同层并行执行
- **iterate**：数组逐元素并行展开（`over: "{...}"`，当前元素绑定 `as` 名，`{item...}` 在 inputs 任意字段解析）
- **retry / timeout_sec / cache**：step 根级声明，逐次尝试生效
- **12 个内置算子**：http / js / filter / sort / dedup / merge / base64 / noop / var / file / command / llm
- **js 算子**：`inputs.code` 内联 QuickJS 沙箱，无 fs/net，`timeout_sec` 可真中断死循环

## CLI 快速开始

```bash
cargo install --path .

weaveflow daemon start                          # 127.0.0.1:9928
weaveflow check -f pipeline.yml                 # 本地校验（无需 daemon）
weaveflow pipeline apply -f pipeline.yml        # 注册
weaveflow run etl_demo -i source_url=<url>      # 运行
weaveflow run etl_demo --watch                  # TUI 实时进度
weaveflow run etl_demo --text-output --output json   # JSONL 流（CI/Agent）
weaveflow task show <task-id>                   # summary 模式（省 token）
weaveflow task snapshot list <task-id>          # 回放每步输出
weaveflow daemon stop
```

## 文档

| 文档 | 内容 |
|------|------|
| [docs/guide.md](docs/guide.md) | 使用手册：CLI 全参考、daemon 生命周期、故障排查 |
| [docs/dsl.md](docs/dsl.md) | DSL 字段级参考 |
| [docs/operators.md](docs/operators.md) | 12 个算子的输入/输出/限制 |
| [docs/api.md](docs/api.md) | HTTP/WS API 参考（请求/响应 schema） |
| [docs/agent.md](docs/agent.md) | Agent 集成指南（MCP、JSONL 流、routine 值班模式） |
| [docs/architecture.md](docs/architecture.md) | 架构设计（引擎语义、存储、并发） |
| [docs/security.md](docs/security.md) | 安全模型（**无鉴权，仅限 localhost**） |

导航入口：[docs/README.md](docs/README.md)。贡献者约定：[AGENTS.md](AGENTS.md)。

## 开发

```bash
cargo build
cargo test --lib          # 227 单元测试
cargo test --test '*'     # 集成测试（in-process，无需 daemon）
cargo bench --bench '*'   # 5 ETL benches
cargo clippy --all-targets   # 保持 0 warning
```

## 安全提示

daemon 所有端点**无鉴权**：绑定非 loopback 地址 = 未授权 RCE。仅在 localhost 使用，详见 [docs/security.md](docs/security.md)。

## License

AGPL-3.0-only，见 [LICENSE](LICENSE)。
