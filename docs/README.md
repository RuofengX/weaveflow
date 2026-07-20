# weaveflow 文档中心

> YAML 声明式 DAG 批处理引擎 — 单二进制 daemon + CLI + 内嵌存储 + QuickJS 沙箱。

## 按角色选路

| 你是谁 | 推荐阅读顺序 |
|--------|--------------|
| **第一次使用** | [guide.md](guide.md) 快速开始 → [dsl.md](dsl.md) 写第一个 pipeline → [operators.md](operators.md) 查算子字段 |
| **调用方 / Agent 开发者** | [agent.md](agent.md) 集成模式 → [api.md](api.md) 接口细节 → [security.md](security.md) 边界 |
| **贡献者 / 维护者** | [architecture.md](architecture.md) 内部设计 → 根目录 [AGENTS.md](../AGENTS.md) 工程约定 → [TODO.md](../TODO.md) 审计台账 |

## 文档清单

| 文档 | 内容 | 什么时候查 |
|------|------|-----------|
| [guide.md](guide.md) | **使用手册**：安装、快速开始、CLI 全参考、daemon 生命周期、变量模型踩坑、故障排查 | 日常使用的第一入口 |
| [dsl.md](dsl.md) | **DSL 参考**：pipeline/slots/steps/iterate/retry/timeout 每个字段，校验规则，error/warning codes | 写 YAML 时逐字段核对 |
| [operators.md](operators.md) | **算子参考**：12 个内置算子的输入字段、默认值、输出格式、限制（64MB/10MB 等） | 配置某个 step 的 inputs 时 |
| [api.md](api.md) | **HTTP/WS API 参考**：每个端点的请求/响应 JSON schema、curl 示例、错误格式、TaskSnapshot 结构 | 不用 CLI、直接编程调用 daemon 时 |
| [agent.md](agent.md) | **Agent 集成指南**：JSON 输出模式、JSONL 进度流、取结果/快照回放的推荐流程、常见坑 | 让 LLM/脚本驱动 weaveflow 时 |
| [architecture.md](architecture.md) | **架构设计**：进程模型、请求生命周期、引擎执行语义、存储层、并发模型、关闭流程 | 改代码前理解内部行为 |
| [security.md](security.md) | **安全模型**：无鉴权现状、SSRF/沙箱/命令执行边界、env 脱敏、加固建议 | 部署前必读 |

## 一致性约定

- 文档中的所有行为描述以 **源码 + 集成测试** 为准；发现文档与行为不符时以代码为准并修文档。
- DSL 字段级事实只写在 [dsl.md](dsl.md) / [operators.md](operators.md)，其他文档引用而不复制。
- 已知的未修复问题（审计遗留）不在文档中展开，统一记录于 [TODO.md](../TODO.md)。
