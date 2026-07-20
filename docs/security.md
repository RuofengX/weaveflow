# weaveflow 安全模型

> 部署前必读。weaveflow 的信任假设是 **daemon 只面向 localhost 的可信调用方**。

## 鉴权现状：没有鉴权

所有 HTTP/WS 端点（`/runs`、`/pipelines`、`/prune` …）**没有任何身份验证**（设计决策：鉴权属网关/反代层职责，见 TODO.md"有意保留"）。

- `--allow-remote` 只是允许绑定非 loopback 地址，**不是**鉴权机制。
- 绑定 `0.0.0.0` = 未授权 RCE：任何人都能注册含 `command`/`file` 算子的 pipeline 并运行。
- 即使只绑 localhost，也存在**浏览器 CSRF** 风险：`POST /pipelines` + `POST /runs` 都是 simple request（无 preflight），恶意网页可在用户浏览器里驱动本机 daemon。

**结论：仅在受信本机环境运行；不要把端口暴露给任何网络。** 如需远程使用，在前面架一层带鉴权的反向代理。

## 算子级边界

### `command` — Shell 执行

- `sh -c` 启动，`env_clear` + 最小环境白名单（`PATH`/`HOME`/`LANG`/`LC_ALL`/`TZ`），宿主其他环境变量不泄露给子进程。
- stdout/stderr 各 10MB 上限，超出截断并标记 `truncated: true`（继续排空管道，子进程可正常退出）。
- `kill_on_drop`：step 超时取消算子 future 时子进程被回收。
- **无命令内容审查**——能跑 weaveflow 任务的人等价于有 shell，这是设计前提而非漏洞。

### `js` — QuickJS 沙箱

- 无 `fs`/`net`/`process` 等宿主 API；256MB 内存上限 + 1MB 栈上限。
- `__native__.inflate` 解压结果在 Rust 侧截断到 256MB（压缩炸弹无法绕过内存上限）。
- step `timeout_sec` 经 drop-guard 触发 interrupt handler，可真中断 `while(1){}`。
- **不配 `timeout_sec` 的死循环会永久占用一个 blocking 线程**（设计决定：超时只存在于 step 层）。

### `file` — 文件读取

- 本地路径 canonicalize 后按 `WEAVEFLOW_FILE_ALLOW_ROOTS`（冒号分隔根目录列表）做前缀校验；白名单的根同样 canonicalize。
- 未配置白名单：放行全部路径，进程内打一次 warn 建议配置。
- 单文件 64MB 上限。已知残余：检查与读取之间的 TOCTOU（见 TODO.md）。

### `http` / `file`（URL 模式）— 共享 HTTP client

- 不跟随 redirect（3xx 原样返回，杜绝跳转型 SSRF 绕过）。
- SSRF 预检：对 DNS 解析出的**全部** IP 逐一分类；`169.254.169.254`（云元数据）始终拒绝；IPv4-mapped IPv6 先归一化再分类；`WEAVEFLOW_HTTP_BLOCK_PRIVATE=1` 时额外拒绝私网、`0.0.0.0`、CGNAT `100.64/10`、`198.18/15`。
- 响应体边读边累计，64MB 上限。
- 无隐式总超时（执行超时只由 step `timeout_sec` 控制）；保留 10s connect timeout 作为建连快速失败下限。
- **已知残余：DNS rebinding TOCTOU**——预检与 reqwest 建连各自独立解析 DNS，低 TTL 恶意域名理论上可"检查时合法、连接时指向被封 IP"（共享 client 无法做 resolve pinning）。

## 数据保护

- **env 脱敏**：step inputs 中引用的 `{env.KEY}` 的值会被记录到 redact 集合，写入持久化快照前替换，避免密钥落盘。
- **错误信息**：5xx 响应对外只有固定文案 `"internal server error"`，内部细节只进 daemon 日志；400/404 会带具体信息（面向调用方排障，属有意设计）。
- **持久化**：redb 文件位于 `WEAVEFLOW_DATA`（默认 `~/.weaveflow`），权限继承文件系统；pipeline 的 slot 值与 step 输出都会落盘，敏感数据注意 `result_ttl` + `system prune`。

## 加固清单

| 场景 | 建议 |
|------|------|
| 任何部署 | 保持默认 `127.0.0.1` 绑定；不要 `--allow-remote` |
| 需要远程调用 | 反向代理终结 TLS + 鉴权，再转发到 loopback |
| 多用户机器 | `WEAVEFLOW_FILE_ALLOW_ROOTS` 限定 `file` 根；`WEAVEFLOW_HTTP_BLOCK_PRIVATE=1` 封私网 |
| 跑不可信 pipeline | 不要。无多租户隔离，`command`/`file`/`http` 等价于代码执行 |
| 密钥使用 | 走 `{env.KEY}` 引用（快照脱敏），不要写死在 YAML 或 slot 值里 |
