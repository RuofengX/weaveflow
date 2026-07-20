//! 统一运行配置层（中间件）：CLI 参数与环境变量在此汇合为一份 `CliConfig`。
//!
//! 优先级：CLI 参数 > 环境变量（WEAVEFLOW_*）> 内置默认值。
//! 优先级本身由 clap 的 `env` 特性实现，本模块负责承载解析后的结果、
//! 提供基于配置的 HTTP client 构造与输出渲染。

use std::time::Duration;

pub const DEFAULT_BIND: &str = "127.0.0.1:9928";

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// 人类可读（pretty JSON / 格式化行）
    Text,
    /// 机器可读（紧凑单行 JSON，面向 Agent / jq）
    Json,
}

impl OutputFormat {
    pub fn is_json(self) -> bool {
        matches!(self, OutputFormat::Json)
    }
}

/// 解析 "30s" / "500ms" / "5m" / "1h" / 裸数字（秒）为 Duration。
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let (num, mult_ms) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (s, 1_000)
    };
    let v: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid duration {s:?} (expect e.g. 500ms, 30s, 5m, 1h)"))?;
    let ms = v
        .checked_mul(mult_ms)
        .ok_or_else(|| format!("duration overflow: {s:?}"))?;
    Ok(Duration::from_millis(ms))
}

/// CLI 客户端统一运行配置。
#[derive(Clone, Debug)]
pub struct CliConfig {
    /// Daemon 地址（http:// 前缀可选，末尾 / 会被裁剪）
    pub daemon: String,
    /// 输出格式
    pub output: OutputFormat,
    /// 普通 HTTP 请求总超时
    pub http_timeout: Duration,
    /// TCP 连接超时
    pub connect_timeout: Duration,
    /// WebSocket 连接超时（run --watch）
    pub ws_timeout: Duration,
    /// prune 专用总超时（全表扫描 + compact 可能很慢）
    pub prune_timeout: Duration,
    /// daemon log 单次拉取超时
    pub log_timeout: Duration,
    /// daemon log -f 轮询间隔
    pub log_poll: Duration,
}

impl CliConfig {
    pub fn http_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(self.http_timeout)
            .connect_timeout(self.connect_timeout)
            .build()
            .expect("build CLI reqwest client")
    }

    pub fn prune_client(&self) -> Result<reqwest::Client, String> {
        reqwest::Client::builder()
            .timeout(self.prune_timeout)
            .connect_timeout(self.connect_timeout)
            .build()
            .map_err(|e| format!("构建 HTTP client 失败: {e}"))
    }

    /// 按输出格式打印一个 JSON 响应：text = pretty，json = 紧凑单行。
    pub fn print_json(&self, v: &serde_json::Value) {
        let s = match self.output {
            OutputFormat::Text => serde_json::to_string_pretty(v).unwrap_or_default(),
            OutputFormat::Json => serde_json::to_string(v).unwrap_or_default(),
        };
        println!("{s}");
    }

    pub fn is_json(&self) -> bool {
        self.output.is_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("42").unwrap(), Duration::from_secs(42));
        assert_eq!(parse_duration(" 10s ").unwrap(), Duration::from_secs(10));
    }

    #[test]
    fn parse_duration_rejects_invalid() {
        for s in ["", "abc", "10x", "s", "1.5s", "-3s"] {
            assert!(parse_duration(s).is_err(), "input {s:?}");
        }
    }

    #[test]
    fn parse_duration_overflow() {
        assert!(parse_duration(&format!("{}h", u64::MAX)).is_err());
    }
}
