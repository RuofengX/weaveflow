use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::Duration;

use crate::operator::OperatorError;

const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

fn reqwest_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            // 不设总超时：执行超时只在 step 层（timeout_sec）显式配置，
            // client 不得隐式截断长耗时请求（慢 LLM 后端、大响应体）。
            // connect_timeout 仅是建连失败的快速失败门槛，不截断已建连的传输。
            .connect_timeout(Duration::from_secs(10))
            // 禁 redirect：防止 302 跳到 169.254.169.254 绕过 block_private_ips 预检。
            // redirect 响应作为正常 3xx 响应返回给调用方。
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build reqwest client")
    })
}

pub fn http_client() -> reqwest::Client {
    reqwest_client().clone()
}

pub fn check_content_length(content_length: Option<u64>) -> Option<()> {
    match content_length {
        Some(len) if len as usize > MAX_RESPONSE_BYTES => None,
        _ => Some(()),
    }
}

pub fn check_body_size(len: usize) -> Result<(), OperatorError> {
    if len > MAX_RESPONSE_BYTES {
        Err(OperatorError::Runtime(
            "response body exceeds 64MB limit".into(),
        ))
    } else {
        Ok(())
    }
}

/// 边读边累计，超限即中断；用于 chunked 等无 Content-Length 的响应，
/// 避免全量读入后才发现超限。
pub async fn read_body_limited(mut resp: reqwest::Response) -> Result<Vec<u8>, OperatorError> {
    let mut buf = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| OperatorError::Runtime(format!("read response body: {e}")))?
    {
        if buf.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(OperatorError::Runtime(
                "response body exceeds 64MB limit".into(),
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

fn split_url(url_str: &str) -> Option<(String, u16)> {
    let url = reqwest::Url::parse(url_str).ok()?;
    let host = url.host_str()?.trim_matches(['[', ']']).to_string();
    let port = url.port_or_known_default()?;
    Some((host, port))
}

pub async fn block_private_ips(url_str: &str) -> Result<(), OperatorError> {
    let (host, port) = split_url(url_str)
        .ok_or_else(|| OperatorError::Config(format!("URL 无法解析或缺少 host: {url_str}")))?;

    let ips: Vec<IpAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![ip],
        Err(_) => {
            let addr_str = format!("{host}:{port}");
            tokio::net::lookup_host(&addr_str)
                .await
                .map_err(|e| OperatorError::Runtime(format!("DNS resolve {host}: {e}")))?
                .map(|sa| sa.ip())
                .collect()
        }
    };
    if ips.is_empty() {
        return Err(OperatorError::Runtime(format!("no IP resolved for {host}")));
    }

    let block_private = std::env::var("WEAVEFLOW_HTTP_BLOCK_PRIVATE")
        .map(|v| v == "1")
        .unwrap_or(false);

    for ip in ips {
        if is_metadata_ip(ip) {
            return Err(OperatorError::Runtime(format!(
                "blocked request to cloud metadata IP {ip}"
            )));
        }
        if block_private && is_private_ip(ip) {
            return Err(OperatorError::Runtime(format!(
                "blocked request to private/internal IP {ip}"
            )));
        }
    }

    Ok(())
}

fn is_metadata_ip(ip: IpAddr) -> bool {
    match normalize_ip(ip) {
        IpAddr::V4(v4) => v4 == std::net::Ipv4Addr::new(169, 254, 169, 254),
        IpAddr::V6(_) => false,
    }
}

/// IPv4-mapped/compatible IPv6（如 ::ffff:169.254.169.254）在 Linux 默认
/// 双栈下实际到达映射的 IPv4 地址 —— 分类前统一归一化，否则绕过全部
/// V4 检查。
fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

fn is_private_ip(ip: IpAddr) -> bool {
    match normalize_ip(ip) {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                // 0.0.0.0 在本机协议栈上等同于 localhost
                || v4.is_unspecified()
                // 100.64.0.0/10 CGNAT（is_shared 尚不稳定，手动判段）
                || (v4.octets()[0] == 100 && (64..=127).contains(&v4.octets()[1]))
                // 198.18.0.0/15 基准测试段
                || (v4.octets()[0] == 198 && (18..=19).contains(&v4.octets()[1]))
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
                || v6.is_unspecified()
        }
    }
}

pub const MAX_STDIO_BYTES: usize = 10 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_ip_detected() {
        assert!(is_metadata_ip(IpAddr::from([169, 254, 169, 254])));
        assert!(is_metadata_ip("::ffff:169.254.169.254".parse().unwrap()));
        assert!(!is_metadata_ip(IpAddr::from([169, 254, 169, 253])));
        assert!(!is_metadata_ip(IpAddr::from([8, 8, 8, 8])));
    }

    #[test]
    fn private_ip_detection() {
        assert!(is_private_ip(IpAddr::from([10, 0, 0, 1])));
        assert!(is_private_ip(IpAddr::from([192, 168, 1, 1])));
        assert!(is_private_ip(IpAddr::from([127, 0, 0, 1])));
        assert!(is_private_ip(IpAddr::from([169, 254, 1, 1])));
        assert!(is_private_ip(IpAddr::from([0, 0, 0, 0])));
        assert!(is_private_ip(IpAddr::from([100, 64, 0, 1])));
        assert!(is_private_ip(IpAddr::from([198, 18, 0, 1])));
        assert!(is_private_ip("::1".parse().unwrap()));
        assert!(is_private_ip("fc00::1".parse().unwrap()));
        // v4-mapped v6 归一化后按 V4 分类
        assert!(is_private_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(is_private_ip("::ffff:10.0.0.1".parse().unwrap()));
        assert!(!is_private_ip("::ffff:8.8.8.8".parse().unwrap()));
        assert!(!is_private_ip(IpAddr::from([8, 8, 8, 8])));
    }

    #[test]
    fn split_url_default_ports() {
        assert_eq!(
            split_url("http://example.com/path"),
            Some(("example.com".into(), 80))
        );
        assert_eq!(
            split_url("https://example.com"),
            Some(("example.com".into(), 443))
        );
        assert_eq!(
            split_url("http://example.com:8080/x"),
            Some(("example.com".into(), 8080))
        );
    }

    #[test]
    fn split_url_userinfo_does_not_spoof_host() {
        assert_eq!(
            split_url("http://user@169.254.169.254/"),
            Some(("169.254.169.254".into(), 80))
        );
    }

    #[test]
    fn split_url_bracketed_ipv6() {
        assert_eq!(split_url("http://[::1]:8080/"), Some(("::1".into(), 8080)));
        assert_eq!(split_url("http://[::1]/"), Some(("::1".into(), 80)));
    }

    #[test]
    fn split_url_invalid_or_hostless() {
        assert_eq!(split_url("not a url"), None);
        assert_eq!(split_url("http://"), None);
    }
}
