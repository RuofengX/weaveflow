use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::Duration;

use crate::operator::OperatorError;

const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

fn reqwest_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .connect_timeout(Duration::from_secs(10))
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

fn split_url(url_str: &str) -> Option<(&str, &str, u16)> {
    let after_scheme = url_str.split("://").nth(1).unwrap_or(url_str);
    let host_port_path = after_scheme.split('/').next().unwrap_or(after_scheme);
    let (host_str, port) = if let Some((h, p)) = host_port_path.rsplit_once(':') {
        let port = p.parse::<u16>().ok()?;
        (h, port)
    } else {
        let scheme = url_str.split("://").next().unwrap_or("http");
        let default_port = if scheme == "https" { 443 } else { 80 };
        (host_port_path, default_port)
    };
    let host = if host_str.starts_with('[') && host_str.ends_with(']') {
        &host_str[1..host_str.len() - 1]
    } else {
        host_str
    };
    if host.is_empty() {
        return None;
    }
    Some((host, host, port))
}

pub async fn block_private_ips(url_str: &str) -> Result<(), OperatorError> {
    let (host, host_for_dns, port) = split_url(url_str)
        .ok_or_else(|| OperatorError::Config("URL missing host".into()))?;

    let ip = match host.parse::<IpAddr>() {
        Ok(ip) => ip,
        Err(_) => {
            let addr_str = format!("{host_for_dns}:{port}");
            let addrs: Vec<_> = tokio::net::lookup_host(&addr_str)
                .await
                .map_err(|e| {
                    OperatorError::Runtime(format!("DNS resolve {host_for_dns}: {e}"))
                })?
                .collect();
            addrs
                .first()
                .map(|sa| sa.ip())
                .ok_or_else(|| {
                    OperatorError::Runtime(format!("no IP resolved for {host_for_dns}"))
                })?
        }
    };

    if is_metadata_ip(ip) {
        return Err(OperatorError::Runtime(format!(
            "blocked request to cloud metadata IP {ip}"
        )));
    }

    let block_private = std::env::var("WEAVE_HTTP_BLOCK_PRIVATE")
        .map(|v| v == "1")
        .unwrap_or(false);
    if block_private && is_private_ip(ip) {
        return Err(OperatorError::Runtime(format!(
            "blocked request to private/internal IP {ip}"
        )));
    }

    Ok(())
}

fn is_metadata_ip(ip: IpAddr) -> bool {
    ip == IpAddr::from([169, 254, 169, 254])
}

fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_multicast()
        }
    }
}

pub const MAX_STDIO_BYTES: usize = 10 * 1024 * 1024;
