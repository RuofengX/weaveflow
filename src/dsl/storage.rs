use serde::{Deserialize, Serialize};

/// 人类可读的 TTL（生存时间）值，从 `"30s"`、`"5m"`、`"2h"`、`"7d"` 等字符串解析。
///
/// 内部存储为 `chrono::TimeDelta`。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ttl(pub chrono::TimeDelta);

impl serde::Serialize for Ttl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let secs = self.0.num_seconds();
        let s = if secs % 86400 == 0 {
            format!("{}d", secs / 86400)
        } else if secs % 3600 == 0 {
            format!("{}h", secs / 3600)
        } else if secs % 60 == 0 {
            format!("{}m", secs / 60)
        } else {
            format!("{}s", secs)
        };
        serializer.serialize_str(&s)
    }
}

impl<'de> serde::Deserialize<'de> for Ttl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.len() < 2 {
            return Err(serde::de::Error::custom(format!("无效: {}", s)));
        }
        let (num_str, unit) = s.split_at(s.len() - 1);
        let num: i64 = num_str.parse().map_err(serde::de::Error::custom)?;
        match unit {
            "s" => Ok(Ttl(chrono::TimeDelta::seconds(num))),
            "m" => Ok(Ttl(chrono::TimeDelta::minutes(num))),
            "h" => Ok(Ttl(chrono::TimeDelta::hours(num))),
            "d" => Ok(Ttl(chrono::TimeDelta::days(num))),
            _ => Err(serde::de::Error::custom(format!("无效单位: {}", unit))),
        }
    }
}

/// 存储 TTL 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDef {
    pub snapshot_ttl: Option<Ttl>,
    pub result_ttl: Option<Ttl>,
}
