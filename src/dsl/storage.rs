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
        let unit_char = s.chars().last().expect("len >= 2");
        let (num_str, unit) = s.split_at(s.len() - unit_char.len_utf8());
        let num: i64 = num_str.parse().map_err(serde::de::Error::custom)?;
        if num < 0 {
            return Err(serde::de::Error::custom(format!("TTL 不能为负: {}", s)));
        }
        let delta = match unit {
            "s" => chrono::TimeDelta::try_seconds(num),
            "m" => chrono::TimeDelta::try_minutes(num),
            "h" => chrono::TimeDelta::try_hours(num),
            "d" => chrono::TimeDelta::try_days(num),
            _ => return Err(serde::de::Error::custom(format!("无效单位: {}", unit))),
        };
        delta
            .map(Ttl)
            .ok_or_else(|| serde::de::Error::custom(format!("TTL 溢出: {}", s)))
    }
}

/// 存储 TTL 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageDef {
    pub result_ttl: Option<Ttl>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_overflow_returns_error_not_panic() {
        let r: Result<Ttl, _> = serde_json::from_str("\"200000000000d\"");
        assert!(r.is_err());
    }

    #[test]
    fn ttl_negative_returns_error() {
        let r: Result<Ttl, _> = serde_json::from_str("\"-5m\"");
        assert!(r.is_err());
    }

    #[test]
    fn ttl_valid_units() {
        let t: Ttl = serde_json::from_str("\"30d\"").unwrap();
        assert_eq!(t.0, chrono::TimeDelta::days(30));
    }

    #[test]
    fn ttl_multibyte_unit_returns_error_not_panic() {
        let r: Result<Ttl, _> = serde_json::from_str("\"5é\"");
        assert!(r.is_err());
    }

    #[test]
    fn ttl_only_multibyte_char_returns_error_not_panic() {
        let r: Result<Ttl, _> = serde_json::from_str("\"é\"");
        assert!(r.is_err());
    }
}
