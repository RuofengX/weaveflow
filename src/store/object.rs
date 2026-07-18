use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// 内容寻址摘要（SHA256 = 32 字节）。相同的 JSON Value 产生相同的 Digest。
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ObjectDigest(pub(crate) [u8; 32]);

impl ObjectDigest {
    pub fn compute(data: &[u8]) -> Self {
        Self(Sha256::digest(data).into())
    }
    pub fn from_value(value: &serde_json::Value) -> Self {
        let json = serde_json::to_vec(value).unwrap_or_default();
        Self::compute(&json)
    }
    pub fn hex(&self) -> String {
        let mut hex = String::with_capacity(64);
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        hex
    }
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.len() != 64 {
            return Err(format!(
                "ObjectDigest 需要 64 个 hex 字符，收到 {}",
                s.len()
            ));
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|_| format!("第 {} 字节的 hex 格式无效", i))?;
        }
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for ObjectDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Digest({})", self.hex())
    }
}

impl Serialize for ObjectDigest {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.hex().serialize(s)
    }
}
impl<'de> Deserialize<'de> for ObjectDigest {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let hex_str = String::deserialize(d)?;
        Self::from_hex(&hex_str).map_err(serde::de::Error::custom)
    }
}

/// Object 表的 Value 类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectValue {
    pub data: Value,
}

impl ObjectValue {
    pub fn new(data: Value) -> Self {
        ObjectValue { data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_value_new() {
        let ov = ObjectValue::new(json!({"a": 1}));
        assert_eq!(ov.data, json!({"a": 1}));
    }
}
