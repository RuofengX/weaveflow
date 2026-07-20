pub mod base64;
pub mod command;
pub mod dedup;
pub mod file;
pub mod filter;
pub mod http;
pub mod http_client;
pub mod js;
pub mod llm;
pub mod merge;
pub mod noop;
pub mod sort;
pub mod var;

use serde_json::Value;

/// 按点分路径从 Value 中取嵌套值。空路径返回原值。
/// 数字路径段作用于 Array 时按索引取元素（与 resolver 数组索引语义一致）。
pub(crate) fn resolve_nested<'a>(value: &'a Value, path: &str) -> &'a Value {
    if path.is_empty() {
        return value;
    }
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        current = match current {
            Value::Array(arr) => part
                .parse::<usize>()
                .ok()
                .and_then(|i| arr.get(i))
                .unwrap_or(&Value::Null),
            _ => current.get(part).unwrap_or(&Value::Null),
        };
    }
    current
}

/// 数字精确比较：整型对整型直接 cmp；混合 int/float 走精确比较
/// （f64 舍入会在 ≥2^53 处破坏全序，sort 依赖全序）。filter 与 sort 共用。
pub(crate) fn compare_json_numbers(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Some(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
        return Some(x.cmp(&y));
    }
    if let Some(x) = a.as_i64() {
        return b.as_f64().map(|y| cmp_i64_f64(x, y));
    }
    if let Some(y) = b.as_i64() {
        return a.as_f64().map(|x| cmp_i64_f64(y, x).reverse());
    }
    if let Some(x) = a.as_u64() {
        return b.as_f64().map(|y| cmp_u64_f64(x, y));
    }
    if let Some(y) = b.as_u64() {
        return a.as_f64().map(|x| cmp_u64_f64(y, x).reverse());
    }
    a.as_f64()
        .zip(b.as_f64())
        .and_then(|(x, y)| x.partial_cmp(&y))
}

const TWO63: f64 = 9_223_372_036_854_775_808.0; // 2^63
const TWO64: f64 = 18_446_744_073_709_551_616.0; // 2^64

/// i64 与 f64 的精确比较（不经过 a as f64 舍入）。
fn cmp_i64_f64(a: i64, b: f64) -> std::cmp::Ordering {
    if b.is_nan() {
        return std::cmp::Ordering::Greater; // JSON 无 NaN，防御
    }
    if b >= TWO63 {
        return std::cmp::Ordering::Less;
    }
    if b < -TWO63 {
        return std::cmp::Ordering::Greater;
    }
    if b.fract() == 0.0 {
        // |b| < 2^63 的整数值 f64 → i64 转换精确
        a.cmp(&(b as i64))
    } else {
        let floor = b.floor() as i64;
        if a <= floor {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    }
}

/// u64 与 f64 的精确比较。
fn cmp_u64_f64(a: u64, b: f64) -> std::cmp::Ordering {
    if b.is_nan() {
        return std::cmp::Ordering::Greater;
    }
    if b < 0.0 {
        return std::cmp::Ordering::Greater;
    }
    if b >= TWO64 {
        return std::cmp::Ordering::Less;
    }
    if b.fract() == 0.0 {
        a.cmp(&(b as u64))
    } else {
        let floor = b.floor() as u64;
        if a <= floor {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    }
}

/// 按名字查找内置算子。直接 match，避免 HashMap 分配。
pub fn get_builtin(name: &str) -> Option<Box<dyn crate::operator::Operator>> {
    match name {
        "noop" => Some(Box::new(noop::NoopOperator)),
        "filter" => Some(Box::new(filter::FilterOperator)),
        "sort" => Some(Box::new(sort::SortOperator)),
        "dedup" => Some(Box::new(dedup::DedupOperator)),
        "merge" => Some(Box::new(merge::MergeOperator)),
        "base64" => Some(Box::new(base64::Base64Operator)),
        "http" => Some(Box::new(http::HttpOperator)),
        "js" => Some(Box::new(js::JsOperator)),
        "file" => Some(Box::new(file::FileOperator)),
        "command" => Some(Box::new(command::CommandOperator)),
        "llm" => Some(Box::new(llm::LlmOperator)),
        "var" => Some(Box::new(var::VarOperator)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn resolve_nested_array_index() {
        let v = json!({ "items": [ { "name": "a" }, { "name": "b" } ] });
        assert_eq!(resolve_nested(&v, "items.1.name"), &json!("b"));
        assert_eq!(resolve_nested(&v, "items.0.name"), &json!("a"));
    }

    #[test]
    fn resolve_nested_array_index_out_of_bounds_is_null() {
        let v = json!({ "items": [1, 2] });
        assert_eq!(resolve_nested(&v, "items.5"), &Value::Null);
        assert_eq!(resolve_nested(&v, "items.name"), &Value::Null);
    }

    #[test]
    fn compare_json_numbers_big_integers_exact() {
        let a = json!(9007199254740992_i64);
        let b = json!(9007199254740993_i64);
        assert_eq!(compare_json_numbers(&a, &b), Some(std::cmp::Ordering::Less));
        assert_eq!(
            compare_json_numbers(&b, &a),
            Some(std::cmp::Ordering::Greater)
        );
    }

    #[test]
    fn compare_json_numbers_mixed_int_float_total_order() {
        use std::cmp::Ordering::*;
        // 2^53 边界：f64 无法区分 2^53 与 2^53+1，但全序必须区分
        let c = json!(9007199254740992_i64); // 2^53
        let a = json!(9007199254740993_i64); // 2^53 + 1
        let b = json!(9007199254740992.0_f64); // 2^53 as f64
        assert_eq!(compare_json_numbers(&a, &b), Some(Greater));
        assert_eq!(compare_json_numbers(&b, &a), Some(Less));
        assert_eq!(compare_json_numbers(&c, &b), Some(Equal));
        assert_eq!(compare_json_numbers(&b, &c), Some(Equal));
        // 非整数 float
        assert_eq!(
            compare_json_numbers(&json!(3_i64), &json!(3.5_f64)),
            Some(Less)
        );
        assert_eq!(
            compare_json_numbers(&json!(4_i64), &json!(3.5_f64)),
            Some(Greater)
        );
        assert_eq!(
            compare_json_numbers(&json!(-4_i64), &json!(-3.5_f64)),
            Some(Less)
        );
        assert_eq!(
            compare_json_numbers(&json!(-3_i64), &json!(-3.5_f64)),
            Some(Greater)
        );
        // 超出 i64 的 u64 与 f64
        let big = json!(u64::MAX);
        assert_eq!(compare_json_numbers(&big, &json!(1.0_f64)), Some(Greater));
        assert_eq!(compare_json_numbers(&json!(1.0_f64), &big), Some(Less));
        assert_eq!(
            compare_json_numbers(&json!(-1_i64), &json!(1.5_f64)),
            Some(Less)
        );
    }
}
