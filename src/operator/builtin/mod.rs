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

/// 数字精确比较：i64/u64 整型直接 cmp，混合符号或小数回落 f64。
/// filter 与 sort 共用，保证整数比较语义一致。
pub(crate) fn compare_json_numbers(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Some(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
        return Some(x.cmp(&y));
    }
    a.as_f64()
        .zip(b.as_f64())
        .and_then(|(x, y)| x.partial_cmp(&y))
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
        assert_eq!(compare_json_numbers(&b, &a), Some(std::cmp::Ordering::Greater));
    }
}
