pub mod base64;
pub mod command;
pub mod dedup;
pub mod file;
pub mod filter;
pub mod http;
pub mod js;
pub mod llm;
pub mod merge;
pub mod noop;
pub mod sort;
pub mod var;

use serde_json::Value;
use std::collections::HashMap;

use crate::operator::Operator;

/// 按点分路径从 Value 中取嵌套值。空路径返回原值。
pub(crate) fn resolve_nested<'a>(value: &'a Value, path: &str) -> &'a Value {
    if path.is_empty() {
        return value;
    }
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        current = current.get(part).unwrap_or(&Value::Null);
    }
    current
}

/// 注册所有内置算子到 map 中。
pub fn register_all(ops: &mut HashMap<String, Box<dyn Operator>>) {
    let list: Vec<Box<dyn Operator>> = vec![
        Box::new(noop::NoopOperator),
        Box::new(filter::FilterOperator),
        Box::new(sort::SortOperator),
        Box::new(dedup::DedupOperator),
        Box::new(merge::MergeOperator),
        Box::new(base64::Base64Operator),
        Box::new(http::HttpOperator),
        Box::new(file::FileOperator),
        Box::new(command::CommandOperator),
        Box::new(llm::LlmOperator),
        Box::new(var::VarOperator),
    ];
    for op in list {
        let name = op.spec().type_name.to_string();
        ops.insert(name, op);
    }
}

/// 按名字查找内置算子。
pub fn get_builtin(name: &str) -> Option<Box<dyn Operator>> {
    let mut ops: HashMap<String, Box<dyn Operator>> = HashMap::new();
    register_all(&mut ops);
    ops.remove(name)
}
