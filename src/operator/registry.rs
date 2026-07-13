use std::collections::HashMap;

use super::r#trait::Operator;

/// 返回所有内置算子（name → impl）。
pub fn builtins() -> HashMap<String, Box<dyn Operator>> {
    let mut ops: HashMap<String, Box<dyn Operator>> = HashMap::new();
    crate::operator::builtin::register_all(&mut ops);
    ops
}

/// 按名字查找内置算子。
pub fn get_builtin(name: &str) -> Option<Box<dyn Operator>> {
    builtins().remove(name)
}
