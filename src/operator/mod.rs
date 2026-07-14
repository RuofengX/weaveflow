pub mod types;
pub mod registry;
pub mod builtin;

pub use types::{Operator, OperatorSpec, OperatorError};
pub use builtin::get_builtin;
pub use builtin::js::JsOperator;

pub fn builtins() -> std::collections::HashMap<&'static str, &'static dyn Operator> {
    let mut m = std::collections::HashMap::new();
    m.insert("noop", &builtin::noop::NoopOperator as &dyn Operator);
    m.insert("filter", &builtin::filter::FilterOperator as &dyn Operator);
    m.insert("sort", &builtin::sort::SortOperator as &dyn Operator);
    m.insert("dedup", &builtin::dedup::DedupOperator as &dyn Operator);
    m.insert("merge", &builtin::merge::MergeOperator as &dyn Operator);
    m.insert("split", &builtin::split::SplitOperator as &dyn Operator);
    m.insert("base64", &builtin::base64::Base64Operator as &dyn Operator);
    m.insert("http", &builtin::http::HttpOperator as &dyn Operator);
    m.insert("file", &builtin::file::FileOperator as &dyn Operator);
    m.insert("command", &builtin::command::CommandOperator as &dyn Operator);
    m.insert("llm", &builtin::llm::LlmOperator as &dyn Operator);
    m.insert("var", &builtin::var::VarOperator as &dyn Operator);
    m.insert("js", &builtin::js::JsOperatorPlaceholder as &dyn Operator);
    m.insert("fork", &builtin::fork::ForkOperator as &dyn Operator);
    m
}
