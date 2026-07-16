use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct OperatorSpec {
    pub type_name: String,
    pub description: String,
    pub iterate: bool,
    pub cache: bool,
}

impl OperatorSpec {
    pub fn new(type_name: impl Into<String>, description: impl Into<String>) -> Self {
        OperatorSpec { type_name: type_name.into(), description: description.into(), iterate: false, cache: true }
    }
    pub fn with_iterate(mut self, yes: bool) -> Self {
        self.iterate = yes;
        self
    }
    pub fn with_cache(mut self, yes: bool) -> Self {
        self.cache = yes;
        self
    }
}

#[derive(Debug, Error)]
pub enum OperatorError {
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("config error: {0}")]
    Config(String),
    #[error("operator timeout")]
    Timeout,
}

#[async_trait]
pub trait Operator: Send + Sync {
    fn spec(&self) -> OperatorSpec;

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError>;
}
