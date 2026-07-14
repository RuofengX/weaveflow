use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::dsl::parser::ParseError;
use crate::engine::dag::DagError;
use crate::operator::OperatorError;

#[derive(Debug, thiserror::Error)]
pub enum WeaveError {
    #[error("{0}")]
    Internal(String),

    #[error("解析错误: {0}")]
    Parse(String),

    #[error("校验错误: {0}")]
    Validation(String),

    #[error("查找失败: {0}")]
    NotFound(String),

    #[error("算子错误: {0}")]
    Operator(String),

    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("数据库错误 — 操作: {operation}, 原因: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: Box<WeaveError>,
    },

    #[error("请求参数错误: {0}")]
    BadRequest(String),
}

impl From<rust_yaml::Error> for WeaveError {
    fn from(e: rust_yaml::Error) -> Self {
        WeaveError::Parse(format!("YAML: {e}"))
    }
}

impl From<serde_json::Error> for WeaveError {
    fn from(e: serde_json::Error) -> Self {
        WeaveError::Parse(format!("JSON: {e}"))
    }
}

impl From<redb::Error> for WeaveError {
    fn from(e: redb::Error) -> Self {
        WeaveError::Internal(format!("storage: {e}"))
    }
}

impl From<redb::DatabaseError> for WeaveError {
    fn from(e: redb::DatabaseError) -> Self {
        WeaveError::Internal(format!("database: {e}"))
    }
}

impl From<redb::TransactionError> for WeaveError {
    fn from(e: redb::TransactionError) -> Self {
        WeaveError::Internal(format!("transaction: {e}"))
    }
}

impl From<redb::TableError> for WeaveError {
    fn from(e: redb::TableError) -> Self {
        WeaveError::Internal(format!("table: {e}"))
    }
}

impl From<redb::StorageError> for WeaveError {
    fn from(e: redb::StorageError) -> Self {
        WeaveError::Internal(format!("storage: {e}"))
    }
}

impl From<redb::CommitError> for WeaveError {
    fn from(e: redb::CommitError) -> Self {
        WeaveError::Internal(format!("commit: {e}"))
    }
}

impl From<redb::CompactionError> for WeaveError {
    fn from(e: redb::CompactionError) -> Self {
        WeaveError::Internal(format!("compaction: {e}"))
    }
}

impl From<ParseError> for WeaveError {
    fn from(e: ParseError) -> Self {
        WeaveError::Parse(e.to_string())
    }
}

impl From<DagError> for WeaveError {
    fn from(e: DagError) -> Self {
        WeaveError::Internal(format!("DAG: {e}"))
    }
}

impl From<OperatorError> for WeaveError {
    fn from(e: OperatorError) -> Self {
        WeaveError::Operator(e.to_string())
    }
}

impl IntoResponse for WeaveError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            WeaveError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            WeaveError::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            WeaveError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            WeaveError::Operator(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        let body = axum::Json(json!({"error": msg}));
        (status, body).into_response()
    }
}

pub type WeaveResult<T> = Result<T, WeaveError>;
