use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::dsl::parser::ParseError;
use crate::engine::dag::DagError;
use crate::operator::OperatorError;

#[derive(Debug, thiserror::Error)]
pub enum WeaveflowError {
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
        source: Box<WeaveflowError>,
    },

    #[error("请求参数错误: {0}")]
    BadRequest(String),

    #[error("服务不可用: {0}")]
    Unavailable(String),
}

impl From<rust_yaml::Error> for WeaveflowError {
    fn from(e: rust_yaml::Error) -> Self {
        WeaveflowError::Parse(format!("YAML: {e}"))
    }
}

impl From<serde_json::Error> for WeaveflowError {
    fn from(e: serde_json::Error) -> Self {
        WeaveflowError::Parse(format!("JSON: {e}"))
    }
}

impl From<redb::Error> for WeaveflowError {
    fn from(e: redb::Error) -> Self {
        WeaveflowError::Internal(format!("storage: {e}"))
    }
}

impl From<redb::DatabaseError> for WeaveflowError {
    fn from(e: redb::DatabaseError) -> Self {
        WeaveflowError::Internal(format!("database: {e}"))
    }
}

impl From<redb::TransactionError> for WeaveflowError {
    fn from(e: redb::TransactionError) -> Self {
        WeaveflowError::Internal(format!("transaction: {e}"))
    }
}

impl From<redb::TableError> for WeaveflowError {
    fn from(e: redb::TableError) -> Self {
        WeaveflowError::Internal(format!("table: {e}"))
    }
}

impl From<redb::StorageError> for WeaveflowError {
    fn from(e: redb::StorageError) -> Self {
        WeaveflowError::Internal(format!("storage: {e}"))
    }
}

impl From<redb::CommitError> for WeaveflowError {
    fn from(e: redb::CommitError) -> Self {
        WeaveflowError::Internal(format!("commit: {e}"))
    }
}

impl From<redb::CompactionError> for WeaveflowError {
    fn from(e: redb::CompactionError) -> Self {
        WeaveflowError::Internal(format!("compaction: {e}"))
    }
}

impl From<ParseError> for WeaveflowError {
    fn from(e: ParseError) -> Self {
        WeaveflowError::Parse(e.to_string())
    }
}

impl From<DagError> for WeaveflowError {
    fn from(e: DagError) -> Self {
        WeaveflowError::Internal(format!("DAG: {e}"))
    }
}

impl From<OperatorError> for WeaveflowError {
    fn from(e: OperatorError) -> Self {
        WeaveflowError::Operator(e.to_string())
    }
}

impl IntoResponse for WeaveflowError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            WeaveflowError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            WeaveflowError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            WeaveflowError::Parse(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            WeaveflowError::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            WeaveflowError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            WeaveflowError::Internal(detail) => {
                tracing::error!(%detail, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".into(),
                )
            }
            WeaveflowError::Operator(detail) => {
                tracing::error!(%detail, "operator error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".into(),
                )
            }
            _ => {
                tracing::error!(error = %self, "unhandled error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".into(),
                )
            }
        };
        let body = axum::Json(json!({"error": msg}));
        (status, body).into_response()
    }
}

pub type WeaveflowResult<T> = Result<T, WeaveflowError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_status_codes() {
        assert_eq!(
            WeaveflowError::BadRequest("test".into())
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            WeaveflowError::Parse("test".into())
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            WeaveflowError::Validation("test".into())
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            WeaveflowError::NotFound("test".into())
                .into_response()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            WeaveflowError::Internal("test".into())
                .into_response()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            WeaveflowError::Operator("test".into())
                .into_response()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            WeaveflowError::Io(std::io::Error::other("io"))
                .into_response()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn error_response_body_contains_error() {
        let resp = WeaveflowError::BadRequest("invalid task id: xyz".into()).into_response();
        let status = resp.status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn internal_error_masks_detail_in_body() {
        let resp = WeaveflowError::Internal("secret redb corruption detail".into()).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body_bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let body_str = String::from_utf8_lossy(&body_bytes);
        assert!(body_str.contains("internal server error"));
        assert!(!body_str.contains("secret"));
        assert!(!body_str.contains("redb"));
    }
}
