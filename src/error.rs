// ---------------------------------------------------------------------------
// WeaveError — 全库统一错误类型
// ---------------------------------------------------------------------------

use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use thiserror::Error;

/// weave 全库统一错误。覆盖 DSL、存储、运行时、API 四个层级。
///
/// 每个变体包含 `#[error("...")]` 格式化字符串，可直接 `{}` 显示。
/// HTTP 状态码映射通过 `status_code()` 方法提供。
#[derive(Debug, Error)]
pub enum WeaveError {
    // ── DSL ──────────────────────────────────────────────────────────────
    #[error("parse error: {0}")]
    Parse(String),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("pipeline not found: {0}")]
    PipelineNotFound(String),

    // ── Store ───────────────────────────────────────────────────────────
    #[error("database error at `{operation}`: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: Box<RedbError>,
    },
    #[error("object not found: {0}")]
    ObjectNotFound(String),

    // ── Runtime ─────────────────────────────────────────────────────────
    #[error("DAG error: {0}")]
    Dag(String),
    #[error("operator error: {0}")]
    Operator(String),
    #[error("timeout")]
    Timeout,
    #[error("task failed: {0}")]
    TaskFailed(String),

    // ── API / CLI ───────────────────────────────────────────────────────
    #[error("not found: {0}")]
    NotFound(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("conflict: {0}")]
    Conflict(String),

    // ── Internal ────────────────────────────────────────────────────────
    #[error("internal error: {0}")]
    Internal(String),
}

/// 包裹 redb 的四种错误类型，使 WeaveError::Database 可以通过 `#[source]` 保留完整错误链。
#[derive(Debug, Error)]
pub enum RedbError {
    #[error(transparent)]
    Database(#[from] redb::DatabaseError),
    #[error(transparent)]
    Transaction(#[from] redb::TransactionError),
    #[error(transparent)]
    Table(#[from] redb::TableError),
    #[error(transparent)]
    Storage(#[from] redb::StorageError),
    #[error(transparent)]
    Commit(#[from] redb::CommitError),
    #[error(transparent)]
    Compaction(#[from] redb::CompactionError),
}

/// Convenience type alias.
pub type WeaveResult<T> = Result<T, WeaveError>;

impl WeaveError {
    /// HTTP 状态码映射，供 Axum handler 使用。
    pub fn status_code(&self) -> axum::http::StatusCode {
        use axum::http::StatusCode;
        match self {
            WeaveError::Parse(_) | WeaveError::BadRequest(_) => StatusCode::BAD_REQUEST,
            WeaveError::Validation(_) => StatusCode::UNPROCESSABLE_ENTITY,
            WeaveError::PipelineNotFound(_) => StatusCode::NOT_FOUND,
            WeaveError::Database { .. } | WeaveError::ObjectNotFound(_) => StatusCode::INTERNAL_SERVER_ERROR,
            WeaveError::Dag(_) => StatusCode::UNPROCESSABLE_ENTITY,
            WeaveError::Operator(_) | WeaveError::Timeout | WeaveError::TaskFailed(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            WeaveError::NotFound(_) => StatusCode::NOT_FOUND,
            WeaveError::Conflict(_) => StatusCode::CONFLICT,
            WeaveError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Serialize)]
struct ApiError {
    error: String,
    code: String,
}

impl IntoResponse for WeaveError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let body = Json(ApiError {
            error: self.to_string(),
            code: format!("{:?}", status.as_u16()),
        });
        (status, body).into_response()
    }
}

// ── From 转换 ──────────────────────────────────────────────────────────────

impl From<crate::dsl::parser::ParseError> for WeaveError {
    fn from(e: crate::dsl::parser::ParseError) -> Self {
        WeaveError::Parse(e.to_string())
    }
}

impl From<crate::dsl::validator::ValidationError> for WeaveError {
    fn from(e: crate::dsl::validator::ValidationError) -> Self {
        WeaveError::Validation(format!("[{}] {}", e.code, e.message))
    }
}

impl From<crate::runtime::dag::DagError> for WeaveError {
    fn from(e: crate::runtime::dag::DagError) -> Self {
        WeaveError::Dag(e.to_string())
    }
}

impl From<crate::operator::OperatorError> for WeaveError {
    fn from(e: crate::operator::OperatorError) -> Self {
        match e {
            crate::operator::OperatorError::Timeout => WeaveError::Timeout,
            other => WeaveError::Operator(other.to_string()),
        }
    }
}

impl From<serde_json::Error> for WeaveError {
    fn from(e: serde_json::Error) -> Self {
        WeaveError::Parse(e.to_string())
    }
}

impl From<serde_yaml::Error> for WeaveError {
    fn from(e: serde_yaml::Error) -> Self {
        WeaveError::Parse(e.to_string())
    }
}
