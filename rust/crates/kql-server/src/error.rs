//! Kusto-style error envelope: `{"error":{"code","message","@type","@message","@permanent"}}`.

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub type_name: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(
        status: StatusCode,
        code: &'static str,
        type_name: &'static str,
        message: impl Into<String>,
    ) -> ApiError {
        ApiError { status, code, type_name, message: message.into() }
    }

    /// Invalid request: unparsable body, bad KQL, unknown table, ...
    pub fn bad_request(message: impl Into<String>) -> ApiError {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "General_BadRequest",
            "Kusto.Data.Exceptions.KustoBadRequestException",
            message,
        )
    }

    /// A translation (parse / semantic) error.
    pub fn translation(message: impl Into<String>) -> ApiError {
        let message = message.into();
        let ty = if message.starts_with("syntax error") {
            "Kusto.Data.Exceptions.SyntaxException"
        } else {
            "Kusto.Data.Exceptions.SemanticException"
        };
        ApiError::new(StatusCode::BAD_REQUEST, "General_BadRequest", ty, message)
    }

    /// The engine failed executing valid SQL.
    pub fn engine(message: impl Into<String>) -> ApiError {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "General_InternalServerError",
            "Kusto.Data.Exceptions.KustoServiceException",
            message,
        )
    }

    pub fn timeout() -> ApiError {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            "Request_ExecutionTimeout",
            "Kusto.Data.Exceptions.KustoRequestExecutionTimeoutException",
            "Query execution has exceeded the allowed limits (the server's --timeout-secs)",
        )
    }

    pub fn unauthorized() -> ApiError {
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "Kusto.Data.Exceptions.KustoRequestDeniedException",
            "missing or invalid bearer token",
        )
    }

    pub fn forbidden(message: impl Into<String>) -> ApiError {
        ApiError::new(StatusCode::FORBIDDEN, "Forbidden", "Kusto.Data.Exceptions.KustoRequestDeniedException", message)
    }

    pub fn not_found(message: impl Into<String>) -> ApiError {
        ApiError::new(
            StatusCode::NOT_FOUND,
            "General_NotFound",
            "Kusto.Data.Exceptions.KustoBadRequestException",
            message,
        )
    }

    pub fn body(&self) -> serde_json::Value {
        json!({
            "error": {
                "code": self.code,
                "message": self.message,
                "@type": self.type_name,
                "@message": self.message,
                "@permanent": self.status != StatusCode::GATEWAY_TIMEOUT && self.status != StatusCode::INTERNAL_SERVER_ERROR,
            }
        })
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut r = (self.status, self.body().to_string()).into_response();
        r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json; charset=utf-8"));
        r
    }
}
