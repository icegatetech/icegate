//! The PPL error envelope.
//!
//! A PPL execution error is answered as `{status, error: {type, reason, details, code,
//! context?}}`, labelled as JSON; `reason` and `details` are always present. A shed, a passed
//! deadline or write contention keeps the `OpenSearch` 503 envelope, and a request-parsing error
//! the `OpenSearch` 400 envelope, so the listener has one retry contract.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value, json};

use crate::ppl::PplError;

/// The content type of the PPL error envelope. It is JSON on purpose: a client that reads an
/// error's reason only from a body labelled JSON shows it then, which the E2E case `a ppl error
/// shows its reason in the workbench` pins.
const PPL_ERROR_CONTENT_TYPE: &str = "application/json; charset=UTF-8";

/// The reason a 500 PPL error reports; the cause goes into `details` and the log.
const INTERNAL_REASON: &str = "There was internal problem at backend";

/// The reason a PPL syntax error reports; the offending token goes into `details`.
const SYNTAX_REASON: &str = "Invalid Query";

/// The `cause` of the refusal of a `timechart` aggregation the other-series row cannot merge
/// exactly; with the refused `function` beside it, a client tells this refusal from any other
/// `SEMANTIC_ERROR` without reading its reason.
const INEXACT_OTHER_SERIES_CAUSE: &str = "inexact_other_series";

/// Answer `error` in its envelope.
pub(crate) fn render_ppl_error(error: &PplError) -> Response {
    let (status, error_type, code, context, fixed_reason) = match error {
        PplError::Unavailable(cause) => return cause.clone().into_response(),
        PplError::Syntax { line, column, .. } => (
            StatusCode::BAD_REQUEST,
            "SyntaxCheckException",
            "SYNTAX_ERROR",
            Some(json!({"line": line, "column": column})),
            Some(SYNTAX_REASON),
        ),
        PplError::IndexNotFound { index } => (
            StatusCode::NOT_FOUND,
            "IndexNotFoundException",
            "INDEX_NOT_FOUND",
            Some(json!({"index": index})),
            None,
        ),
        PplError::FieldNotFound { field } => (
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "FIELD_NOT_FOUND",
            Some(json!({"field": field})),
            None,
        ),
        PplError::UnsupportedCommand { command } => (
            StatusCode::BAD_REQUEST,
            "UnsupportedOperationException",
            "UNSUPPORTED_OPERATION",
            Some(json!({"command": command})),
            None,
        ),
        PplError::UnsupportedFunction { function } => (
            StatusCode::BAD_REQUEST,
            "UnsupportedOperationException",
            "UNSUPPORTED_OPERATION",
            Some(json!({"function": function})),
            None,
        ),
        PplError::Semantic { name, .. } => (
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
            name.as_ref().map(|name| json!({"name": name})),
            None,
        ),
        PplError::InexactOtherSeries { function } => (
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
            Some(json!({"function": function, "cause": INEXACT_OTHER_SERIES_CAUSE})),
            None,
        ),
        PplError::RowLimitExceeded { limit } => (
            StatusCode::BAD_REQUEST,
            "IllegalArgumentException",
            "RESOURCE_LIMIT_EXCEEDED",
            Some(json!({"limit": limit})),
            None,
        ),
        PplError::NestingLimitExceeded { limit_depth } => (
            StatusCode::BAD_REQUEST,
            "IllegalArgumentException",
            "RESOURCE_LIMIT_EXCEEDED",
            Some(json!({"limit_depth": limit_depth})),
            None,
        ),
        PplError::SubsearchNestingLimitExceeded { limit_subsearch_depth } => (
            StatusCode::BAD_REQUEST,
            "IllegalArgumentException",
            "RESOURCE_LIMIT_EXCEEDED",
            Some(json!({"limit_subsearch_depth": limit_subsearch_depth})),
            None,
        ),
        PplError::OperatorLimitExceeded { limit_operators } => (
            StatusCode::BAD_REQUEST,
            "IllegalArgumentException",
            "RESOURCE_LIMIT_EXCEEDED",
            Some(json!({"limit_operators": limit_operators})),
            None,
        ),
        PplError::MemoryLimitExceeded { limit_bytes } => (
            StatusCode::BAD_REQUEST,
            "IllegalArgumentException",
            "RESOURCE_LIMIT_EXCEEDED",
            Some(json!({"limit_bytes": limit_bytes})),
            None,
        ),
        PplError::Internal { reason } => {
            tracing::error!(%reason, "PPL query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "IllegalStateException",
                "EXECUTION_ERROR",
                None,
                Some(INTERNAL_REASON),
            )
        }
    };
    let details = error.to_string();
    let reason = fixed_reason.map_or_else(|| details.clone(), str::to_string);
    let mut body = Map::new();
    body.insert("type".to_string(), json!(error_type));
    body.insert("reason".to_string(), json!(reason));
    body.insert("details".to_string(), json!(details));
    body.insert("code".to_string(), json!(code));
    if let Some(context) = context {
        body.insert("context".to_string(), context);
    }
    let envelope = json!({"status": status.as_u16(), "error": Value::Object(body)});
    let text = serde_json::to_string_pretty(&envelope).unwrap_or_else(|_| envelope.to_string());
    let mut response = (status, text).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(PPL_ERROR_CONTENT_TYPE));
    response
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::error::OpenSearchError;

    async fn render_ppl_answer(error: &PplError) -> (StatusCode, String, Value) {
        let response = render_ppl_error(error);
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = to_bytes(response.into_body(), usize::MAX).await.expect("body");
        (status, content_type, serde_json::from_slice(&body).expect("JSON body"))
    }

    #[tokio::test]
    async fn a_syntax_error_is_invalid_query_with_the_token_in_details_as_json() {
        let error = PplError::Syntax {
            offending_text: "=".to_string(),
            line: 1,
            column: 20,
        };
        let (status, content_type, mut body) = render_ppl_answer(&error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(content_type, "application/json; charset=UTF-8");
        // `details` is IceGate's own wording: only that it names the offending token is a contract.
        let details_value = body["error"]
            .as_object_mut()
            .and_then(|error| error.remove("details"))
            .expect("details");
        let details = details_value.as_str().expect("details is a string");
        assert!(details.contains("'='"), "details {details}");
        assert_eq!(
            body,
            json!({
                "status": 400,
                "error": {
                    "type": "SyntaxCheckException",
                    "reason": "Invalid Query",
                    "code": "SYNTAX_ERROR",
                    "context": {"line": 1, "column": 20}
                }
            })
        );
    }

    #[tokio::test]
    async fn an_unknown_index_is_a_404_with_its_code_and_an_internal_error_hides_its_cause_in_details() {
        let (status, _, body) = render_ppl_answer(&PplError::IndexNotFound {
            index: "nope".to_string(),
        })
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], json!("IndexNotFoundException"));
        assert_eq!(body["error"]["reason"], json!("no such index [nope]"));
        assert_eq!(body["error"]["code"], json!("INDEX_NOT_FOUND"));
        assert_eq!(body["error"]["context"], json!({"index": "nope"}));
        let (status, _, body) = render_ppl_answer(&PplError::Internal {
            reason: "disk on fire".to_string(),
        })
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"]["reason"], json!("There was internal problem at backend"));
        assert_eq!(body["error"]["details"], json!("disk on fire"));
    }

    #[tokio::test]
    async fn the_nesting_subsearch_and_operator_limits_and_the_inexact_other_series_refusals_carry_their_fields_in_context()
     {
        let (status, content_type, body) = render_ppl_answer(&PplError::NestingLimitExceeded { limit_depth: 16 }).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(content_type, "application/json; charset=UTF-8");
        assert_eq!(body["status"], json!(400));
        assert_eq!(body["error"]["type"], json!("IllegalArgumentException"));
        assert_eq!(body["error"]["code"], json!("RESOURCE_LIMIT_EXCEEDED"));
        assert_eq!(body["error"]["context"], json!({"limit_depth": 16}));

        let (status, _, body) = render_ppl_answer(&PplError::SubsearchNestingLimitExceeded {
            limit_subsearch_depth: 4,
        })
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], json!("IllegalArgumentException"));
        assert_eq!(body["error"]["code"], json!("RESOURCE_LIMIT_EXCEEDED"));
        assert_eq!(body["error"]["context"], json!({"limit_subsearch_depth": 4}));

        let (status, _, body) = render_ppl_answer(&PplError::OperatorLimitExceeded { limit_operators: 128 }).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], json!("IllegalArgumentException"));
        assert_eq!(body["error"]["code"], json!("RESOURCE_LIMIT_EXCEEDED"));
        assert_eq!(body["error"]["context"], json!({"limit_operators": 128}));

        let (status, _, body) = render_ppl_answer(&PplError::InexactOtherSeries {
            function: "avg".to_string(),
        })
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], json!("SemanticCheckException"));
        assert_eq!(body["error"]["code"], json!("SEMANTIC_ERROR"));
        assert_eq!(
            body["error"]["context"],
            json!({"function": "avg", "cause": "inexact_other_series"})
        );
    }

    #[tokio::test]
    async fn a_semantic_refusal_answers_its_name_as_context_and_one_without_a_name_answers_no_context() {
        let reason = "function [lower] takes 1 argument(s), not 2";
        let (status, content_type, named_body) = render_ppl_answer(&PplError::Semantic {
            reason: reason.to_string(),
            name: Some("lower".to_string()),
        })
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(content_type, "application/json; charset=UTF-8");
        assert_eq!(
            named_body,
            json!({
                "status": 400,
                "error": {
                    "type": "SemanticCheckException",
                    "reason": reason,
                    "details": reason,
                    "code": "SEMANTIC_ERROR",
                    "context": {"name": "lower"}
                }
            })
        );
        let (status, _, unnamed_body) = render_ppl_answer(&PplError::Semantic {
            reason: reason.to_string(),
            name: None,
        })
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            unnamed_body,
            json!({
                "status": 400,
                "error": {
                    "type": "SemanticCheckException",
                    "reason": reason,
                    "details": reason,
                    "code": "SEMANTIC_ERROR"
                }
            })
        );
    }

    #[tokio::test]
    async fn an_unavailable_error_keeps_the_opensearch_503_envelope() {
        let cause = OpenSearchError::deadline_exceeded();
        let expected = cause.clone().into_response();
        let (expected_status, expected_type) =
            (expected.status(), expected.headers().get(header::CONTENT_TYPE).cloned());
        let response = render_ppl_error(&PplError::Unavailable(cause));
        assert_eq!(response.status(), expected_status);
        assert_eq!(response.headers().get(header::CONTENT_TYPE).cloned(), expected_type);
    }
}
