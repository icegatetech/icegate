//! Prometheus API request and response models.
//!
//! Contains typed request parameter structs for axum extractors and
//! the standard Prometheus JSON response envelope.

use serde::{Deserialize, Serialize};

/// Parameters for instant query (`POST /api/v1/query`).
///
/// The `time` field accepts a Unix timestamp in seconds (float) or an
/// RFC3339 string. If omitted, the server uses the current time.
#[derive(Debug, Deserialize)]
pub struct InstantQueryParams {
    /// `PromQL` expression to evaluate.
    pub query: String,
    /// Evaluation timestamp (Unix seconds or RFC3339). Defaults to now.
    pub time: Option<String>,
    /// Evaluation timeout. Optional.
    #[allow(dead_code)] // Will be used when timeout support is added
    pub timeout: Option<String>,
}

/// Parameters for range query (`POST /api/v1/query_range`).
///
/// All time parameters accept Unix timestamps in seconds (float) or
/// RFC3339 strings.
#[derive(Debug, Deserialize)]
pub struct RangeQueryParams {
    /// `PromQL` expression to evaluate.
    pub query: String,
    /// Start timestamp (inclusive).
    pub start: String,
    /// End timestamp (inclusive).
    pub end: String,
    /// Query resolution step width (duration string or float seconds).
    pub step: String,
    /// Evaluation timeout. Optional.
    #[allow(dead_code)] // Will be used when timeout support is added
    pub timeout: Option<String>,
}

/// Parameters for series query (`GET /api/v1/series`).
///
/// All fields are deserialized from the request but not yet consumed
/// because the series endpoint is not implemented.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields deserialized from request; read when series endpoint is implemented
pub struct SeriesParams {
    /// Series selector matchers. At least one required.
    #[serde(rename = "match[]")]
    pub matchers: Option<Vec<String>>,
    /// Start timestamp. Optional.
    pub start: Option<String>,
    /// End timestamp. Optional.
    pub end: Option<String>,
}

/// Parameters for labels query (`GET /api/v1/labels`).
///
/// All fields are deserialized from the request but not yet consumed
/// because the labels endpoint is not implemented.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields deserialized from request; read when labels endpoint is implemented
pub struct LabelsParams {
    /// Start timestamp. Optional.
    pub start: Option<String>,
    /// End timestamp. Optional.
    pub end: Option<String>,
    /// Series selector matchers for filtering. Optional.
    #[serde(rename = "match[]")]
    pub matchers: Option<Vec<String>>,
}

/// Parameters for label values query (`GET /api/v1/label/{name}/values`).
///
/// All fields are deserialized from the request but not yet consumed
/// because the label values endpoint is not implemented.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields deserialized from request; read when label values endpoint is implemented
pub struct LabelValuesParams {
    /// Start timestamp. Optional.
    pub start: Option<String>,
    /// End timestamp. Optional.
    pub end: Option<String>,
    /// Series selector matchers for filtering. Optional.
    #[serde(rename = "match[]")]
    pub matchers: Option<Vec<String>>,
}

/// Generic Prometheus API response envelope.
///
/// Follows the Prometheus HTTP API response format with `status`,
/// optional `data`, and optional error fields.
#[derive(Debug, Serialize)]
pub struct PrometheusResponse {
    /// Either "success" or "error".
    pub status: String,
    /// Response payload. Present on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Error message. Present on error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Error type classification. Present on error.
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

impl PrometheusResponse {
    /// Create a success response with the given data payload.
    pub fn success(data: serde_json::Value) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error: None,
            error_type: None,
        }
    }
}
