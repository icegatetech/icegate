//! Request extractors that refuse in the `OpenSearch` envelope instead of axum's plain-text
//! rejections.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::error::OpenSearchError;

/// Parameters every endpoint accepts. `filter_path` is applied by the `filter_path` middleware;
/// the others select output formatting that IceGate does not vary.
const COMMON_PARAMETERS: &[&str] = &["filter_path", "pretty", "human", "error_trace"];

/// The query-string parameters of one endpoint.
///
/// An implementor derives `Deserialize` with `#[serde(deny_unknown_fields)]`. A parameter that is
/// not a field, not in [`Self::IGNORED`] and not common is then refused with a `400`: silently
/// ignoring an unknown parameter could change what the request means.
pub(crate) trait EndpointParameters: DeserializeOwned {
    /// Parameters the endpoint accepts and deliberately ignores.
    const IGNORED: &'static [&'static str];
}

/// The parameters of an endpoint that takes only the common ones.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoParameters {}

impl EndpointParameters for NoParameters {
    const IGNORED: &'static [&'static str] = &[];
}

/// Extractor of an endpoint's [`EndpointParameters`]; its rejection is the envelope.
pub(crate) struct Parameters<T>(pub(crate) T);

impl<T, S> FromRequestParts<S> for Parameters<T>
where
    // `Send` because the ready future carries the decoded value, and axum requires a `Send`
    // extraction future.
    T: EndpointParameters + Send,
    S: Send + Sync,
{
    type Rejection = OpenSearchError;

    fn from_request_parts(parts: &mut Parts, _state: &S) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        // Decoding the query string needs no I/O, so the future is ready at once.
        std::future::ready(parse_parameters(parts.uri.query()).map(Self))
    }
}

/// Decode `query` into `T` after dropping the common parameters and `T::IGNORED`.
///
/// `serde_urlencoded` deserializes typed fields only from an encoded string, so the remaining
/// pairs are encoded once more and decoded into `T`: one extra pass over the query string, which
/// buys typed, strict decoding with no hand-written parser.
///
/// # Errors
///
/// Returns `illegal_argument_exception` for a malformed query string, an unknown parameter or a
/// value of the wrong type.
pub(crate) fn parse_parameters<T: EndpointParameters>(query: Option<&str>) -> Result<T, OpenSearchError> {
    let pairs: Vec<(String, String)> = serde_urlencoded::from_str(query.unwrap_or_default())
        .map_err(|error| OpenSearchError::illegal_argument(format!("malformed query string: {error}")))?;
    let endpoint_pairs: Vec<(String, String)> = pairs
        .into_iter()
        .filter(|(key, _)| !COMMON_PARAMETERS.contains(&key.as_str()) && !T::IGNORED.contains(&key.as_str()))
        .collect();
    let endpoint_query = serde_urlencoded::to_string(&endpoint_pairs)
        .map_err(|error| OpenSearchError::internal(format!("re-encoding query parameters: {error}")))?;
    serde_urlencoded::from_str(&endpoint_query).map_err(|error| {
        OpenSearchError::illegal_argument(format!(
            "request contains an unrecognized or invalid parameter: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde::Deserialize;

    use super::*;
    use crate::opensearch::error::render_envelope;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct SampleParameters {
        fields: Option<String>,
    }

    impl EndpointParameters for SampleParameters {
        const IGNORED: &'static [&'static str] = &["preference"];
    }

    #[test]
    fn parameters_accept_common_ignored_and_declared_parameters() {
        let parameters: SampleParameters =
            parse_parameters(Some("filter_path=a.b&pretty&preference=1700000000&fields=*")).expect("accepted");

        assert_eq!(
            parameters,
            SampleParameters {
                fields: Some("*".to_string())
            }
        );
    }

    /// Ignoring an unknown parameter could silently change what a request means, so it is refused.
    #[tokio::test]
    async fn parameters_reject_an_unknown_parameter() {
        let error = parse_parameters::<SampleParameters>(Some("fields=*&unknown_knob=1")).expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }

    #[tokio::test]
    async fn no_parameters_accepts_only_the_common_ones() {
        assert!(parse_parameters::<NoParameters>(None).is_ok());
        assert!(parse_parameters::<NoParameters>(Some("")).is_ok());
        assert!(parse_parameters::<NoParameters>(Some("pretty&human=true&error_trace=false&filter_path=x")).is_ok());

        let error = parse_parameters::<NoParameters>(Some("size=1")).expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }
}
