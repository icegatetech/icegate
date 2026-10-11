//! Request extractors that refuse in the `OpenSearch` envelope instead of axum's plain-text
//! rejections.

use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, FailedToBufferBody, PathRejection};
use axum::extract::{FromRequest, FromRequestParts, OptionalFromRequestParts, Path, Request};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};

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

/// Serde helper for boolean query flags: a bare or empty flag and `true` mean `true`, `false`
/// means `false`, and an absent flag is `None`.
///
/// # Errors
///
/// Returns a serde error for any other value, so a misspelt flag is refused instead of read as
/// `false`.
pub(crate) fn deserialize_flag<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<String>::deserialize(deserializer)?.as_deref() {
        None => Ok(None),
        Some("" | "true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(unknown_flag) => Err(serde::de::Error::custom(format!(
            "expected true or false, got [{unknown_flag}]"
        ))),
    }
}

/// What a wildcard expands to (`expand_wildcards`).
///
/// IceGate has no closed indices, so `closed` expands to nothing. The dot-prefixed system indices
/// are hidden: a wildcard reaches them only with `hidden`, while a pattern starting with `.`
/// reaches them regardless. `hidden` alone, without `open`, expands to nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ExpandWildcards {
    /// Wildcards match open indices.
    pub(crate) is_open: bool,
    /// Wildcards also match hidden indices.
    pub(crate) is_hidden: bool,
}

impl ExpandWildcards {
    /// Parse a comma list of `open`, `closed`, `hidden`, `all` and `none`. Absent means `open`.
    ///
    /// # Errors
    ///
    /// Returns `illegal_argument_exception` for any other value, an empty one included.
    pub(crate) fn parse(text: Option<&str>) -> Result<Self, OpenSearchError> {
        let Some(text) = text else {
            return Ok(Self {
                is_open: true,
                is_hidden: false,
            });
        };
        let mut expand_wildcards = Self {
            is_open: false,
            is_hidden: false,
        };
        for expansion in text.split(',') {
            match expansion {
                "open" => expand_wildcards.is_open = true,
                "hidden" => expand_wildcards.is_hidden = true,
                "all" => {
                    expand_wildcards.is_open = true;
                    expand_wildcards.is_hidden = true;
                }
                "closed" | "none" => {}
                unknown_expansion => {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "no enum constant for expand_wildcards [{unknown_expansion}]"
                    )));
                }
            }
        }
        Ok(expand_wildcards)
    }
}

/// The path parameters of an `OpenSearch` route, read like `axum::extract::Path` but refused in
/// the `OpenSearch` envelope, never with a plain-text body: a segment that is not UTF-8 once
/// percent-decoded, or whose value does not parse as `T`, answers `400`
/// `illegal_argument_exception`; a route whose parameters do not fit `T` in number or kind, a
/// wiring mistake no request can fix, answers `500`.
pub(crate) struct PathParameters<T>(pub(crate) T);

impl<T, S> FromRequestParts<S> for PathParameters<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = OpenSearchError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        // `Path` implements both `FromRequestParts` and `OptionalFromRequestParts`, whose methods
        // share a name, so the trait is named explicitly.
        <Path<T> as FromRequestParts<S>>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(map_path_rejection)
    }
}

/// A route without the parameter yields `None`; a parameter that is present but rejected answers
/// the envelope.
impl<T, S> OptionalFromRequestParts<S> for PathParameters<T>
where
    T: DeserializeOwned + Send + 'static,
    S: Send + Sync,
{
    type Rejection = OpenSearchError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Option<Self>, Self::Rejection> {
        <Path<T> as OptionalFromRequestParts<S>>::from_request_parts(parts, state)
            .await
            .map(|path| path.map(|Path(value)| Self(value)))
            .map_err(map_path_rejection)
    }
}

/// The envelope for a rejected path parameter: `400` for what the client sent, `500` for a route
/// whose parameters do not fit its handler, a wiring mistake no request can fix.
// Taken by value so that it is the `map_err` callback itself; it reads the rejection only through
// `&self` methods, which is what `needless_pass_by_value` flags.
#[allow(clippy::needless_pass_by_value)]
fn map_path_rejection(rejection: PathRejection) -> OpenSearchError {
    let reason = format!("invalid path parameter: {}", rejection.body_text());
    if rejection.status().is_client_error() {
        OpenSearchError::illegal_argument(reason)
    } else {
        OpenSearchError::internal(reason)
    }
}

/// The raw request body, read up to the router's `DefaultBodyLimit`.
///
/// A body over the limit is refused with the `413` envelope, any other read failure with `400`,
/// so neither reaches the client as axum's plain-text rejection.
#[derive(Debug)]
pub(crate) struct RequestBody(pub(crate) Bytes);

impl<S> FromRequest<S> for RequestBody
where
    S: Send + Sync,
{
    type Rejection = OpenSearchError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Bytes::from_request(request, state).await {
            Ok(bytes) => Ok(Self(bytes)),
            Err(BytesRejection::FailedToBufferBody(FailedToBufferBody::LengthLimitError(_))) => {
                Err(OpenSearchError::payload_too_large())
            }
            Err(rejection) => Err(OpenSearchError::illegal_argument(rejection.body_text())),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::{get, post};
    use serde::Deserialize;
    use tower::ServiceExt as _;

    use super::*;
    use crate::opensearch::error::{read_envelope, render_envelope};

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

    #[derive(Debug, Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct FlagParameters {
        #[serde(default, deserialize_with = "deserialize_flag")]
        ignore_unavailable: Option<bool>,
    }

    impl EndpointParameters for FlagParameters {
        const IGNORED: &'static [&'static str] = &[];
    }

    #[test]
    fn flags_accept_bare_empty_true_and_false_and_refuse_anything_else() {
        let read_flag = |query: &str| {
            parse_parameters::<FlagParameters>(Some(query)).map(|parameters| parameters.ignore_unavailable)
        };

        assert_eq!(read_flag("ignore_unavailable").expect("bare"), Some(true));
        assert_eq!(read_flag("ignore_unavailable=").expect("empty"), Some(true));
        assert_eq!(read_flag("ignore_unavailable=true").expect("true"), Some(true));
        assert_eq!(read_flag("ignore_unavailable=false").expect("false"), Some(false));
        assert_eq!(read_flag("").expect("absent"), None);
        assert!(read_flag("ignore_unavailable=yes").is_err());
        assert!(read_flag("ignore_unavailable=TRUE").is_err());
    }

    /// `hidden` sets only `is_hidden`, `all` sets both flags, and `closed` and `none` set neither.
    #[test]
    fn expand_wildcards_reads_open_hidden_all_none_and_closed() {
        let cases = [
            (None, true, false),
            (Some("open"), true, false),
            (Some("hidden"), false, true),
            (Some("open,hidden"), true, true),
            (Some("all"), true, true),
            (Some("none"), false, false),
            (Some("closed"), false, false),
            (Some("closed,open"), true, false),
        ];
        for (text, is_open, is_hidden) in cases {
            let expand_wildcards = ExpandWildcards::parse(text).expect("a served value");

            assert_eq!(expand_wildcards.is_open, is_open, "{text:?}");
            assert_eq!(expand_wildcards.is_hidden, is_hidden, "{text:?}");
        }
        assert!(ExpandWildcards::parse(Some("everything")).is_err());
        assert!(ExpandWildcards::parse(Some("open,")).is_err());
    }

    /// A route whose parameters do not fit its handler is a wiring mistake no request can fix, so
    /// it is a `500`, never a `400` that blames the client. Two segments read into one `String`
    /// are such a mismatch.
    #[tokio::test]
    async fn path_parameters_that_do_not_fit_their_route_are_a_500_envelope() {
        let app = Router::new().route(
            "/{first}/{second}",
            get(|PathParameters(text): PathParameters<String>| async move { text }),
        );

        let response = app
            .oneshot(Request::builder().uri("/a/b").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["status"], 500);
        assert_eq!(body["error"]["type"], "exception");
    }

    /// A body that breaks off mid-transfer, as when the client's connection drops while it sends,
    /// is a `400` in the envelope rather than axum's plain-text rejection. The same route reads a
    /// body that arrives whole, so the refusal is the body read's own.
    #[tokio::test]
    async fn a_body_that_fails_mid_transfer_is_a_400_envelope() {
        let app = Router::new().route(
            "/",
            post(|RequestBody(body): RequestBody| async move { body.len().to_string() }),
        );
        let failing_chunks = futures::stream::iter([
            Ok(Bytes::from_static(b"{")),
            Err(std::io::Error::other("connection reset")),
        ]);

        let whole_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .body(Body::from("{}"))
                    .expect("request"),
            )
            .await
            .expect("response");
        let failed_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .body(Body::from_stream(failing_chunks))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(whole_response.status(), StatusCode::OK);
        let (status, body) = read_envelope(failed_response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status"], 400);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }
}
