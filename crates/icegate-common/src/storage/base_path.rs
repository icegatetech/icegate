//! Base-path parsing: which store a configured location names.
//!
//! A base path is what an operator configures a component with (`queue.common.base_path`,
//! a warehouse location). This module answers what such a string names — S3, the local
//! filesystem, or nothing recognisable — and nothing else; picking and caching the
//! operator that serves it is the [`registry`](super::registry)'s job. The two are kept
//! apart because the predicate below is a config-validation tool, used by callers that
//! never resolve a store at all.

/// URI schemes served from S3.
const S3_SCHEMES: [&str; 2] = ["s3://", "s3a://"];

/// URI scheme served from the local filesystem; a leading `/` names one too.
const LOCAL_SCHEME: &str = "file://";

/// Strip the S3 scheme from `base_path`, leaving `bucket/prefix`.
pub(super) fn strip_s3_scheme(base_path: &str) -> Option<&str> {
    S3_SCHEMES.iter().find_map(|scheme| base_path.strip_prefix(scheme))
}

/// Whether `base_path` names a local filesystem location.
pub(super) fn is_local_base_path(base_path: &str) -> bool {
    base_path.starts_with(LOCAL_SCHEME) || base_path.starts_with('/')
}

/// Whether [`OperatorRegistry::resolve_object_store`](super::OperatorRegistry::resolve_object_store)
/// maps `base_path` onto a store that outlives the process.
///
/// Everything it does not recognise — an empty string, a typo such as `s3:/`,
/// a bare bucket name — resolves to a fresh in-memory store instead of an
/// error, because tests and the in-memory catalog depend on that fallback.
/// A component whose data MUST land in object storage therefore has to reject
/// such a path itself, before it resolves one: writes to the fallback are
/// accepted, reads return nothing, and the process is the only place the data
/// ever was.
#[must_use]
pub fn is_persistent_base_path(base_path: &str) -> bool {
    strip_s3_scheme(base_path).is_some() || is_local_base_path(base_path)
}
