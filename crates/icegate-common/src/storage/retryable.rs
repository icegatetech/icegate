//! Which object-store failures are worth trying again.
//!
//! Every IceGate component reaches object storage through `object_store`, whose
//! error type says nothing about whether the fault was transient. The answer
//! lives one link below: [`object_store_opendal`] maps every `opendal::Error`
//! except `NotFound`/`AlreadyExists`/`ConditionNotMatch`/`Unsupported` into
//! [`object_store::Error::Generic`] with the `opendal::Error` as its source, and
//! the S3 backend marks 409/429/499/5xx on that error as temporary. So opendal
//! is the source of truth here: a classifier that only looks for
//! [`std::io::Error`] finds nothing in the chain and calls throttling permanent.

use std::{error::Error, io};

/// Whether the store may succeed at the same operation if it is tried again.
///
/// Only [`object_store::Error::Generic`] is examined, and only through
/// [`is_retryable_error_source`]. Every other variant names an outcome the
/// service already decided — the object is not there, the precondition did not
/// hold, the credentials do not allow it — and repeating the request answers
/// the same way.
pub fn is_retryable_object_store_error(error: &object_store::Error) -> bool {
    match error {
        object_store::Error::Generic { source, .. } => is_retryable_error_source(source.as_ref()),
        // `object_store::Error` is `#[non_exhaustive]`, so a downstream crate
        // cannot name the remaining variants exhaustively and the catch-all is
        // the only form this match can take.
        _ => false,
    }
}

/// Whether any link of an error's source chain reports a transient fault.
///
/// The chain is walked from `error` itself downwards, so passing an already
/// typed error (an [`std::io::Error`], an `opendal::Error`) classifies that
/// error directly. A link counts as transient when opendal marked it temporary
/// or rate-limited, or when it is an [`std::io::Error`] whose kind names a
/// network fault the next attempt may get past.
pub fn is_retryable_error_source(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(link) = current {
        if let Some(opendal_error) = link.downcast_ref::<opendal::Error>()
            && (opendal_error.is_temporary() || opendal_error.kind() == opendal::ErrorKind::RateLimited)
        {
            return true;
        }
        if let Some(io_error) = link.downcast_ref::<io::Error>()
            && is_retryable_io_kind(io_error)
        {
            return true;
        }
        current = link.source();
    }
    false
}

/// Whether an IO error names a fault a repeat of the same call may get past.
fn is_retryable_io_kind(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wrap `source` the way `object_store_opendal` wraps everything it cannot
    /// map to a named variant.
    fn generic_error(source: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: source.into(),
        }
    }

    /// The case the old classifier got wrong: throttling and 5xx arrive as an
    /// `opendal::Error` marked temporary, with no `std::io::Error` anywhere in
    /// the chain.
    #[test]
    fn a_temporary_opendal_fault_is_retryable() {
        let error =
            generic_error(opendal::Error::new(opendal::ErrorKind::Unexpected, "503 from the store").set_temporary());

        assert!(is_retryable_object_store_error(&error));
    }

    #[test]
    fn a_permanent_opendal_fault_is_not_retryable() {
        let error = generic_error(opendal::Error::new(
            opendal::ErrorKind::PermissionDenied,
            "403 from the store",
        ));

        assert!(!is_retryable_object_store_error(&error));
    }

    #[test]
    fn a_missing_object_is_not_retryable() {
        let error = object_store::Error::NotFound {
            path: "queue/logs/0.parquet".to_string(),
            source: "no such key".into(),
        };

        assert!(!is_retryable_object_store_error(&error));
    }

    /// Regression guard for the path that already worked: a raw network error
    /// reaching the chain as an `std::io::Error`.
    #[test]
    fn a_network_io_fault_in_the_chain_is_retryable() {
        let error = generic_error(io::Error::from(io::ErrorKind::ConnectionReset));

        assert!(is_retryable_object_store_error(&error));
    }

    #[test]
    fn an_untyped_source_is_not_retryable() {
        let error = generic_error("deletion refused");

        assert!(!is_retryable_object_store_error(&error));
    }
}
