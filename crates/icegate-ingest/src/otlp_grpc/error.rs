//! OTLP gRPC error handling.
//!
//! Provides error-to-gRPC-status conversion for the OTLP gRPC API via the
//! [`GrpcError`] newtype wrapper implementing conversion to `tonic::Status`.

use tonic::{Code, Status};

use crate::error::IngestError;

/// Newtype wrapper for `IngestError` that implements conversion to
/// `tonic::Status`.
///
/// Enables idiomatic error handling in gRPC service handlers.
#[derive(Debug)]
pub struct GrpcError(pub IngestError);

impl From<IngestError> for GrpcError {
    fn from(err: IngestError) -> Self {
        Self(err)
    }
}

impl From<GrpcError> for Status {
    fn from(err: GrpcError) -> Self {
        let (code, message) = match &err.0 {
            // InvalidArgument - client sent invalid data
            IngestError::Decode(_) | IngestError::Validation(_) => (Code::InvalidArgument, err.0.to_string()),

            // Unimplemented - feature not yet available
            IngestError::NotImplemented(_) => (Code::Unimplemented, err.0.to_string()),

            // Internal - server-side errors
            IngestError::Io(_)
            | IngestError::Queue(_)
            | IngestError::Config(_)
            | IngestError::Iceberg(_)
            | IngestError::Join(_)
            | IngestError::Shift(_)
            | IngestError::ShiftQueueRead(_)
            | IngestError::Arrow(_)
            | IngestError::Other(_)
            | IngestError::Multiple(_) => (Code::Internal, err.0.to_string()),

            IngestError::Cancelled => (Code::Cancelled, err.0.to_string()),

            // Retryable load-shedding: `RESOURCE_EXHAUSTED` when the WAL channel is
            // full (request not enqueued), `UNAVAILABLE` when retries are exhausted
            // or the durability ack exceeds its deadline (GH-158).
            IngestError::QueueFull => (Code::ResourceExhausted, err.0.to_string()),
            IngestError::MaxAttemptsReached | IngestError::AckTimeout => (Code::Unavailable, err.0.to_string()),
        };

        Self::new(code, message)
    }
}

#[cfg(test)]
mod tests {
    use tonic::{Code, Status};

    use super::GrpcError;
    use crate::error::IngestError;

    #[test]
    fn queue_full_maps_to_resource_exhausted() {
        assert_eq!(
            Status::from(GrpcError(IngestError::QueueFull)).code(),
            Code::ResourceExhausted
        );
    }

    #[test]
    fn ack_timeout_maps_to_unavailable() {
        assert_eq!(
            Status::from(GrpcError(IngestError::AckTimeout)).code(),
            Code::Unavailable
        );
    }
}
