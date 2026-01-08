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
            | IngestError::Other(_)
            | IngestError::Multiple(_) => (Code::Internal, err.0.to_string()),
        };

        Self::new(code, message)
    }
}
