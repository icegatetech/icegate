//! OTLP to Arrow transform utilities (shared by gRPC and HTTP).
//!
//! Split by signal: `logs` and `spans` each own their schema accessor and
//! `*_to_record_batch` entry point. Shared attribute-flattening, map-field, and
//! byte-validation helpers live in `attributes`.

mod attributes;
mod logs;
mod metrics;
mod spans;

pub use logs::{logs_arrow_schema, logs_to_record_batch};
pub use metrics::{metrics_arrow_schema, metrics_to_record_batch};
pub use spans::{spans_arrow_schema, spans_to_record_batch};
