//! OTLP to Arrow transform utilities (shared by gRPC and HTTP).
//!
//! Split by signal: `logs`, `metrics`, `operations`, and `spans` each own their schema
//! accessor and `*_to_record_batch` entry point. Shared attribute-flattening, map-field,
//! and byte-validation helpers live in `attributes`.

mod attributes;
mod logs;
mod metrics;
pub(crate) mod operations;
mod spans;

pub use logs::{logs_arrow_schema, logs_to_record_batch};
pub use metrics::{metrics_arrow_schema, metrics_to_record_batch};
pub use operations::{operations_arrow_schema, operations_to_record_batch};
pub use spans::{spans_arrow_schema, spans_to_record_batch};

/// Fixtures shared by the tests of every signal, and by the tests of the callers
/// that drive a transform (`wal::batch`).
#[cfg(test)]
pub(crate) mod test_support {
    use icegate_common::TenantId;

    /// The tenant a transform case writes to, as the newtype the entry points
    /// take.
    ///
    /// # Panics
    ///
    /// Panics when `id` is not a usable tenant identifier, which in a test is a
    /// broken fixture rather than an input to handle.
    pub(crate) fn test_tenant(id: &str) -> TenantId {
        TenantId::new(id).expect("a test fixture names a valid tenant identifier")
    }
}

#[cfg(test)]
mod tests {
    /// The operations module must be reachable both path-qualified (the handler
    /// wiring contract) and via the flat re-export (parity with logs/spans).
    #[test]
    fn operations_entry_points_are_reachable() {
        let _path_qualified = super::operations::operations_arrow_schema();
        let _re_exported = super::operations_arrow_schema();
    }
}
