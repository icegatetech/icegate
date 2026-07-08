//! Tonic gRPC adapter for the memory-pressure guard (feature `shed-tonic`).
//!
//! [`MemoryShedInterceptor`] runs on `Request<()>` (metadata only) at the transport
//! layer — before protobuf decode, session build, and the handler — rejecting RPCs
//! with `RESOURCE_EXHAUSTED` while the node is under memory pressure. Shared by the
//! Flight SQL and OTLP gRPC surfaces.

use tonic::{Request, Status, service::Interceptor};

use super::guard::MemoryPressure;

/// Rejects RPCs with `RESOURCE_EXHAUSTED` while the node is under memory pressure.
///
/// Wrap the configured server with `InterceptedService::new(server, interceptor)` (NOT
/// codegen `with_interceptor`, which would rebuild the service and revert any
/// `max_decoding_message_size`). The reject is a graceful trailers-only response
/// (`grpc-status = 8`) that stock OTLP/ADBC clients treat as retryable-with-backoff;
/// the gRPC body is not drained (transport flow control stops the sender).
#[derive(Clone)]
pub struct MemoryShedInterceptor {
    guard: MemoryPressure,
    surface: &'static str,
}

impl MemoryShedInterceptor {
    /// Build an interceptor for one surface. `surface` is the metric attribute value
    /// (`"flight_sql" | "otlp_grpc"`).
    #[must_use]
    pub const fn new(guard: MemoryPressure, surface: &'static str) -> Self {
        Self { guard, surface }
    }
}

impl Interceptor for MemoryShedInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if self.guard.is_under_pressure() {
            self.guard.record_shed(self.surface);
            return Err(Status::resource_exhausted("service under memory pressure, retry later"));
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tonic::{Code, Request, service::Interceptor};

    use super::*;
    use crate::memory::guard::{MemoryPressure, MemoryPressureConfig, MemoryPressureSampler, UsageReader};

    /// Deterministic `UsageReader` returning a fixed working-set / limit.
    struct FixedReader {
        limit: u64,
        working_set: u64,
    }

    impl UsageReader for FixedReader {
        fn limit_bytes(&self) -> u64 {
            self.limit
        }

        fn read_working_set_bytes(&self) -> crate::error::Result<u64> {
            Ok(self.working_set)
        }
    }

    /// A handle driven into the pressured state (ratio 0.99 >= default high 0.90).
    fn pressured_handle() -> MemoryPressure {
        let cfg = MemoryPressureConfig::default();
        let sampler = MemoryPressureSampler::with_reader(
            &cfg,
            Arc::new(FixedReader {
                limit: 100,
                working_set: 99,
            }),
        );
        let handle = sampler.handle();
        sampler.sample_once().expect("sample_once must succeed for a fixed reader");
        assert!(handle.is_under_pressure(), "fixture must be under pressure");
        handle
    }

    #[test]
    fn rejects_with_resource_exhausted_when_pressured() {
        let mut interceptor = MemoryShedInterceptor::new(pressured_handle(), "test");
        let err = interceptor.call(Request::new(())).expect_err("must reject under pressure");
        assert_eq!(err.code(), Code::ResourceExhausted);
    }

    #[test]
    fn passes_when_inert() {
        let mut interceptor = MemoryShedInterceptor::new(MemoryPressure::inert(), "test");
        assert!(interceptor.call(Request::new(())).is_ok());
    }
}
