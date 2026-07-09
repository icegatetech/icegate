//! Container memory-pressure request-shedding guard.
//!
//! A single background sampler reads the cgroup working-set and publishes a
//! lock-free pressure flag; the axum/tonic adapters shed new requests while the
//! flag is set. Inert when no finite cgroup limit exists (dev/CI/bare-metal).
pub mod cgroup;
#[cfg(feature = "shed-tonic")]
pub mod grpc;
pub mod guard;
pub mod http;

#[cfg(feature = "shed-tonic")]
pub use grpc::MemoryShedInterceptor;
pub use guard::{MemoryPressure, MemoryPressureConfig, MemoryPressureSampler, UsageReader};
pub use http::{SHED_RETRY_AFTER_SECS, ShedPolicy, default_shed_response, shed_when_pressured};
