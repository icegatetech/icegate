//! Tenant resolution for a gRPC surface.
//!
//! Runs as a tonic interceptor on `Request<()>` — metadata only, before the
//! protobuf decode and the handler — so an RPC carrying no usable tenant costs
//! nothing beyond reading its headers.
//!
//! Shared by every gRPC surface of the project (OTLP ingest, Flight SQL read):
//! the policy, the refusal status and the extension the handler reads are one
//! contract, and a second copy of it would let two surfaces answer the same
//! request differently.

use tonic::{Request, Status, metadata::MetadataMap, service::Interceptor};

use crate::tenant::{TENANT_ID_HEADER, TenantHeader, TenantRejection, TenantRejectionRecorder, TenantResolver};

/// Rejects RPCs that carry no usable tenant, and hands the resolved
/// [`TenantId`](crate::TenantId) to the handler through the request extensions.
///
/// One instance per wrapped server: `protocol` is a property of the surface, so
/// it is captured at construction rather than derived from the request on every
/// call.
///
/// Wrap the configured server with `InterceptedService::new(server, interceptor)`
/// (NOT codegen `with_interceptor`, which would rebuild the service and revert
/// any `max_decoding_message_size`).
#[derive(Clone)]
pub struct TenantPolicyInterceptor<R: TenantRejectionRecorder> {
    resolver: TenantResolver,
    recorder: R,
    protocol: &'static str,
}

impl<R: TenantRejectionRecorder> TenantPolicyInterceptor<R> {
    /// Build an interceptor for one surface. `protocol` is the metric attribute
    /// value the refusals of this server are counted under.
    #[must_use]
    pub const fn new(resolver: TenantResolver, recorder: R, protocol: &'static str) -> Self {
        Self {
            resolver,
            recorder,
            protocol,
        }
    }
}

impl<R: TenantRejectionRecorder> Interceptor for TenantPolicyInterceptor<R> {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let outcome = self.resolver.resolve_tenant(read_tenant_metadata(request.metadata()));
        match outcome {
            Ok(tenant) => {
                request.extensions_mut().insert(tenant);
                Ok(request)
            }
            Err(rejection) => {
                self.recorder.add_tenant_rejection(self.protocol, rejection.reason());
                Err(reject_request(rejection))
            }
        }
    }
}

/// Read [`TENANT_ID_HEADER`] from the request metadata as the policy input.
///
/// Only the mapping from a gRPC metadata map; what the values mean is
/// [`TenantHeader::from_values`], which the HTTP surfaces reach through
/// [`TenantHeader::from_header_map`].
fn read_tenant_metadata(metadata: &MetadataMap) -> TenantHeader<'_> {
    TenantHeader::from_values(metadata.get_all(TENANT_ID_HEADER).iter().map(|value| value.to_str().ok()))
}

/// The status an RPC carrying no usable tenant is refused with.
///
/// `INVALID_ARGUMENT` rather than `UNAUTHENTICATED`: the request is malformed as
/// far as this deployment is concerned, and no credential would make it succeed
/// as sent.
fn reject_request(rejection: TenantRejection) -> Status {
    Status::invalid_argument(rejection.message())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tonic::{Code, metadata::MetadataValue};

    use super::*;
    use crate::tenant::{TenantId, TenantPolicy};

    /// Recorder that keeps what it was called with, so a test can assert on the
    /// port rather than on a metric exported behind it.
    #[derive(Clone, Default)]
    struct RecordedRejections(Arc<Mutex<Vec<(String, String)>>>);

    impl RecordedRejections {
        fn calls(&self) -> Vec<(String, String)> {
            self.0.lock().expect("the recorder mutex is never poisoned").clone()
        }
    }

    impl TenantRejectionRecorder for RecordedRejections {
        fn add_tenant_rejection(&self, protocol: &str, reason: &str) {
            self.0
                .lock()
                .expect("the recorder mutex is never poisoned")
                .push((protocol.to_string(), reason.to_string()));
        }
    }

    const TEST_PROTOCOL: &str = "test_grpc";

    fn multi_interceptor() -> TenantPolicyInterceptor<RecordedRejections> {
        TenantPolicyInterceptor::new(TenantResolver::Multi, RecordedRejections::default(), TEST_PROTOCOL)
    }

    fn request_with_tenants(values: &[&str]) -> Request<()> {
        let mut request = Request::new(());
        for value in values {
            request.metadata_mut().append(
                TENANT_ID_HEADER,
                MetadataValue::try_from(*value).expect("ascii metadata value"),
            );
        }
        request
    }

    #[test]
    fn rejects_with_invalid_argument_when_the_header_is_absent_in_multi() {
        let status = multi_interceptor()
            .call(Request::new(()))
            .expect_err("multi must refuse an RPC with no tenant header");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn rejects_a_duplicated_header() {
        let status = multi_interceptor()
            .call(request_with_tenants(&["acme", "acme"]))
            .expect_err("a duplicated header is refused even when the values agree");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn rejects_a_header_naming_another_tenant_in_single() {
        let resolver = TenantPolicy::Single { id: "acme".to_string() }
            .into_resolver()
            .expect("valid single policy");
        let status = TenantPolicyInterceptor::new(resolver, RecordedRejections::default(), TEST_PROTOCOL)
            .call(request_with_tenants(&["victim"]))
            .expect_err("single must refuse a header naming another tenant");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    /// A value present but not readable as text must not read as an absent
    /// header: in `single` that would serve the deployment's own tenant to a
    /// request that named some other one. The bytes are accepted by tonic's
    /// ASCII metadata and refused by `to_str`.
    #[test]
    fn rejects_an_unreadable_header_as_an_invalid_tenant_in_single() {
        let resolver = TenantPolicy::Single { id: "acme".to_string() }
            .into_resolver()
            .expect("valid single policy");
        let recorder = RecordedRejections::default();
        let mut request = Request::new(());
        request.metadata_mut().insert(
            TENANT_ID_HEADER,
            MetadataValue::try_from(&[0xff_u8, 0xfe][..]).expect("opaque ascii metadata value"),
        );

        let status = TenantPolicyInterceptor::new(resolver, recorder.clone(), TEST_PROTOCOL)
            .call(request)
            .expect_err("an unreadable header must not resolve to the deployment's tenant");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            recorder.calls(),
            vec![(TEST_PROTOCOL.to_string(), "invalid".to_string())]
        );
    }

    /// The whole of what this crate owes the component: one call, carrying the
    /// surface's protocol and the reason the resolver returned. What the
    /// component then labels and exports is its own test.
    #[test]
    fn a_refused_rpc_calls_the_recorder_once_with_the_protocol_and_the_reason() {
        let recorder = RecordedRejections::default();
        TenantPolicyInterceptor::new(TenantResolver::Multi, recorder.clone(), TEST_PROTOCOL)
            .call(request_with_tenants(&["acme", "acme"]))
            .expect_err("a duplicated header is refused");

        assert_eq!(
            recorder.calls(),
            vec![(
                TEST_PROTOCOL.to_string(),
                TenantRejection::DuplicateHeader.reason().to_string()
            )]
        );
    }

    #[test]
    fn an_admitted_rpc_records_nothing() {
        let recorder = RecordedRejections::default();
        TenantPolicyInterceptor::new(TenantResolver::Multi, recorder.clone(), TEST_PROTOCOL)
            .call(request_with_tenants(&["tenant-b"]))
            .expect("a valid header resolves");

        assert!(recorder.calls().is_empty(), "an admitted RPC is not a rejection");
    }

    #[test]
    fn puts_the_resolved_tenant_into_extensions() {
        let request = multi_interceptor()
            .call(request_with_tenants(&["tenant-b"]))
            .expect("a valid header resolves");
        let tenant = request
            .extensions()
            .get::<TenantId>()
            .expect("the interceptor must place the tenant in the extensions");
        assert_eq!(tenant.as_ref(), "tenant-b");
    }
}
