//! Every S3 client maintain builds signs with EKS Pod Identity credentials when
//! no static keys are configured.
//!
//! Maintain reaches S3 through three clients, each resolving credentials on its
//! own: `object_store` under the S3 catalog, `OpenDAL` under the data and WAL
//! operator registry, and the AWS SDK under jobmanager's job state. Under Pod
//! Identity none of them is given keys; each has to find the container
//! credentials endpoint named by `AWS_CONTAINER_CREDENTIALS_FULL_URI` and
//! present the token in `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` (the AWS SDKs
//! reference guide, "Container credential provider").
//!
//! The credential chains read the process environment, and `unsafe_code =
//! "forbid"` rules out `std::env::set_var`, so the outer test re-executes this
//! test binary with exactly that environment and runs the ignored inner test in
//! it. The credentials endpoint and S3 are both local recording fakes; the outer
//! test asserts on what they received.
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::HashMap;
use std::process::Command;
use std::time::Duration;

use common::recording_endpoint::{RecordedRequest, RecordingEndpoint};
use icegate_common::storage::{S3Config, StorageBackend};
use icegate_common::{CancellationToken, CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle};
use icegate_maintain::jobs::JobsStorageConfig;
use jobmanager::prelude::{JobsManager, TaskDefinition, task_fn};
use object_store::ObjectStoreExt;
use object_store::path::Path;

/// The inner test the outer one runs in the child process.
const INNER_TEST: &str = "sign_one_request_per_s3_client";

/// Names the S3 endpoint the child's clients address.
const S3_ENDPOINT_VARIABLE: &str = "ICEGATE_TEST_S3_ENDPOINT";

/// Bucket of the S3 catalog's state.
const CATALOG_BUCKET: &str = "catalog-bucket";

/// Bucket the data and WAL operator registry resolves.
const DATA_BUCKET: &str = "data-bucket";

/// Bucket of jobmanager's job state.
const JOBS_BUCKET: &str = "jobs-bucket";

/// Region every client is configured with.
const REGION: &str = "us-east-1";

/// The temporary credentials the fake container endpoint hands out.
const POD_ACCESS_KEY_ID: &str = "ASIAPODIDENTITYTEST";
const POD_SECRET_ACCESS_KEY: &str = "pod-identity-secret";
const POD_SESSION_TOKEN: &str = "pod-session-token";

/// Far enough ahead that no client refreshes the credentials mid-test.
const POD_CREDENTIALS_EXPIRATION: &str = "2099-01-01T00:00:00Z";

/// The token a pod presents to the container credentials endpoint.
const POD_AUTHORIZATION_TOKEN: &str = "pod-auth-token";

/// Bound on the child process. It runs one read per client against local
/// fakes, so reaching this means a client is stuck, not slow.
const CHILD_TIMEOUT: Duration = Duration::from_secs(120);

/// The child process signs a request to each of the three buckets, one per
/// client, with the credentials the container endpoint served, and every
/// credentials request presents the pod's token.
#[tokio::test]
async fn pod_identity_credentials_sign_every_s3_client() {
    let credentials_endpoint = RecordingEndpoint::start(answer_with_pod_identity_credentials);
    let s3_endpoint = RecordingEndpoint::start(answer_s3_request);
    let pod_files = tempfile::tempdir().expect("temporary directory for the pod's files");
    let token_file = pod_files.path().join("eks-pod-identity-token");
    // No trailing newline, so the expected header value is the file's content
    // byte for byte.
    std::fs::write(&token_file, POD_AUTHORIZATION_TOKEN).expect("write the pod's token file");

    let mut command = Command::new(std::env::current_exe().expect("path of this test binary"));
    command.args(["--exact", INNER_TEST, "--ignored", "--nocapture"]);
    // Every inherited AWS_* variable goes — the static keys, AWS_SESSION_TOKEN,
    // AWS_PROFILE and AWS_WEB_IDENTITY_TOKEN_FILE among them — and the profile
    // files point at paths that do not exist, so the container endpoint is the
    // only source of credentials any chain can find.
    for (name, _) in std::env::vars_os() {
        if name.as_encoded_bytes().starts_with(b"AWS_") {
            command.env_remove(name);
        }
    }
    command
        .env(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            format!("{}/v1/credentials", credentials_endpoint.url()),
        )
        .env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", &token_file)
        .env("AWS_REGION", REGION)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_CONFIG_FILE", pod_files.path().join("absent-config"))
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            pod_files.path().join("absent-credentials"),
        )
        .env(S3_ENDPOINT_VARIABLE, s3_endpoint.url());
    let mut child = tokio::process::Command::from(command);
    // A timed-out child is killed when the dropped future drops it.
    child.kill_on_drop(true);

    let output = tokio::time::timeout(CHILD_TIMEOUT, child.output())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the child test did not finish within {CHILD_TIMEOUT:?}; S3 requests so far:\n{}",
                describe_requests(&s3_endpoint.requests())
            )
        })
        .expect("run the child test process");

    let s3_requests = s3_endpoint.requests();
    let report = format!(
        "child exit: {}\nS3 requests:\n{}\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        output.status,
        describe_requests(&s3_requests),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "the child test failed\n{report}");
    for bucket in [CATALOG_BUCKET, DATA_BUCKET, JOBS_BUCKET] {
        assert!(
            s3_requests
                .iter()
                .any(|request| names_bucket(request, bucket) && is_signed_with_pod_identity(request)),
            "no request to {bucket} was signed with the Pod Identity credentials\n{report}"
        );
    }

    let credentials_requests = credentials_endpoint.requests();
    assert!(
        !credentials_requests.is_empty(),
        "no client fetched credentials from the container endpoint\n{report}"
    );
    for request in &credentials_requests {
        assert_eq!(
            header_value(request, "authorization"),
            Some(POD_AUTHORIZATION_TOKEN),
            "credentials request {} {} did not present the pod's token",
            request.method,
            request.target
        );
    }
}

/// Builds the S3 catalog, the data registry and a job-state pool through the
/// constructors `maintain run` uses, with no key anywhere, against the endpoint
/// named by `ICEGATE_TEST_S3_ENDPOINT`, and sends one read through each.
///
/// Run only as the child process of
/// [`pod_identity_credentials_sign_every_s3_client`], which provides the
/// environment and asserts on what the fakes received. Ignoring it loses no
/// coverage: the outer test runs it on every `cargo test`.
#[tokio::test]
#[ignore = "GH-207: runs only as the child of pod_identity_credentials_sign_every_s3_client"]
async fn sign_one_request_per_s3_client() {
    let s3_endpoint = std::env::var(S3_ENDPOINT_VARIABLE)
        .expect("ICEGATE_TEST_S3_ENDPOINT; run through pod_identity_credentials_sign_every_s3_client");
    let backend = StorageBackend::S3(S3Config {
        bucket: DATA_BUCKET.to_string(),
        region: REGION.to_string(),
        endpoint: Some(s3_endpoint.clone()),
        access_key_id: None,
        secret_access_key: None,
    });
    let io = IoHandle::from_config(None, Some(&backend))
        .await
        .expect("io handle over the data backend");

    let catalog = CatalogBuilder::from_config(&catalog_config(&s3_endpoint), &io, CancellationToken::new())
        .await
        .expect("build the S3 catalog");
    // The fake answers the GET of the root with 404, which the catalog reads
    // as a catalog with nothing in it; any other failure is an error here.
    let namespaces = catalog.list_namespaces(None).await.expect("read the catalog root");
    assert!(
        namespaces.is_empty(),
        "an absent root reads as no namespaces, got {namespaces:?}"
    );

    let (data_store, prefix) = io
        .object_store_operator_registry()
        .resolve_object_store(&format!("s3://{DATA_BUCKET}/wal"))
        .expect("resolve the data store");
    data_store
        .head(&Path::from(format!("{prefix}/probe")))
        .await
        .expect("stat an object in the data bucket");

    let jobs_storage = JobsStorageConfig {
        endpoint: Some(s3_endpoint),
        bucket: JOBS_BUCKET.to_string(),
        region: REGION.to_string(),
        ..JobsStorageConfig::default()
    };
    // `build` checks the bucket with a HEAD before it returns. The builder
    // refuses a pool without a job, so the pool gets one; it is never started,
    // so the job never runs.
    JobsManager::builder()
        .s3(jobs_storage
            .to_s3_storage_config("compaction")
            .expect("job-state storage configuration"))
        .job("pod-identity-probe", |job| {
            job.add_task(
                TaskDefinition::new("probe", Duration::from_secs(5)),
                task_fn(|_context| async { Ok(Vec::new().into()) }),
            );
        })
        .build()
        .await
        .expect("build the job-state pool");
}

/// The catalog block of `config/docker/maintain.yaml` without its keys.
fn catalog_config(s3_endpoint: &str) -> CatalogConfig {
    CatalogConfig {
        backend: CatalogBackend::S3 {
            warehouse: "catalog".to_string(),
        },
        warehouse: format!("s3://{CATALOG_BUCKET}/"),
        properties: HashMap::from([
            ("bucket".to_string(), CATALOG_BUCKET.to_string()),
            ("region".to_string(), REGION.to_string()),
            ("endpoint".to_string(), s3_endpoint.to_string()),
        ]),
        cache: None,
    }
}

/// The container credentials endpoint: the same temporary credentials for
/// every request. Whether a request presented the pod's token is asserted
/// afterwards rather than refused here, so a missing token shows up as that
/// assertion instead of as a client error in the child.
fn answer_with_pod_identity_credentials(_request: &RecordedRequest) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let credentials = serde_json::json!({
        "AccessKeyId": POD_ACCESS_KEY_ID,
        "SecretAccessKey": POD_SECRET_ACCESS_KEY,
        "Token": POD_SESSION_TOKEN,
        "Expiration": POD_CREDENTIALS_EXPIRATION,
    });
    (
        200,
        vec![("content-type".to_string(), "application/json".to_string())],
        credentials.to_string().into_bytes(),
    )
}

/// S3: `HEAD` gets 200 with an empty body, anything else 404. Each client's one
/// read then comes back without an error that could hide a signing failure.
fn answer_s3_request(request: &RecordedRequest) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let status = if request.method == "HEAD" { 200 } else { 404 };
    (status, Vec::new(), Vec::new())
}

/// Whether `request` addresses `bucket`. Every client here is given an
/// endpoint and addresses it path-style, so the bucket is the first segment of
/// the request path; a virtual-hosted request would not reach the fake at all.
fn names_bucket(request: &RecordedRequest, bucket: &str) -> bool {
    request.target.strip_prefix('/').and_then(|path| path.split(['/', '?']).next()) == Some(bucket)
}

/// Whether `request` is signed with the credentials the container endpoint
/// served: their key id in the signature's credential scope, their session
/// token in its own header.
fn is_signed_with_pod_identity(request: &RecordedRequest) -> bool {
    let credential_scope = format!("Credential={POD_ACCESS_KEY_ID}/");
    header_value(request, "x-amz-security-token") == Some(POD_SESSION_TOKEN)
        && header_value(request, "authorization").is_some_and(|authorization| authorization.contains(&credential_scope))
}

/// The value of the header field `name`, given in lower case (the endpoint
/// records names lower-cased).
fn header_value<'a>(request: &'a RecordedRequest, name: &str) -> Option<&'a str> {
    request
        .headers
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value.as_str())
}

/// One line per request: what it addressed, which key id signed it, and which
/// session token it carried.
fn describe_requests(requests: &[RecordedRequest]) -> String {
    requests
        .iter()
        .map(|request| {
            let key_id = header_value(request, "authorization")
                .and_then(|authorization| authorization.split("Credential=").nth(1))
                .and_then(|scope| scope.split('/').next())
                .unwrap_or("<unsigned>");
            let session_token = header_value(request, "x-amz-security-token").unwrap_or("<none>");
            format!(
                "  {} {} key_id={key_id} session_token={session_token}",
                request.method, request.target
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}
