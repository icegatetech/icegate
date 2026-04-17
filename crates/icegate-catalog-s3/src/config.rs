//! Configuration for the S3-backed catalog.

use crate::codec::CatalogCodecKind;

/// S3 catalog configuration.
#[derive(Debug, Clone)]
pub struct S3CatalogConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region.
    pub region: String,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
    /// Optional explicit S3 access key (mainly for local test environments).
    ///
    /// When omitted, credentials are resolved from the environment and default AWS provider chain.
    pub access_key_id: Option<String>,
    /// Optional explicit S3 secret key (mainly for local test environments).
    ///
    /// When omitted, credentials are resolved from the environment and default AWS provider chain.
    pub secret_access_key: Option<String>,
    /// S3 key prefix used as warehouse root (without leading/trailing `/`).
    pub warehouse: String,
    /// Serialization codec for catalog root and metadata.
    pub codec: CatalogCodecKind,
}
