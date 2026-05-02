//! Configuration for the S3-backed catalog.

use std::sync::Arc;

use crate::codec::{CatalogCodec, json::JsonCatalogCodec};
use crate::error::{Error, Result};

/// Catalog root serialization codec kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CatalogCodecKind {
    /// JSON serialization.
    #[default]
    Json,
}

impl CatalogCodecKind {
    pub(crate) fn into_codec(self) -> Arc<dyn CatalogCodec> {
        match self {
            Self::Json => Arc::new(JsonCatalogCodec),
        }
    }
}

impl std::str::FromStr for CatalogCodecKind {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "json" => Ok(Self::Json),
            other => Err(Error::InvalidMetadata(format!("Unsupported catalog codec: {other}"))),
        }
    }
}

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
    /// Serialization codec for catalog root only.
    pub codec: CatalogCodecKind,
}
