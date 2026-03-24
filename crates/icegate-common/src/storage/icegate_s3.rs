//! S3 configuration helpers forked from `iceberg-storage-opendal` at
//! commit `335961ae`.
//!
//! These functions translate Iceberg's property map into an `OpenDAL`
//! [`S3Config`](opendal::services::S3Config) and build an [`Operator`]
//! from it. They are used by [`IceGateStorage`](super::icegate_storage::IceGateStorage)
//! to construct S3 operators with the correct credentials and settings.
//!
//! Forked because `iceberg-storage-opendal` is a separate crate not
//! included in IceGate's workspace, and we need to inject custom
//! `OpenDAL` layers between operator creation and use.

use std::collections::HashMap;

use iceberg::io::{
    CLIENT_REGION, S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ASSUME_ROLE_ARN, S3_ASSUME_ROLE_EXTERNAL_ID,
    S3_ASSUME_ROLE_SESSION_NAME, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_PATH_STYLE_ACCESS,
    S3_REGION, S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, S3_SSE_KEY, S3_SSE_MD5, S3_SSE_TYPE,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::services::S3Config;
use opendal::{Configurator, Operator};
use url::Url;

/// Check whether a string represents a truthy boolean value.
///
/// Recognized values (case-insensitive): `"true"`, `"t"`, `"1"`, `"on"`.
pub fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}

/// Convert an [`opendal::Error`] into an [`iceberg::Error`].
pub fn from_opendal_error(e: opendal::Error) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, "Failure in doing io operation").with_source(e)
}

/// Parse an Iceberg property map into an `OpenDAL` [`S3Config`].
///
/// Handles endpoint, credentials, region, encryption (SSE-S3/KMS/custom),
/// assume-role settings, and anonymous/EC2 metadata toggles.
///
/// # Errors
///
/// Returns an error if `s3.sse.type` has an unrecognized value.
pub fn s3_config_parse(mut m: HashMap<String, String>) -> Result<S3Config> {
    let mut cfg = S3Config::default();

    if let Some(endpoint) = m.remove(S3_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    }
    if let Some(access_key_id) = m.remove(S3_ACCESS_KEY_ID) {
        cfg.access_key_id = Some(access_key_id);
    }
    if let Some(secret_access_key) = m.remove(S3_SECRET_ACCESS_KEY) {
        cfg.secret_access_key = Some(secret_access_key);
    }
    if let Some(session_token) = m.remove(S3_SESSION_TOKEN) {
        cfg.session_token = Some(session_token);
    }
    if let Some(region) = m.remove(S3_REGION) {
        cfg.region = Some(region);
    }
    // CLIENT_REGION overrides S3_REGION if both are set (last-write wins).
    if let Some(region) = m.remove(CLIENT_REGION) {
        cfg.region = Some(region);
    }
    if let Some(path_style_access) = m.remove(S3_PATH_STYLE_ACCESS) {
        cfg.enable_virtual_host_style = !is_truthy(&path_style_access);
    }
    if let Some(arn) = m.remove(S3_ASSUME_ROLE_ARN) {
        cfg.role_arn = Some(arn);
    }
    if let Some(external_id) = m.remove(S3_ASSUME_ROLE_EXTERNAL_ID) {
        cfg.external_id = Some(external_id);
    }
    if let Some(session_name) = m.remove(S3_ASSUME_ROLE_SESSION_NAME) {
        cfg.role_session_name = Some(session_name);
    }

    let s3_sse_key = m.remove(S3_SSE_KEY);
    if let Some(sse_type) = m.remove(S3_SSE_TYPE) {
        match sse_type.to_lowercase().as_str() {
            "none" => {}
            "s3" => {
                cfg.server_side_encryption = Some("AES256".to_string());
            }
            "kms" => {
                cfg.server_side_encryption = Some("aws:kms".to_string());
                cfg.server_side_encryption_aws_kms_key_id = s3_sse_key;
            }
            "custom" => {
                let key = s3_sse_key.ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("{S3_SSE_TYPE} is 'custom' (SSE-C) but {S3_SSE_KEY} is not set"),
                    )
                })?;
                cfg.server_side_encryption_customer_algorithm = Some("AES256".to_string());
                cfg.server_side_encryption_customer_key = Some(key);
                cfg.server_side_encryption_customer_key_md5 = m.remove(S3_SSE_MD5);
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid {S3_SSE_TYPE}: {sse_type}. Expected one of (custom, kms, s3, none)"),
                ));
            }
        }
    }

    if let Some(allow_anonymous) = m.remove(S3_ALLOW_ANONYMOUS)
        && is_truthy(&allow_anonymous)
    {
        cfg.allow_anonymous = true;
    }
    if let Some(disable_ec2_metadata) = m.remove(S3_DISABLE_EC2_METADATA)
        && is_truthy(&disable_ec2_metadata)
    {
        cfg.disable_ec2_metadata = true;
    }
    if let Some(disable_config_load) = m.remove(S3_DISABLE_CONFIG_LOAD)
        && is_truthy(&disable_config_load)
    {
        cfg.disable_config_load = true;
    }

    Ok(cfg)
}

/// Build an `OpenDAL` [`Operator`] from an [`S3Config`] and an S3 URL.
///
/// Parses `path` (e.g. `s3://bucket/prefix/file.json`) to extract the
/// bucket name, then constructs a bare operator **without** any layers.
/// Callers are responsible for adding tracing, metrics, and caching
/// layers before calling `.finish()`.
///
/// # Arguments
///
/// * `cfg` — parsed S3 configuration (region, credentials, encryption, …)
/// * `path` — full S3 URL used to determine the bucket
///
/// # Errors
///
/// Returns an error if the URL cannot be parsed or has no host (bucket).
pub fn s3_config_build(cfg: &S3Config, path: &str) -> Result<Operator> {
    let url =
        Url::parse(path).map_err(|e| Error::new(ErrorKind::DataInvalid, format!("Invalid s3 url: {path}: {e}")))?;
    let bucket = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid s3 url: {path}, missing bucket"),
        )
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);

    Operator::new(builder)
        .map_err(from_opendal_error)
        .map(opendal::OperatorBuilder::finish)
}
