//! Validated runtime configuration for the Catalog REST API.

use std::num::NonZeroUsize;

use serde::Deserialize;

/// Exact catalog prefix used in REST resource paths below `/v1`.
///
/// A prefix is a non-empty relative path of non-dot URL segments containing
/// only RFC 3986 unreserved characters. Keeping this validated prevents YAML
/// and TOML parsing from accepting a value the router cannot register exactly.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct CatalogPrefix(String);

/// Error returned when a catalog prefix cannot form an exact REST relative path.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("catalog prefix must be a non-empty relative path of RFC 3986 unreserved URL segments")]
pub struct CatalogPrefixError;

impl CatalogPrefix {
    /// Return the prefix text used in REST configuration and route paths.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CatalogPrefix {
    type Error = CatalogPrefixError;

    fn try_from(prefix: String) -> Result<Self, Self::Error> {
        if prefix.is_empty() || !prefix.split('/').all(is_valid_url_segment) {
            return Err(CatalogPrefixError);
        }
        Ok(Self(prefix))
    }
}

fn is_valid_url_segment(segment: &str) -> bool {
    !segment.is_empty() && !matches!(segment, "." | "..") && segment.bytes().all(is_unreserved_url_byte)
}

const fn is_unreserved_url_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

/// Rejected REST API settings.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogApiConfigError {
    /// The warehouse URI announced by `GET /v1/config` must identify a warehouse.
    #[error("catalog warehouse URI cannot be empty")]
    EmptyWarehouseUri,
    /// The Iceberg `pageSize` parameter has a minimum of one, so a zero default
    /// could never open a page.
    #[error("catalog default page size must be greater than zero")]
    ZeroDefaultPageSize,
    /// A zero limit would clamp every page to nothing.
    #[error("catalog maximum page size must be greater than zero")]
    ZeroMaxPageSize,
    /// The default page size must not exceed the page-size limit.
    #[error("catalog default page size must not exceed the maximum page size")]
    DefaultPageSizeExceedsMaximum,
}

/// Validated settings for the Catalog REST API.
///
/// This is the single validation point for REST behaviour: both the standalone
/// process and an embedded caller reach the router through it, so a router can
/// never be built on page-size bounds or a warehouse URI the handlers reject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogApiConfig {
    warehouse_uri: String,
    catalog_prefix: Option<CatalogPrefix>,
    default_page_size: NonZeroUsize,
    max_page_size: NonZeroUsize,
}

impl CatalogApiConfig {
    /// Validate REST API settings.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogApiConfigError`] when the warehouse URI is empty, either
    /// page-size bound is zero, or the default page size exceeds the limit.
    pub fn new(
        warehouse_uri: String,
        catalog_prefix: Option<CatalogPrefix>,
        default_page_size: usize,
        max_page_size: usize,
    ) -> Result<Self, CatalogApiConfigError> {
        if warehouse_uri.trim().is_empty() {
            return Err(CatalogApiConfigError::EmptyWarehouseUri);
        }
        let default_page_size =
            NonZeroUsize::new(default_page_size).ok_or(CatalogApiConfigError::ZeroDefaultPageSize)?;
        let max_page_size = NonZeroUsize::new(max_page_size).ok_or(CatalogApiConfigError::ZeroMaxPageSize)?;
        if default_page_size > max_page_size {
            return Err(CatalogApiConfigError::DefaultPageSizeExceedsMaximum);
        }
        Ok(Self {
            warehouse_uri,
            catalog_prefix,
            default_page_size,
            max_page_size,
        })
    }

    /// Warehouse URI announced by `GET /v1/config`.
    #[must_use]
    pub fn warehouse_uri(&self) -> &str {
        &self.warehouse_uri
    }

    /// Catalog prefix the router mounts the catalog resources under, if any.
    #[must_use]
    pub const fn catalog_prefix(&self) -> Option<&CatalogPrefix> {
        self.catalog_prefix.as_ref()
    }

    /// Page size applied to a paginated request that omits `pageSize`.
    #[must_use]
    pub const fn default_page_size(&self) -> NonZeroUsize {
        self.default_page_size
    }

    /// Upper bound on the number of records one page returns.
    #[must_use]
    pub const fn max_page_size(&self) -> NonZeroUsize {
        self.max_page_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_prefix_accepts_unreserved_relative_path() {
        let prefix = CatalogPrefix::try_from("ice/warehouses/my-._~42".to_string()).expect("valid catalog prefix");

        assert_eq!(prefix.as_str(), "ice/warehouses/my-._~42");
    }

    #[test]
    fn catalog_prefix_rejects_ambiguous_or_reserved_relative_paths() {
        let route_parameter_prefix = ["{", "prefix}"].concat();
        for prefix in [
            "",
            ".",
            "..",
            "/ice",
            "ice/",
            "ice//my",
            "ice/./my",
            "ice/../my",
            "catalog prefix",
            r"catalog\prefix",
            "catalog%2Fprefix",
            route_parameter_prefix.as_str(),
            "catalog?query",
            "catalog#fragment",
            "catalog\u{0007}",
        ] {
            assert_eq!(
                CatalogPrefix::try_from(prefix.to_string()),
                Err(CatalogPrefixError),
                "prefix {prefix:?}",
            );
        }
    }

    #[test]
    fn api_config_rejects_an_empty_warehouse_uri() {
        assert_eq!(
            CatalogApiConfig::new("   ".to_string(), None, 1, 2),
            Err(CatalogApiConfigError::EmptyWarehouseUri)
        );
    }

    #[test]
    fn api_config_rejects_zero_and_inverted_page_size_bounds() {
        assert_eq!(
            CatalogApiConfig::new("s3://warehouse".to_string(), None, 0, 2),
            Err(CatalogApiConfigError::ZeroDefaultPageSize)
        );
        assert_eq!(
            CatalogApiConfig::new("s3://warehouse".to_string(), None, 1, 0),
            Err(CatalogApiConfigError::ZeroMaxPageSize)
        );
        assert_eq!(
            CatalogApiConfig::new("s3://warehouse".to_string(), None, 3, 2),
            Err(CatalogApiConfigError::DefaultPageSizeExceedsMaximum)
        );
    }

    #[test]
    fn api_config_accepts_equal_page_size_bounds() {
        let config = CatalogApiConfig::new("s3://warehouse".to_string(), None, 2, 2).expect("valid api config");

        assert_eq!(config.default_page_size().get(), 2);
        assert_eq!(config.max_page_size().get(), 2);
        assert_eq!(config.warehouse_uri(), "s3://warehouse");
        assert_eq!(config.catalog_prefix(), None);
    }
}
