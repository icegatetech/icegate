//! `CatalogServerConfig::from_file` contract: the file formats the `serve`
//! command accepts, and the validation that must reject a configuration before
//! a listener is bound or S3 is touched.

use std::path::PathBuf;

use icegate_catalog_s3::{CatalogApiConfigError, CatalogPrefix, CatalogServerConfig, CatalogServerConfigError};
use tempfile::TempDir;

fn write_config(dir: &TempDir, filename: &str, contents: &str) -> PathBuf {
    let path = dir.path().join(filename);
    std::fs::write(&path, contents).expect("write config fixture");
    path
}

const VALID_YAML: &str = "\
http:
  host: 127.0.0.1
  port: 8181
  prefix: ice/warehouses/my
  default_page_size: 10
  max_page_size: 100
s3:
  bucket: catalog
  region: us-east-1
  endpoint: http://127.0.0.1:9000
  path_style_access: true
  access_key_id: access
  secret_access_key: secret
  warehouse: warehouse
  codec: json
";

const VALID_TOML: &str = "\
[http]
host = \"127.0.0.1\"
port = 8181
prefix = \"ice/warehouses/my\"
default_page_size = 10
max_page_size = 100

[s3]
bucket = \"catalog\"
region = \"us-east-1\"
endpoint = \"http://127.0.0.1:9000\"
path_style_access = true
access_key_id = \"access\"
secret_access_key = \"secret\"
warehouse = \"warehouse\"
codec = \"json\"
";

fn assert_full_config(config: &CatalogServerConfig, source: &str) {
    assert_eq!(config.http.host, "127.0.0.1", "{source}");
    assert_eq!(config.http.port, 8181, "{source}");
    assert_eq!(
        config.http.prefix.as_ref().map(CatalogPrefix::as_str),
        Some("ice/warehouses/my"),
        "{source}",
    );
    assert_eq!(config.http.default_page_size, 10, "{source}");
    assert_eq!(config.http.max_page_size, 100, "{source}");
    assert_eq!(config.s3.bucket, "catalog", "{source}");
    assert_eq!(config.s3.region, "us-east-1", "{source}");
    assert_eq!(config.s3.endpoint.as_deref(), Some("http://127.0.0.1:9000"), "{source}");
    assert_eq!(config.s3.path_style_access, Some(true), "{source}");
    assert_eq!(config.s3.access_key_id.as_deref(), Some("access"), "{source}");
    assert_eq!(config.s3.secret_access_key.as_deref(), Some("secret"), "{source}");
    assert_eq!(config.s3.warehouse, "warehouse", "{source}");
    assert_eq!(config.s3.codec, "json", "{source}");
}

/// One configuration, three accepted spellings: `.yaml`, `.yml`, and `.toml`
/// must load the same settings, or a deployment switching formats would
/// silently change server behavior.
#[test]
fn from_file_loads_the_same_settings_from_yaml_yml_and_toml() {
    let dir = TempDir::new().expect("config fixture directory");
    for (filename, contents) in [
        ("catalog.yaml", VALID_YAML),
        ("catalog.yml", VALID_YAML),
        ("catalog.toml", VALID_TOML),
    ] {
        let path = write_config(&dir, filename, contents);

        let config =
            CatalogServerConfig::from_file(&path).unwrap_or_else(|error| panic!("{filename} must load: {error}"));

        assert_full_config(&config, filename);
    }
}

/// The tri-state matters because an absent value is a decision in itself: the
/// catalog derives addressing from the endpoint only when the operator has not
/// spoken. See `resolve_path_style_access`.
#[test]
fn from_file_preserves_the_path_style_access_tri_state() {
    let dir = TempDir::new().expect("config fixture directory");
    for (value_line, expected) in [
        ("  path_style_access: true\n", Some(true)),
        ("  path_style_access: false\n", Some(false)),
        ("", None),
    ] {
        let contents = format!(
            "\
http:
  host: 127.0.0.1
  port: 8181
  default_page_size: 10
  max_page_size: 100
s3:
  bucket: catalog
  region: us-east-1
{value_line}  warehouse: warehouse
",
        );
        let path = write_config(&dir, "tri-state.yaml", &contents);

        let config = CatalogServerConfig::from_file(&path)
            .unwrap_or_else(|error| panic!("path_style_access {expected:?} must load: {error}"));

        assert_eq!(config.s3.path_style_access, expected);
    }
}

/// `serve` must fail on these before binding a listener or touching S3, and
/// with a config error rather than a late runtime failure.
///
/// The region cases stay quoted strings on purpose: an unquoted blank scalar is
/// YAML `null`, which fails `String` deserialization before the emptiness guard
/// under test ever runs.
#[test]
fn from_file_rejects_unreadable_unsupported_and_invalid_configurations() {
    let dir = TempDir::new().expect("config fixture directory");
    let yaml = |mutate: fn(&str) -> String| mutate(VALID_YAML);

    let cases: Vec<(&str, PathBuf, fn(&CatalogServerConfigError) -> bool)> = vec![
        ("missing file", dir.path().join("absent.yaml"), |error| {
            matches!(error, CatalogServerConfigError::Read { .. })
        }),
        (
            "unsupported extension",
            write_config(&dir, "catalog.json", VALID_YAML),
            |error| matches!(error, CatalogServerConfigError::UnsupportedExtension { .. }),
        ),
        (
            "extensionless path",
            write_config(&dir, "catalog", VALID_YAML),
            |error| matches!(error, CatalogServerConfigError::UnsupportedExtension { .. }),
        ),
        (
            "malformed YAML",
            write_config(&dir, "malformed.yaml", "http: [unterminated"),
            |error| matches!(error, CatalogServerConfigError::Parse(_)),
        ),
        (
            "malformed TOML",
            write_config(&dir, "malformed.toml", "[http\nhost = \"127.0.0.1\""),
            |error| matches!(error, CatalogServerConfigError::Parse(_)),
        ),
        (
            "empty region",
            write_config(
                &dir,
                "empty-region.yaml",
                &yaml(|base| base.replace("region: us-east-1", "region: \"\"")),
            ),
            |error| matches!(error, CatalogServerConfigError::EmptyRegion),
        ),
        (
            "whitespace-only region",
            write_config(
                &dir,
                "whitespace-region.yaml",
                &yaml(|base| base.replace("region: us-east-1", "region: \" \"")),
            ),
            |error| matches!(error, CatalogServerConfigError::EmptyRegion),
        ),
        (
            "access key without secret key",
            write_config(
                &dir,
                "access-only.yaml",
                &yaml(|base| base.replace("  secret_access_key: secret\n", "")),
            ),
            |error| matches!(error, CatalogServerConfigError::AccessKeyWithoutSecretKey),
        ),
        (
            "secret key without access key",
            write_config(
                &dir,
                "secret-only.yaml",
                &yaml(|base| base.replace("  access_key_id: access\n", "")),
            ),
            |error| matches!(error, CatalogServerConfigError::SecretKeyWithoutAccessKey),
        ),
        (
            // Only `http.host` mutates: touching the second `127.0.0.1` (the S3
            // endpoint) could let the case pass on an endpoint guard instead of
            // the listen-host guard under test.
            "unparsable listen host",
            write_config(
                &dir,
                "bad-host.yaml",
                &yaml(|base| base.replace("host: 127.0.0.1", "host: not a host")),
            ),
            |error| matches!(error, CatalogServerConfigError::InvalidListenAddress(_)),
        ),
        (
            "zero default page size",
            write_config(
                &dir,
                "zero-default.yaml",
                &yaml(|base| base.replace("default_page_size: 10", "default_page_size: 0")),
            ),
            |error| {
                matches!(
                    error,
                    CatalogServerConfigError::Api(CatalogApiConfigError::ZeroDefaultPageSize)
                )
            },
        ),
        (
            "zero max page size",
            write_config(
                &dir,
                "zero-max.yaml",
                &yaml(|base| base.replace("max_page_size: 100", "max_page_size: 0")),
            ),
            |error| {
                matches!(
                    error,
                    CatalogServerConfigError::Api(CatalogApiConfigError::ZeroMaxPageSize)
                )
            },
        ),
        (
            "default page size above the maximum",
            write_config(
                &dir,
                "inverted-pages.yaml",
                &yaml(|base| base.replace("max_page_size: 100", "max_page_size: 9")),
            ),
            |error| {
                matches!(
                    error,
                    CatalogServerConfigError::Api(CatalogApiConfigError::DefaultPageSizeExceedsMaximum)
                )
            },
        ),
        (
            "unsupported codec",
            write_config(
                &dir,
                "bad-codec.yaml",
                &yaml(|base| base.replace("codec: json", "codec: bincode")),
            ),
            |error| matches!(error, CatalogServerConfigError::Codec(_)),
        ),
    ];

    for (case, path, error_matches) in cases {
        let error = CatalogServerConfig::from_file(&path)
            .map(|_| ())
            .expect_err(&format!("{case} must fail to load"));

        assert!(error_matches(&error), "{case}: {error}");
    }
}

/// `serve` resolves the listener from the loaded file; host and port must come
/// through as one socket address.
#[test]
fn a_loaded_configuration_resolves_its_listen_address() {
    let dir = TempDir::new().expect("config fixture directory");
    let path = write_config(&dir, "catalog.yaml", VALID_YAML);

    let config = CatalogServerConfig::from_file(&path).expect("valid config");

    assert_eq!(
        config.listen_address().expect("listen address"),
        "127.0.0.1:8181".parse::<std::net::SocketAddr>().expect("socket address"),
    );
}
