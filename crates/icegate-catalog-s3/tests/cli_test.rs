//! `catalog` binary contract: argument parsing, the version the packaged
//! binary reports, and the failure paths `execute` must surface before a
//! server is considered started.

use clap::Parser;
use icegate_catalog_s3::CatalogServerConfigError;
use icegate_catalog_s3::cli::{CatalogCli, CliError};
use tempfile::TempDir;

/// The clap surface the binary exposes: exactly two subcommands, and `serve`
/// requires its `--config`.
#[test]
fn cli_parses_only_the_advertised_subcommands() {
    for (case, arguments, must_parse) in [
        (
            "serve with config",
            vec!["catalog", "serve", "--config", "catalog.yaml"],
            true,
        ),
        ("version", vec!["catalog", "version"], true),
        ("serve without config", vec!["catalog", "serve"], false),
        ("unknown subcommand", vec!["catalog", "bogus"], false),
        ("no subcommand", vec!["catalog"], false),
    ] {
        assert_eq!(
            CatalogCli::try_parse_from(&arguments).is_ok(),
            must_parse,
            "{case}: {arguments:?}",
        );
    }
}

/// `version` is what deployment smoke checks read, so the packaged binary must
/// print exactly the package version and nothing else. Runs the real binary:
/// an in-process call could not catch a broken entrypoint or stray output.
#[test]
fn version_prints_exactly_the_package_version() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_catalog"))
        .arg("version")
        .output()
        .expect("run catalog binary");

    assert!(output.status.success(), "catalog version must succeed: {output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout is UTF-8"),
        format!("{}\n", env!("CARGO_PKG_VERSION")),
    );
    assert!(
        output.stderr.is_empty(),
        "catalog version must not write to stderr: {:?}",
        String::from_utf8_lossy(&output.stderr),
    );
}

#[tokio::test]
async fn serve_fails_on_a_missing_config_file() {
    let dir = TempDir::new().expect("config fixture directory");
    let absent = dir.path().join("absent.yaml");
    let cli = CatalogCli::try_parse_from(["catalog", "serve", "--config", absent.to_str().expect("config path")])
        .expect("parse serve command");

    let error = cli.execute().await.expect_err("missing config must fail");

    assert!(
        matches!(error, CliError::Config(CatalogServerConfigError::Read { .. })),
        "{error}"
    );
}
