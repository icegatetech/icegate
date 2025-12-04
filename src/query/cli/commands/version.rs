//! Version command implementation

use crate::common::errors::IceGateError;

/// Execute the version command
///
/// Shows version information about `IceGate` and its dependencies
#[allow(clippy::unnecessary_wraps, clippy::print_stdout)]
pub fn execute() -> Result<(), IceGateError> {
    println!("IceGate v{}", env!("CARGO_PKG_VERSION"));
    println!("Observability data lake query engine");
    println!();
    println!("Dependencies:");
    println!("  iceberg: 0.7.0"); // TODO: Get from Cargo.toml
    println!("  datafusion: 48");
    println!("  tokio: 1.48.0");
    println!();
    println!("Build:");
    println!("  Rust edition: 2024");
    #[cfg(debug_assertions)]
    println!("  Profile: debug");
    #[cfg(not(debug_assertions))]
    println!("  Profile: release");

    Ok(())
}