//! Version command implementation

use crate::error::IngestError;

/// Execute the version command
///
/// Shows version information about `IceGate` ingest
#[allow(clippy::unnecessary_wraps, clippy::print_stdout)]
pub fn execute() -> Result<(), IngestError> {
    println!("IceGate Ingest v{}", env!("CARGO_PKG_VERSION"));
    println!("OTLP data ingestion for observability data lake");
    println!();
    println!("Endpoints:");
    println!("  OTLP/HTTP: :4318 (default)");
    println!("  OTLP/gRPC: :4317 (default)");
    println!();
    println!("Build:");
    println!("  Rust edition: 2024");
    #[cfg(debug_assertions)]
    println!("  Profile: debug");
    #[cfg(not(debug_assertions))]
    println!("  Profile: release");

    Ok(())
}
