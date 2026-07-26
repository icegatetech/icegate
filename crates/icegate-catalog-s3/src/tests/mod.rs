// Integration tests module.
// These tests are kept in src/tests to access pub(crate) catalog internals.

mod catalog_test;
// `pub(crate)`: inline `#[cfg(test)]` modules outside this subtree (e.g.
// `cli::serve`) reuse the same harness instead of growing a parallel one.
pub(crate) mod common;
#[cfg(feature = "rest")]
mod rest_api_test;
