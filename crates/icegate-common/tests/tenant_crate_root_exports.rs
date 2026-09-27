//! The tenant items `icegate-ee` imports from this crate's root.
//!
//! An integration test because only a test outside the crate sees its root the
//! way an external consumer does: the unit tests of `tenant.rs` reach the same
//! functions through `use super::*`, so dropping a re-export from `lib.rs` breaks
//! `icegate-ee` while every test inside this crate still compiles. The import
//! below is the assertion; the call only keeps it from being unused.
//!
//! Removed together with `is_valid_tenant_id` — see the `TODO(high)` on it in
//! `src/tenant.rs`.
//!
//! ```text
//! cargo test -p icegate-common --test tenant_crate_root_exports
//! ```

use icegate_common::{DEFAULT_TENANT_ID, is_valid_tenant_id};

#[test]
fn the_crate_root_exports_the_default_tenant_and_the_tenant_id_rule() {
    assert!(is_valid_tenant_id(DEFAULT_TENANT_ID));
}
