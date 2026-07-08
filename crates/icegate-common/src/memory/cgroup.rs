//! cgroup v1/v2 working-set readers and platform detection.
//!
//! `std::fs`-only sources for the memory-pressure sampler's numerator. The
//! concrete v1/v2 readers and the `Limit` enum are private to this module;
//! only [`cgroupfs_present`] and [`detect_default_reader`] are public, so the
//! readers carry no `missing_docs` burden. No `unsafe`, no new dependencies.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::guard::UsageReader;
use crate::error::{CommonError, Result};

/// Page-size-robust "unlimited" sentinel for cgroup v1 `memory.limit_in_bytes`.
///
/// The kernel writes v1's unlimited limit as `(LONG_MAX / page_size) * page_size`,
/// which is *smallest* for the *largest* page size. arm64 kernels can use 64 `KiB`
/// pages, giving `0x7FFF_FFFF_FFFF_0000` — smaller than the 4 `KiB` value
/// `0x7FFF_FFFF_FFFF_F000`. Images are multi-arch, so any limit at or above this
/// 64 `KiB` value is treated as unlimited (→ an inert guard).
const PAGE_COUNTER_UNLIMITED: u64 = 0x7FFF_FFFF_FFFF_0000;

/// Parsed cgroup memory limit: a finite positive byte count, or unlimited.
enum Limit {
    Bytes(u64),
    Unlimited,
}

/// Parse a cgroup v2 `memory.max` value.
///
/// The literal `"max"` is unlimited; otherwise a `u64` byte count where `0` is
/// also treated as unlimited (a degenerate never-run state — yields an inert
/// guard and avoids a divide-by-zero in the ratio).
fn parse_v2_limit(raw: &str) -> Result<Limit> {
    let trimmed = raw.trim();
    if trimmed == "max" {
        return Ok(Limit::Unlimited);
    }
    let value = trimmed
        .parse::<u64>()
        .map_err(|_| CommonError::Config(format!("invalid memory.max value: {trimmed}")))?;
    Ok(if value == 0 {
        Limit::Unlimited
    } else {
        Limit::Bytes(value)
    })
}

/// Parse a cgroup v1 `memory.limit_in_bytes` value.
///
/// `0` or any value at/above [`PAGE_COUNTER_UNLIMITED`] is unlimited; every
/// other value is a finite limit.
fn parse_v1_limit(raw: &str) -> Result<Limit> {
    let trimmed = raw.trim();
    let value = trimmed
        .parse::<u64>()
        .map_err(|_| CommonError::Config(format!("invalid memory.limit_in_bytes value: {trimmed}")))?;
    Ok(if value == 0 || value >= PAGE_COUNTER_UNLIMITED {
        Limit::Unlimited
    } else {
        Limit::Bytes(value)
    })
}

/// Read the value of an exact key from a `memory.stat`-format file.
///
/// Each line is `"<key> <value>"`. The FIRST whitespace-delimited token is
/// compared for EXACT equality to `key`: v1's `memory.stat` lists both
/// `inactive_file` and the hierarchical `total_inactive_file`, so a
/// substring/prefix match would select the wrong line for a cgroup with
/// children.
///
/// # Errors
/// `CommonError::Io` if the file cannot be read; `CommonError::Config` if the
/// key is absent or its value is not a `u64`. A missing key is an error (not a
/// silent `0`) so the sampler retains its last state rather than over-counting.
fn read_stat_key(path: &Path, key: &str) -> Result<u64> {
    let contents = std::fs::read_to_string(path)?;
    for line in contents.lines() {
        let mut tokens = line.split_whitespace();
        if tokens.next() == Some(key) {
            let value = tokens
                .next()
                .ok_or_else(|| CommonError::Config(format!("memory.stat key {key} has no value")))?;
            return value
                .parse::<u64>()
                .map_err(|_| CommonError::Config(format!("memory.stat key {key} is not a u64")));
        }
    }
    Err(CommonError::Config(format!("memory.stat missing key {key}")))
}

/// Return `true` when `/sys/fs/cgroup` exists as a directory.
///
/// Distinguishes "containerized but no finite limit detected" (the caller
/// warns and exposes an inert metric) from "not on Linux / bare-metal" (quiet
/// inert).
#[must_use]
pub fn cgroupfs_present() -> bool {
    Path::new("/sys/fs/cgroup").is_dir()
}

/// Read a cgroup file holding a single decimal `u64` (`memory.current` or
/// `memory.usage_in_bytes`).
///
/// # Errors
/// `CommonError::Io` if the file cannot be read; `CommonError::Config` if the
/// trimmed contents do not parse as `u64`.
fn read_u64_file(path: &Path) -> Result<u64> {
    let raw = std::fs::read_to_string(path)?;
    raw.trim()
        .parse::<u64>()
        .map_err(|_| CommonError::Config(format!("invalid u64 in {}", path.display())))
}

/// cgroup v2 (unified hierarchy) working-set reader.
struct CgroupV2Reader {
    limit: u64,
    root: PathBuf,
}

impl CgroupV2Reader {
    /// Build a reader rooted at `root` (the unified mount, e.g. `/sys/fs/cgroup`).
    /// Returns `None` when `memory.max` is absent, unreadable, `"max"`, or `0`.
    fn at(root: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(root.join("memory.max")).ok()?;
        match parse_v2_limit(&raw).ok()? {
            Limit::Bytes(limit) => Some(Self {
                limit,
                root: root.to_path_buf(),
            }),
            Limit::Unlimited => None,
        }
    }
}

impl UsageReader for CgroupV2Reader {
    fn limit_bytes(&self) -> u64 {
        self.limit
    }

    fn read_working_set_bytes(&self) -> Result<u64> {
        let usage = read_u64_file(&self.root.join("memory.current"))?;
        let inactive_file = read_stat_key(&self.root.join("memory.stat"), "inactive_file")?;
        Ok(usage.saturating_sub(inactive_file))
    }
}

/// cgroup v1 (memory controller subtree) working-set reader.
struct CgroupV1Reader {
    limit: u64,
    root: PathBuf,
}

impl CgroupV1Reader {
    /// Build a reader rooted at `root` (the memory controller subtree, e.g.
    /// `/sys/fs/cgroup/memory`). Returns `None` when `memory.limit_in_bytes` is
    /// absent, unreadable, `0`, or at/above [`PAGE_COUNTER_UNLIMITED`].
    fn at(root: &Path) -> Option<Self> {
        let raw = std::fs::read_to_string(root.join("memory.limit_in_bytes")).ok()?;
        match parse_v1_limit(&raw).ok()? {
            Limit::Bytes(limit) => Some(Self {
                limit,
                root: root.to_path_buf(),
            }),
            Limit::Unlimited => None,
        }
    }
}

impl UsageReader for CgroupV1Reader {
    fn limit_bytes(&self) -> u64 {
        self.limit
    }

    fn read_working_set_bytes(&self) -> Result<u64> {
        let usage = read_u64_file(&self.root.join("memory.usage_in_bytes"))?;
        // v1 uses the HIERARCHICAL total_inactive_file, not inactive_file.
        let inactive_file = read_stat_key(&self.root.join("memory.stat"), "total_inactive_file")?;
        Ok(usage.saturating_sub(inactive_file))
    }
}

/// Dispatch to the first reader whose root yields a finite limit: v2 (`v2_root`)
/// before v1 (`v1_root`). Parameterizing the roots keeps the ordering
/// unit-testable against `tempfile` fixtures; [`detect_default_reader`] is the
/// thin production wrapper that pins the real `/sys/fs/cgroup` paths.
fn detect_reader_at(v2_root: &Path, v1_root: &Path) -> Option<Arc<dyn UsageReader>> {
    if let Some(reader) = CgroupV2Reader::at(v2_root) {
        return Some(Arc::new(reader));
    }
    if let Some(reader) = CgroupV1Reader::at(v1_root) {
        return Some(Arc::new(reader));
    }
    None
}

/// Detect the platform cgroup reader once at startup.
///
/// Tries the cgroup v2 unified hierarchy (`/sys/fs/cgroup`) first, then the v1
/// memory controller subtree (`/sys/fs/cgroup/memory`). Returns `None` on
/// macOS, bare-metal, or an unlimited limit — the guard is then inert.
///
/// Assumes `cgroupns=private` (the k8s/EKS/orbstack/skaffold default): the
/// container's leaf cgroup sits at the hierarchy root, so no `/proc/self/cgroup`
/// sub-path resolution is required.
#[must_use]
pub fn detect_default_reader() -> Option<Arc<dyn UsageReader>> {
    detect_reader_at(Path::new("/sys/fs/cgroup"), Path::new("/sys/fs/cgroup/memory"))
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    /// Create a temp dir populated with `(filename, contents)` cgroup fixture
    /// files; the dir and files live until the returned handle drops.
    fn fixture_dir(files: &[(&str, &str)]) -> TempDir {
        let tmp = TempDir::new().expect("create tempdir");
        for (name, contents) in files {
            std::fs::write(tmp.path().join(name), contents).expect("write fixture file");
        }
        tmp
    }

    #[test]
    fn parse_v2_limit_max_is_unlimited() {
        assert!(matches!(parse_v2_limit("max\n"), Ok(Limit::Unlimited)));
    }

    #[test]
    fn parse_v2_limit_zero_is_unlimited() {
        assert!(matches!(parse_v2_limit("0"), Ok(Limit::Unlimited)));
    }

    #[test]
    fn parse_v2_limit_finite_is_bytes() {
        assert!(matches!(parse_v2_limit("536870912\n"), Ok(Limit::Bytes(536_870_912))));
    }

    #[test]
    fn parse_v1_limit_sentinel_is_unlimited() {
        let raw = PAGE_COUNTER_UNLIMITED.to_string();
        assert!(matches!(parse_v1_limit(&raw), Ok(Limit::Unlimited)));
    }

    #[test]
    fn parse_v1_limit_above_sentinel_is_unlimited() {
        // 4 KiB-page unlimited value 0x7FFF_FFFF_FFFF_F000 exceeds the 64 KiB sentinel.
        let raw = 0x7FFF_FFFF_FFFF_F000_u64.to_string();
        assert!(matches!(parse_v1_limit(&raw), Ok(Limit::Unlimited)));
    }

    #[test]
    fn parse_v1_limit_zero_is_unlimited() {
        assert!(matches!(parse_v1_limit("0"), Ok(Limit::Unlimited)));
    }

    #[test]
    fn parse_v1_limit_finite_is_bytes() {
        assert!(matches!(parse_v1_limit("536870912"), Ok(Limit::Bytes(536_870_912))));
    }

    #[test]
    fn read_stat_key_selects_exact_key_not_prefix() {
        let tmp = fixture_dir(&[("memory.stat", "inactive_file 111\ntotal_inactive_file 999\n")]);
        let stat = tmp.path().join("memory.stat");
        assert_eq!(read_stat_key(&stat, "inactive_file").expect("inactive_file"), 111);
        assert_eq!(
            read_stat_key(&stat, "total_inactive_file").expect("total_inactive_file"),
            999
        );
    }

    #[test]
    fn read_stat_key_missing_key_is_config_error() {
        let tmp = fixture_dir(&[("memory.stat", "anon 100\nfile 200\n")]);
        let stat = tmp.path().join("memory.stat");
        assert!(matches!(
            read_stat_key(&stat, "inactive_file"),
            Err(CommonError::Config(_))
        ));
    }

    #[test]
    fn cgroup_v2_max_is_no_reader() {
        let tmp = fixture_dir(&[("memory.max", "max\n")]);
        assert!(CgroupV2Reader::at(tmp.path()).is_none());
    }

    #[test]
    fn cgroup_v2_finite_limit_builds_reader() {
        let tmp = fixture_dir(&[("memory.max", "536870912\n")]);
        let reader = CgroupV2Reader::at(tmp.path()).expect("finite v2 limit");
        assert_eq!(reader.limit_bytes(), 536_870_912);
    }

    #[test]
    fn cgroup_v2_working_set_subtracts_inactive_file() {
        let tmp = fixture_dir(&[
            ("memory.max", "536870912\n"),
            ("memory.current", "1000000\n"),
            // total_inactive_file differs, proving v2 uses the non-hierarchical key.
            ("memory.stat", "inactive_file 400000\ntotal_inactive_file 999999\n"),
        ]);
        let reader = CgroupV2Reader::at(tmp.path()).expect("v2 reader");
        assert_eq!(reader.read_working_set_bytes().expect("working set"), 600_000);
    }

    #[test]
    fn cgroup_v2_working_set_saturates_when_inactive_exceeds_usage() {
        let tmp = fixture_dir(&[
            ("memory.max", "536870912\n"),
            ("memory.current", "100\n"),
            ("memory.stat", "inactive_file 500\n"),
        ]);
        let reader = CgroupV2Reader::at(tmp.path()).expect("v2 reader");
        assert_eq!(reader.read_working_set_bytes().expect("working set"), 0);
    }

    #[test]
    fn cgroup_v2_missing_inactive_file_key_is_error() {
        let tmp = fixture_dir(&[
            ("memory.max", "536870912\n"),
            ("memory.current", "1000\n"),
            ("memory.stat", "anon 500\n"),
        ]);
        let reader = CgroupV2Reader::at(tmp.path()).expect("v2 reader");
        assert!(matches!(reader.read_working_set_bytes(), Err(CommonError::Config(_))));
    }

    #[test]
    fn cgroup_v1_sentinel_limit_is_no_reader() {
        let limit = PAGE_COUNTER_UNLIMITED.to_string();
        let tmp = fixture_dir(&[("memory.limit_in_bytes", &limit)]);
        assert!(CgroupV1Reader::at(tmp.path()).is_none());
    }

    #[test]
    fn cgroup_v1_above_sentinel_limit_is_no_reader() {
        // 4 KiB-page unlimited value 0x7FFF_FFFF_FFFF_F000 > the 64 KiB sentinel.
        let limit = 0x7FFF_FFFF_FFFF_F000_u64.to_string();
        let tmp = fixture_dir(&[("memory.limit_in_bytes", &limit)]);
        assert!(CgroupV1Reader::at(tmp.path()).is_none());
    }

    #[test]
    fn cgroup_v1_finite_limit_builds_reader() {
        let tmp = fixture_dir(&[("memory.limit_in_bytes", "536870912\n")]);
        let reader = CgroupV1Reader::at(tmp.path()).expect("finite v1 limit");
        assert_eq!(reader.limit_bytes(), 536_870_912);
    }

    #[test]
    fn cgroup_v1_working_set_uses_total_inactive_file() {
        let tmp = fixture_dir(&[
            ("memory.limit_in_bytes", "536870912\n"),
            ("memory.usage_in_bytes", "1000\n"),
            // inactive_file (100) differs from total_inactive_file (700); v1 must pick 700.
            ("memory.stat", "inactive_file 100\ntotal_inactive_file 700\n"),
        ]);
        let reader = CgroupV1Reader::at(tmp.path()).expect("v1 reader");
        assert_eq!(reader.read_working_set_bytes().expect("working set"), 300);
    }

    #[test]
    fn detect_prefers_v2_over_v1() {
        let v2 = fixture_dir(&[("memory.max", "536870912\n")]);
        let v1 = fixture_dir(&[("memory.limit_in_bytes", "999999\n")]);
        let reader = detect_reader_at(v2.path(), v1.path()).expect("v2 reader");
        // v2's limit (536870912), not v1's (999999), proves v2 won the dispatch.
        assert_eq!(reader.limit_bytes(), 536_870_912);
    }

    #[test]
    fn detect_falls_back_to_v1_when_v2_absent() {
        let v2 = fixture_dir(&[]);
        let v1 = fixture_dir(&[("memory.limit_in_bytes", "999999\n")]);
        let reader = detect_reader_at(v2.path(), v1.path()).expect("v1 reader");
        assert_eq!(reader.limit_bytes(), 999_999);
    }

    #[test]
    fn detect_none_when_neither_present() {
        let v2 = fixture_dir(&[]);
        let v1 = fixture_dir(&[]);
        assert!(detect_reader_at(v2.path(), v1.path()).is_none());
    }

    #[test]
    fn detect_default_reader_does_not_panic() {
        // Host-dependent (Some in a limited container, None on macOS/dev); only
        // assert the thin production wrapper runs without panicking.
        let _ = detect_default_reader();
    }
}
