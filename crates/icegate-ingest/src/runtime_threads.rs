//! Runtime thread planning helpers for ingest process components.

/// Planned Tokio worker thread allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeThreadPlan {
    /// Total available parallelism reported by Rust.
    pub total: usize,
    /// Worker threads for the main ingest runtime.
    pub main_threads: usize,
    /// Worker threads for the dedicated shift runtime.
    pub shift_threads: usize,
}

/// Compute runtime thread allocation for main and shift runtimes.
///
/// The allocation splits available parallelism approximately in half:
/// - `main_threads = ceil(total / 2)`
/// - `shift_threads = floor(total / 2)` with a minimum of `1`
#[must_use]
pub fn compute_runtime_threads() -> RuntimeThreadPlan {
    let total = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let main_threads = total.div_ceil(2).max(1);
    let shift_threads = (total / 2).max(1);

    RuntimeThreadPlan {
        total,
        main_threads,
        shift_threads,
    }
}

#[cfg(test)]
mod tests {
    use super::RuntimeThreadPlan;

    fn plan_for(total: usize) -> RuntimeThreadPlan {
        let main_threads = total.div_ceil(2).max(1);
        let shift_threads = (total / 2).max(1);
        RuntimeThreadPlan {
            total,
            main_threads,
            shift_threads,
        }
    }

    #[test]
    fn plan_for_one() {
        assert_eq!(
            plan_for(1),
            RuntimeThreadPlan {
                total: 1,
                main_threads: 1,
                shift_threads: 1
            }
        );
    }

    #[test]
    fn plan_for_two() {
        assert_eq!(
            plan_for(2),
            RuntimeThreadPlan {
                total: 2,
                main_threads: 1,
                shift_threads: 1
            }
        );
    }

    #[test]
    fn plan_for_three() {
        assert_eq!(
            plan_for(3),
            RuntimeThreadPlan {
                total: 3,
                main_threads: 2,
                shift_threads: 1
            }
        );
    }

    #[test]
    fn plan_for_four() {
        assert_eq!(
            plan_for(4),
            RuntimeThreadPlan {
                total: 4,
                main_threads: 2,
                shift_threads: 2
            }
        );
    }
}
