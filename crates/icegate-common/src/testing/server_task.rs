//! Lifecycle handling for a harness-spawned server task.
//!
//! Every server harness in the workspace runs its server on a temporary
//! warehouse directory that is dropped as the harness frame unwinds, which is
//! what makes draining the task non-optional: a task still running when that
//! directory is removed serves from a deleted path, and under LeakSanitizer it
//! also outlives the check. The harnesses are separate compilation units, so
//! the drain sequence and its time limit live here rather than once per
//! harness.

use tokio::{task::JoinHandle, time::Duration};

/// Grace period for a cancelled server task to wind down.
///
/// Bounded rather than unbounded because a task wedged in non-yielding code
/// would never join at all, and a hung suite is a worse outcome than a failure.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a drained server task stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// The task returned on its own within [`SHUTDOWN_TIMEOUT`].
    Finished,
    /// The task outlived [`SHUTDOWN_TIMEOUT`], was aborted, and was joined
    /// again under the same bound. Cancellation is not guaranteed to have
    /// completed even so, but the handle was never dropped, so the task was
    /// never detached.
    Aborted,
}

/// Drain a server task whose cancellation token has already fired.
///
/// Returns once the task is no longer running, or once it has been aborted and
/// re-joined. The caller may then drop the warehouse directory.
///
/// # Panics
///
/// Resumes the task's own panic when it has one, so the failure that actually
/// stopped the server surfaces instead of the caller's summary message — a
/// startup failure reaches the harness only as a closed port channel, which
/// says nothing about the cause. Panics with `server` named on a non-panic
/// `JoinError`.
pub async fn drain_server_task(handle: &mut JoinHandle<()>, server: &str) -> DrainOutcome {
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut *handle).await {
        Ok(Ok(())) => DrainOutcome::Finished,
        Ok(Err(join_err)) if join_err.is_panic() => std::panic::resume_unwind(join_err.into_panic()),
        Ok(Err(join_err)) => panic!("{server} server task failed to join: {join_err}"),
        Err(_elapsed) => {
            // `abort()` only schedules cancellation, so join again before
            // handing control back to a caller that is about to delete the
            // warehouse this task is still reading.
            handle.abort();
            let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, handle).await;
            DrainOutcome::Aborted
        }
    }
}
