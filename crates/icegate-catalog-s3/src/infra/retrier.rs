//! Async retry primitive with backoff, jitter, and cancellation.
//!
//! Mirrors `icegate-common::retrier` (avoids a cyclic dependency between
//! `icegate-common` and `icegate-catalog-s3`). Consolidating into a shared
//! crate is tracked separately.
//!
//! TODO(med): DRY with icegate-common and job-manager crates

use std::future::Future;

use rand::Rng;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// Retry configuration for backoff and jitter.
#[derive(Debug, Clone)]
pub struct RetrierConfig {
    /// Maximum number of retry attempts before giving up.
    pub max_attempts: usize,
    /// Random jitter added on top of each backoff delay.
    pub rand_delay: Duration,
    /// Backoff delays indexed by attempt number.
    pub delays: Vec<Duration>,
}

/// Error contract for retry cancellation and exhaustion.
pub trait RetryError: Sized {
    /// Error returned when the caller's cancellation token fires.
    fn cancelled() -> Self;
    /// Error returned when the retry limit is reached.
    fn max_attempts() -> Self;
}

/// Retries async operations with backoff and cancellation support.
#[derive(Debug, Clone)]
pub struct Retrier {
    config: RetrierConfig,
}

impl Retrier {
    /// Create a new retrier with the provided config.
    pub const fn new(config: RetrierConfig) -> Self {
        Self { config }
    }

    /// Retry an async handler until success, cancellation, or max attempts.
    ///
    /// Handler returns `Ok((need_retry, value))`: `false` returns the value,
    /// `true` triggers a backoff and a new attempt. Hard errors are propagated
    /// without retry.
    pub async fn retry<F, Fut, T, E>(&self, mut handler: F, cancel_token: &CancellationToken) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(bool, T), E>>,
        E: RetryError,
    {
        let mut attempt = 0;
        loop {
            let result = tokio::select! {
                () = cancel_token.cancelled() => return Err(E::cancelled()),
                result = handler() => result,
            };

            match result {
                Ok((need_retry, value)) => {
                    if !need_retry {
                        return Ok(value);
                    }

                    attempt += 1;
                    if attempt >= self.config.max_attempts {
                        return Err(E::max_attempts());
                    }

                    let delay = self.calculate_delay(attempt);
                    debug!("retry attempt {} after {:?}", attempt, delay);
                    tokio::select! {
                        () = cancel_token.cancelled() => return Err(E::cancelled()),
                        () = sleep(delay) => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Borrow the underlying config (read-only).
    pub const fn config(&self) -> &RetrierConfig {
        &self.config
    }

    /// Compute the backoff delay for a given attempt number.
    ///
    /// `attempt` is the 1-based retry number (first retry is `1`), matching the
    /// counter used by `retry`. The curve is indexed by `attempt - 1`, so the
    /// first retry maps to `delays[0]`. Attempts past the curve clamp to the
    /// last delay.
    ///
    /// Exposed so callers that implement custom retry loops (multi-step
    /// orchestrations that can't fit the `retry` callback shape) can share
    /// the same backoff curve as `retry`.
    pub fn calculate_delay(&self, attempt: usize) -> Duration {
        // Retry numbers are 1-based; the curve is 0-based. `saturating_sub`
        // guards against an underflow if a caller passes `0`.
        let index = attempt.saturating_sub(1);
        let base_delay = if index < self.config.delays.len() {
            self.config.delays[index]
        } else {
            *self.config.delays.last().unwrap_or(&Duration::from_millis(200))
        };

        let max_jitter = u64::try_from(self.config.rand_delay.as_millis()).unwrap_or(u64::MAX);
        let jitter_ms = if max_jitter == 0 {
            0
        } else {
            rand::rng().random_range(0..max_jitter)
        };
        base_delay + Duration::from_millis(jitter_ms)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Cancelled,
        MaxAttempts,
        Fatal,
    }

    impl RetryError for TestError {
        fn cancelled() -> Self {
            Self::Cancelled
        }

        fn max_attempts() -> Self {
            Self::MaxAttempts
        }
    }

    fn fast_config(max_attempts: usize) -> RetrierConfig {
        RetrierConfig {
            max_attempts,
            rand_delay: Duration::from_millis(0),
            delays: vec![Duration::from_millis(0)],
        }
    }

    #[test]
    fn first_retry_uses_first_delay() {
        let config = RetrierConfig {
            max_attempts: 5,
            rand_delay: Duration::from_millis(0),
            delays: vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(30),
            ],
        };
        let retrier = Retrier::new(config);

        // 1-based retry numbers map to a 0-based curve: first retry -> delays[0].
        assert_eq!(retrier.calculate_delay(1), Duration::from_millis(10));
        assert_eq!(retrier.calculate_delay(2), Duration::from_millis(20));
        assert_eq!(retrier.calculate_delay(3), Duration::from_millis(30));
        // Attempts past the curve clamp to the last delay.
        assert_eq!(retrier.calculate_delay(4), Duration::from_millis(30));
    }

    #[tokio::test]
    async fn returns_value_on_first_success() {
        let retrier = Retrier::new(fast_config(5));
        let token = CancellationToken::new();
        let result: Result<u32, TestError> = retrier.retry(|| async { Ok((false, 42)) }, &token).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let retrier = Retrier::new(fast_config(5));
        let token = CancellationToken::new();
        let attempts = AtomicUsize::new(0);
        let result: Result<u32, TestError> = retrier
            .retry(
                || async {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        Ok((true, 0))
                    } else {
                        Ok((false, u32::try_from(n).expect("attempt count fits in u32")))
                    }
                },
                &token,
            )
            .await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn propagates_fatal_error() {
        let retrier = Retrier::new(fast_config(5));
        let token = CancellationToken::new();
        let result: Result<(), TestError> = retrier.retry(|| async { Err(TestError::Fatal) }, &token).await;
        assert_eq!(result.unwrap_err(), TestError::Fatal);
    }

    #[tokio::test]
    async fn returns_max_attempts_on_exhaustion() {
        let retrier = Retrier::new(fast_config(3));
        let token = CancellationToken::new();
        let result: Result<(), TestError> = retrier.retry(|| async { Ok((true, ())) }, &token).await;
        assert_eq!(result.unwrap_err(), TestError::MaxAttempts);
    }

    #[tokio::test]
    async fn returns_cancelled_when_token_fires() {
        let retrier = Retrier::new(fast_config(5));
        let token = CancellationToken::new();
        token.cancel();
        let result: Result<(), TestError> = retrier.retry(|| async { Ok((true, ())) }, &token).await;
        assert_eq!(result.unwrap_err(), TestError::Cancelled);
    }

    #[tokio::test(start_paused = true)]
    async fn returns_cancelled_during_backoff() {
        // Unlike `returns_cancelled_when_token_fires`, which cancels before the
        // first handler call, here the handler requests a retry and the token
        // fires while the retrier sleeps on the backoff — covering the cancel
        // arm of the sleep `select`. A 1h delay plus paused time means the
        // spawned canceller (runnable immediately) always wins over the timer.
        let config = RetrierConfig {
            max_attempts: 5,
            rand_delay: Duration::from_millis(0),
            delays: vec![Duration::from_secs(3600)],
        };
        let retrier = Retrier::new(config);
        let token = CancellationToken::new();
        let canceller = token.clone();
        tokio::spawn(async move {
            canceller.cancel();
        });
        let calls = AtomicUsize::new(0);

        let result: Result<(), TestError> = retrier
            .retry(
                || async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok((true, ()))
                },
                &token,
            )
            .await;

        assert_eq!(result.unwrap_err(), TestError::Cancelled);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cancellation must land during the first backoff, after one handler call"
        );
    }
}
