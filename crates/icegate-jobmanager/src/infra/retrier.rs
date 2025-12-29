use std::future::Future;

use rand::Rng;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[derive(Clone)]
pub struct RetrierConfig {
    /// Maximum number of retry attempts.
    pub max_attempts: usize,
    /// Random jitter added to each delay.
    pub rand_delay: Duration,
    /// Backoff delays by attempt index.
    pub delays: Vec<Duration>,
}

impl Default for RetrierConfig {
    fn default() -> Self {
        Self {
            max_attempts: 20,
            rand_delay: Duration::from_millis(50),
            delays: vec![
                Duration::from_millis(50),
                Duration::from_millis(50),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(300),
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_secs(60),
            ],
        }
    }
}

pub(crate) struct Retrier {
    config: RetrierConfig,
}

pub(crate) trait RetryError: Sized {
    fn cancelled() -> Self;
    fn max_attempts() -> Self;
}

impl Retrier {
    pub const fn new(config: RetrierConfig) -> Self {
        Self {
            config,
        }
    }

    pub async fn retry<F, Fut, T, E>(&self, mut handler: F, cancel_token: &CancellationToken) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(bool, T), E>>,
        E: RetryError,
    {
        let mut attempt = 0;
        loop {
            let result = tokio::select! {
                _ = cancel_token.cancelled() => return Err(E::cancelled()),
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
                        _ = cancel_token.cancelled() => return Err(E::cancelled()),
                        _ = sleep(delay) => {}
                    }
                },
                Err(e) => return Err(e),
            }
        }
    }

    fn calculate_delay(&self, attempt: usize) -> Duration {
        let base_delay = if attempt < self.config.delays.len() {
            self.config.delays[attempt]
        } else {
            *self.config.delays.last().unwrap_or(&Duration::from_millis(200))
        };

        // Add randomization to prevent thundering herd
        let jitter_ms = rand::rng().random_range(0..self.config.rand_delay.as_millis() as u64);
        base_delay + Duration::from_millis(jitter_ms)
    }
}
