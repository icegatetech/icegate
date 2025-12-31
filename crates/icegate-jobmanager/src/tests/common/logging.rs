use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use parking_lot::{Mutex, MutexGuard};
use tracing::Subscriber;
use tracing_subscriber::{EnvFilter, Layer, layer::Context, prelude::*};

static ERROR_COUNTER: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
static INIT: OnceLock<()> = OnceLock::new();
static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

struct ErrorCountLayer {
    counter: Arc<AtomicUsize>,
}

// TODO(med): think about how to better implement error monitoring. This logger is currently blocking parallel test runs.

impl<S> Layer<S> for ErrorCountLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // Count only our crate's error logs to avoid unrelated noise from dependencies.
        let meta = event.metadata();
        if *meta.level() == tracing::Level::ERROR && meta.target().starts_with("icegate_jobmanager") {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}

pub struct ErrorLogGuard {
    _lock: MutexGuard<'static, ()>,
    counter: Arc<AtomicUsize>,
    baseline: usize,
}

impl ErrorLogGuard {
    pub fn assert_no_errors(self) {
        let after = self.counter.load(Ordering::SeqCst);
        assert_eq!(after, self.baseline, "unexpected error logs during test");
    }
}

// Initialize test logging - call this at the start of each test
pub fn init_test_logging() -> ErrorLogGuard {
    let counter = ERROR_COUNTER.get_or_init(|| Arc::new(AtomicUsize::new(0))).clone();
    let lock = TEST_MUTEX.get_or_init(Mutex::default).lock();

    // Install global subscriber once to capture logs from all threads.
    INIT.get_or_init(|| {
        let fmt_layer = tracing_subscriber::fmt::layer().with_test_writer();
        let error_layer = ErrorCountLayer {
            counter: Arc::clone(&counter),
        };
        let filter = EnvFilter::new("icegate_jobmanager=debug");

        let subscriber = tracing_subscriber::registry()
            .with(fmt_layer.with_filter(filter))
            .with(error_layer);

        let _ = tracing::subscriber::set_global_default(subscriber);
    });

    let baseline = counter.load(Ordering::SeqCst);

    ErrorLogGuard { _lock: lock, counter, baseline }
}
