use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use tracing::Subscriber;
use tracing_subscriber::{EnvFilter, Layer, layer::Context, prelude::*};

static ERROR_COUNTER: OnceLock<Arc<AtomicUsize>> = OnceLock::new();

struct ErrorCountLayer {
    counter: Arc<AtomicUsize>,
}

impl<S> Layer<S> for ErrorCountLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::ERROR {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }
}

pub struct ErrorLogGuard {
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
    let baseline = counter.load(Ordering::SeqCst);
    let fmt_layer = tracing_subscriber::fmt::layer().with_test_writer();
    let error_layer = ErrorCountLayer {
        counter: Arc::clone(&counter),
    };
    let filter = EnvFilter::new("jobmanager=debug");

    let _ = tracing_subscriber::registry().with(fmt_layer.with_filter(filter)).with(error_layer).try_init();

    ErrorLogGuard {
        counter,
        baseline,
    }
}
