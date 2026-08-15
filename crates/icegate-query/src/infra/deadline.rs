//! Absolute wall-clock deadline for a gRPC response, including its body.
//!
//! Flight SQL answers `DoGet` with a STREAM: the response future resolves as
//! soon as the first frame is ready, and the query keeps running while frames
//! are pulled. A request-future timeout (`tower_http::timeout::TimeoutLayer`,
//! what the HTTP APIs use) therefore bounds nothing here, and
//! `ResponseBodyTimeoutLayer` bounds only the gap BETWEEN frames — its
//! `TimeoutBody` resets the timer after each frame (`tower-http`,
//! `src/timeout/body.rs`), so a stream that keeps producing can run forever.
//!
//! [`ResponseDeadlineLayer`] arms one timer when the response is produced and
//! never rearms it. When it fires, the body ends with gRPC trailers carrying
//! `DEADLINE_EXCEEDED`, which is a proper gRPC termination the client reports as
//! a status rather than a broken stream.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body::{Body, Frame};
use tokio::time::Sleep;
use tower::{Layer, Service};

/// gRPC status code for a deadline overrun (`DEADLINE_EXCEEDED`), as defined by
/// the gRPC wire protocol.
const GRPC_STATUS_DEADLINE_EXCEEDED: &str = "4";

/// Trailer key carrying the gRPC status code.
const GRPC_STATUS_HEADER: &str = "grpc-status";

/// Trailer key carrying the human-readable gRPC status message.
const GRPC_MESSAGE_HEADER: &str = "grpc-message";

/// Message sent with the deadline status, so an operator reading a client log
/// learns which setting cut the query off.
const DEADLINE_MESSAGE: &str = "query exceeded engine.max_query_duration_secs";

/// Layer bounding the total wall-clock time of a response, body included.
#[derive(Debug, Clone, Copy)]
pub struct ResponseDeadlineLayer {
    /// Wall-clock budget granted to one response.
    deadline: Duration,
}

impl ResponseDeadlineLayer {
    /// Build a layer granting each response `deadline` of wall-clock time.
    #[must_use]
    pub const fn new(deadline: Duration) -> Self {
        Self { deadline }
    }
}

impl<S> Layer<S> for ResponseDeadlineLayer {
    type Service = ResponseDeadline<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ResponseDeadline {
            inner,
            deadline: self.deadline,
        }
    }
}

/// Service wrapping each response body in a [`DeadlineBody`].
#[derive(Debug, Clone, Copy)]
pub struct ResponseDeadline<S> {
    /// Wrapped service.
    inner: S,
    /// Wall-clock budget granted to one response.
    deadline: Duration,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for ResponseDeadline<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    S::Future: Send + 'static,
    ResBody: Body<Data = Bytes>,
{
    type Response = Response<DeadlineBody<ResBody>>;
    type Error = S::Error;
    // Boxed so the layer needs no hand-written pinned future type; one
    // allocation per gRPC call is noise next to the query it fronts.
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let deadline = self.deadline;
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await?;
            // The timer starts here — when the response exists — not when the
            // first frame is polled, so a stream that is slow to start spends
            // its own budget rather than the body's.
            Ok(response.map(|body| DeadlineBody::new(body, deadline)))
        })
    }
}

/// Response body that ends with a `DEADLINE_EXCEEDED` status once its deadline
/// passes, however many frames it has produced by then.
pub struct DeadlineBody<B> {
    /// Wrapped body. Boxed so it can be polled through a pin without `unsafe`
    /// projection (`unsafe_code` is forbidden workspace-wide).
    inner: Pin<Box<B>>,
    /// The single, never-rearmed timer.
    sleep: Pin<Box<Sleep>>,
    /// Set once the deadline trailers have been emitted; the body yields
    /// nothing afterwards.
    finished: bool,
}

impl<B> DeadlineBody<B> {
    /// Wrap `body`, arming its deadline now.
    fn new(body: B, deadline: Duration) -> Self {
        Self {
            inner: Box::pin(body),
            sleep: Box::pin(tokio::time::sleep(deadline)),
            finished: false,
        }
    }
}

impl<B> Body for DeadlineBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        // The timer is polled FIRST: an inner body that stays pending is exactly
        // the case this layer exists for, and polling it first would park the
        // task on the query instead of the deadline.
        if self.sleep.as_mut().poll(cx).is_ready() {
            self.finished = true;
            return Poll::Ready(Some(Ok(Frame::trailers(deadline_trailers()))));
        }
        self.inner.as_mut().poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

impl<B> std::fmt::Debug for DeadlineBody<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadlineBody")
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

/// gRPC trailers reporting a deadline overrun.
fn deadline_trailers() -> HeaderMap {
    let mut trailers = HeaderMap::with_capacity(2);
    trailers.insert(
        GRPC_STATUS_HEADER,
        HeaderValue::from_static(GRPC_STATUS_DEADLINE_EXCEEDED),
    );
    trailers.insert(GRPC_MESSAGE_HEADER, HeaderValue::from_static(DEADLINE_MESSAGE));
    trailers
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use http_body_util::BodyExt;

    use super::*;

    /// A body that never yields: the shape a runaway query's stream has while it
    /// is still working.
    struct PendingBody;

    impl Body for PendingBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }
    }

    /// A body that yields `count` data frames and then ends.
    struct CountedBody {
        remaining: usize,
    }

    impl Body for CountedBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            if self.remaining == 0 {
                return Poll::Ready(None);
            }
            self.remaining -= 1;
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"row")))))
        }
    }

    /// The whole point of the layer: a stream that produces nothing is cut off
    /// at the deadline with a gRPC status, not left hanging.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_body_ends_with_deadline_exceeded_trailers() {
        let body = DeadlineBody::new(PendingBody, Duration::from_secs(30));

        let frame = body.boxed().frame().await.expect("a frame is produced").expect("no body error");
        let trailers = frame.into_trailers().expect("the frame carries trailers");

        assert_eq!(
            trailers.get(GRPC_STATUS_HEADER).expect("status"),
            GRPC_STATUS_DEADLINE_EXCEEDED
        );
    }

    /// Frames arriving do NOT push the deadline back — the difference from
    /// `tower_http`'s inactivity timeout, and the reason a long stream cannot
    /// outlive the retention window it is measured against.
    #[tokio::test(start_paused = true)]
    async fn a_body_producing_frames_is_still_cut_off_at_the_deadline() {
        let deadline = Duration::from_secs(30);
        // Far more frames than the deadline allows: each poll advances the
        // paused clock past the previous one, so a resetting timer would never
        // fire and the body would run to `frames` frames.
        let frames = 100;
        let mut body = std::pin::pin!(DeadlineBody::new(CountedBody { remaining: frames }, deadline));

        let mut data_frames = 0usize;
        let trailers = loop {
            let frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx))
                .await
                .expect("the body must end with trailers, not exhaustion")
                .expect("no body error");
            if frame.is_trailers() {
                break frame.into_trailers().expect("trailers");
            }
            data_frames += 1;
            // One second of query time per frame: 30 frames fit the deadline.
            tokio::time::advance(Duration::from_secs(1)).await;
        };

        assert_eq!(
            trailers.get(GRPC_STATUS_HEADER).expect("status"),
            GRPC_STATUS_DEADLINE_EXCEEDED
        );
        assert!(
            data_frames < frames,
            "the deadline must cut the stream short, got all {frames} frames"
        );
    }

    /// A body finishing inside its budget is passed through untouched: no
    /// injected trailers, no truncation.
    #[tokio::test(start_paused = true)]
    async fn a_body_finishing_before_the_deadline_is_untouched() {
        let mut body = std::pin::pin!(DeadlineBody::new(CountedBody { remaining: 3 }, Duration::from_secs(30)));

        let mut data_frames = 0usize;
        while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            let frame = frame.expect("no body error");
            assert!(frame.is_data(), "no trailers are injected before the deadline");
            data_frames += 1;
        }

        assert_eq!(data_frames, 3);
    }
}
