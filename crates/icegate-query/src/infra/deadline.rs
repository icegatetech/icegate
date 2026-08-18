//! Absolute wall-clock deadline for one gRPC call, response phase and body
//! alike.
//!
//! Flight SQL answers `DoGet` with a STREAM: the response future resolves as
//! soon as the first frame is ready, and the query keeps running while frames
//! are pulled. A timeout over the request future alone
//! (`tower_http::timeout::TimeoutLayer`, what the HTTP APIs use) therefore
//! bounds nothing here, and an inactivity timeout between frames
//! (`tower_http::timeout::ResponseBodyTimeoutLayer`) is not the bound this needs
//! either: what has to be capped is the whole stream, not the gap inside it.
//! The timer below is never rearmed, which is what the
//! `a_body_producing_frames_is_still_cut_off_at_the_deadline` test in this file
//! holds.
//!
//! [`ResponseDeadlineLayer`] fixes ONE instant when the request arrives and
//! never moves it, and both halves of the call end at that instant with the
//! SAME answer: gRPC trailers carrying `DEADLINE_EXCEEDED` and naming
//! `engine.max_query_duration_secs`. Reaching it during the response phase
//! (planning, and every unary RPC) yields a response whose body carries nothing
//! but those trailers; reaching it while the stream runs ends the stream with
//! them. Either way one setting has one outcome, and the client reports a gRPC
//! status rather than a broken stream.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Request, Response, header::CONTENT_TYPE};
use http_body::{Body, Frame, SizeHint};
use tokio::time::{Instant, Sleep};
use tonic::metadata::GRPC_CONTENT_TYPE;
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

/// Layer bounding the total wall-clock time of one call, body included.
#[derive(Debug, Clone, Copy)]
pub struct ResponseDeadlineLayer {
    /// Wall-clock budget granted to one call, response phase and body together.
    deadline: Duration,
}

impl ResponseDeadlineLayer {
    /// Build a layer granting each call `deadline` of wall-clock time, spent by
    /// the response phase and the body it produces out of the one budget.
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

/// Service ending each call at its deadline, in whichever phase it arrives.
#[derive(Debug, Clone, Copy)]
pub struct ResponseDeadline<S> {
    /// Wrapped service.
    inner: S,
    /// Wall-clock budget granted to one call, response phase and body together.
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
        // Taken BEFORE the inner call, so a slow response phase is spent out of
        // the same budget the body then runs under. Taken after it, the body
        // would start a second full deadline on top of the response phase, and a
        // request could live for twice `max_query_duration_secs`.
        let deadline_at = Instant::now() + self.deadline;
        let response = self.inner.call(request);
        Box::pin(async move {
            match tokio::time::timeout_at(deadline_at, response).await {
                Ok(response) => Ok(response?.map(|body| DeadlineBody::new(body, deadline_at))),
                // The budget ran out before a body existed. Answering here, and
                // not leaving the phase to a transport timeout, is what keeps
                // ONE status for one setting: the caller sees the same
                // `DEADLINE_EXCEEDED` a cut stream ends with.
                Err(_elapsed) => Ok(deadline_response()),
            }
        })
    }
}

/// The gRPC response for a call whose budget ran out before the inner service
/// produced one: a normal `200` response whose body is the deadline trailers and
/// nothing else.
///
/// The content type is the one the gRPC wire protocol defines for a response,
/// so a synthesised response states it exactly as an inner service would.
fn deadline_response<B>() -> Response<DeadlineBody<B>> {
    let mut response = Response::new(DeadlineBody::exhausted());
    response.headers_mut().insert(CONTENT_TYPE, GRPC_CONTENT_TYPE);
    response
}

/// Response body that ends with a `DEADLINE_EXCEEDED` status once the call's
/// deadline passes, however many frames it has produced by then.
pub struct DeadlineBody<B> {
    /// What this body still has to do.
    state: DeadlineBodyState<B>,
}

/// The three things a [`DeadlineBody`] can be: still streaming under its timer,
/// holding the trailers of a call that ran out of budget before it had a body,
/// or done.
enum DeadlineBodyState<B> {
    /// Wrapping a body the deadline has not reached yet. Both parts are boxed so
    /// they can be polled through a pin without `unsafe` projection
    /// (`unsafe_code` is forbidden workspace-wide); the timer rests on the
    /// instant the budget ends at and is never rearmed.
    Streaming {
        /// Wrapped body.
        inner: Pin<Box<B>>,
        /// The single timer.
        sleep: Pin<Box<Sleep>>,
    },
    /// The budget was already spent when the response was built, so there is no
    /// inner body: the trailers are all this one has to yield.
    Exhausted,
    /// The deadline trailers have been emitted; the body yields nothing
    /// afterwards.
    Finished,
}

impl<B> DeadlineBody<B> {
    /// Wrap `body`, ending it at `deadline_at` however much of the budget the
    /// response phase already spent.
    fn new(body: B, deadline_at: Instant) -> Self {
        Self {
            state: DeadlineBodyState::Streaming {
                inner: Box::pin(body),
                sleep: Box::pin(tokio::time::sleep_until(deadline_at)),
            },
        }
    }

    /// Body of the response synthesised for a call whose budget ran out during
    /// the response phase: it yields the deadline trailers and ends.
    const fn exhausted() -> Self {
        Self {
            state: DeadlineBodyState::Exhausted,
        }
    }
}

impl<B> Body for DeadlineBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let body = self.get_mut();
        match &mut body.state {
            DeadlineBodyState::Finished => return Poll::Ready(None),
            DeadlineBodyState::Exhausted => {}
            // The timer is polled FIRST: an inner body that stays pending is
            // exactly the case this layer exists for, and polling it first would
            // park the task on the query instead of the deadline.
            DeadlineBodyState::Streaming { inner, sleep } => {
                if sleep.as_mut().poll(cx).is_pending() {
                    return inner.as_mut().poll_frame(cx);
                }
            }
        }
        body.state = DeadlineBodyState::Finished;
        Poll::Ready(Some(Ok(Frame::trailers(deadline_trailers()))))
    }

    fn is_end_stream(&self) -> bool {
        match &self.state {
            DeadlineBodyState::Finished => true,
            // The trailers are still ahead.
            DeadlineBodyState::Exhausted => false,
            DeadlineBodyState::Streaming { inner, .. } => inner.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.state {
            DeadlineBodyState::Streaming { inner, .. } => inner.size_hint(),
            // Trailers carry no DATA, so nothing is left to count.
            DeadlineBodyState::Exhausted | DeadlineBodyState::Finished => SizeHint::with_exact(0),
        }
    }
}

impl<B> std::fmt::Debug for DeadlineBody<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self.state {
            DeadlineBodyState::Streaming { .. } => "streaming",
            DeadlineBodyState::Exhausted => "exhausted",
            DeadlineBodyState::Finished => "finished",
        };
        f.debug_struct("DeadlineBody").field("state", &state).finish_non_exhaustive()
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
        let body = DeadlineBody::new(PendingBody, Instant::now() + Duration::from_secs(30));

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
        let mut body = std::pin::pin!(DeadlineBody::new(
            CountedBody { remaining: frames },
            Instant::now() + deadline
        ));

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
        let mut body = std::pin::pin!(DeadlineBody::new(
            CountedBody { remaining: 3 },
            Instant::now() + Duration::from_secs(30)
        ));

        let mut data_frames = 0usize;
        while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
            let frame = frame.expect("no body error");
            assert!(frame.is_data(), "no trailers are injected before the deadline");
            data_frames += 1;
        }

        assert_eq!(data_frames, 3);
    }

    /// Both phases of one call share a single budget: a response future that
    /// spends part of the deadline before the stream exists leaves the body only
    /// the remainder. Armed at the response instead, the request would live for
    /// the response phase PLUS a full deadline, which is what the
    /// `keep_segments_count` floor is sized against.
    #[tokio::test(start_paused = true)]
    async fn the_response_phase_and_the_body_share_one_deadline() {
        const DEADLINE: Duration = Duration::from_secs(30);
        /// Spent before the response exists, so the body gets the rest.
        const RESPONSE_DELAY: Duration = Duration::from_secs(20);

        let mut service =
            ResponseDeadlineLayer::new(DEADLINE).layer(tower::service_fn(|_request: Request<()>| async {
                tokio::time::sleep(RESPONSE_DELAY).await;
                Ok::<_, Infallible>(Response::new(PendingBody))
            }));
        let started = Instant::now();

        let response = service.call(Request::new(())).await.expect("the service answers");
        let frame = response
            .into_body()
            .boxed()
            .frame()
            .await
            .expect("a frame is produced")
            .expect("no body error");

        let trailers = frame.into_trailers().expect("the frame carries trailers");
        assert_eq!(
            trailers.get(GRPC_STATUS_HEADER).expect("status"),
            GRPC_STATUS_DEADLINE_EXCEEDED
        );
        assert_eq!(
            started.elapsed(),
            DEADLINE,
            "the call must end one deadline after it arrived, not after the response phase plus one"
        );
    }

    /// The other half of the same budget: a response phase that never finishes
    /// is answered by the layer itself, with the status a cut stream carries.
    /// Left to a transport timeout instead, one setting would report two
    /// different codes depending on which half was running.
    #[tokio::test(start_paused = true)]
    async fn a_response_phase_outliving_the_deadline_answers_deadline_exceeded() {
        const DEADLINE: Duration = Duration::from_secs(30);

        let mut service =
            ResponseDeadlineLayer::new(DEADLINE).layer(tower::service_fn(|_request: Request<()>| async {
                // Never answers: the shape planning has while it is still
                // working, and the reason the response phase needs a bound of
                // its own.
                std::future::pending::<()>().await;
                Ok::<_, Infallible>(Response::new(PendingBody))
            }));
        let started = Instant::now();

        let response = service.call(Request::new(())).await.expect("the layer answers for the call");

        assert_eq!(
            response.headers().get(CONTENT_TYPE).expect("content type"),
            GRPC_CONTENT_TYPE,
            "a gRPC response carries this content type, whoever synthesised it"
        );
        let mut body = std::pin::pin!(response.into_body());
        let frame = std::future::poll_fn(|cx| body.as_mut().poll_frame(cx))
            .await
            .expect("a frame is produced")
            .expect("no body error");
        let trailers = frame.into_trailers().expect("the frame carries trailers");

        assert_eq!(
            trailers.get(GRPC_STATUS_HEADER).expect("status"),
            GRPC_STATUS_DEADLINE_EXCEEDED
        );
        assert_eq!(trailers.get(GRPC_MESSAGE_HEADER).expect("message"), DEADLINE_MESSAGE);
        assert_eq!(
            started.elapsed(),
            DEADLINE,
            "the call must end at the deadline it arrived with"
        );
        assert!(
            std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await.is_none(),
            "the synthesised body ends after its trailers"
        );
    }
}
