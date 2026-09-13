use crate::error::EGResult;
use exchange_types::request::{ETHttpRequest, ETWebsocketRequest};
use std::{fmt, future::Future, pin::Pin};

/// The outcome of submitting a request to an exchange.
///
/// On an [`Unknown`](Self::Unknown) outcome the exchange may or may not have
/// received and processed the request, so it is not safe to blindly resend it.
/// Instead the original request is returned so that the caller can derive the
/// exchange's reconciliation request and query the true state of the
/// submission.
///
/// For convenience, [`SubmissionOutcome::reconciliation_request_http`] and
/// [`SubmissionOutcome::reconciliation_request_websocket`] will produce the
/// reconciliation request for an unknown outcome directly.
#[derive(Debug)]
pub enum SubmissionOutcome<Response, Request> {
    /// The exchange confirmed the outcome of the request.
    Confirmed(Response),
    /// The outcome of the request is unknown, so the original request is
    /// returned to the caller.
    Unknown(Request),
}

impl<Response, Request: ETHttpRequest> SubmissionOutcome<Response, Request> {
    /// The HTTP reconciliation request for an unknown outcome, if the exchange
    /// provides one.
    pub fn reconciliation_request_http(&self) -> Option<impl ETHttpRequest> {
        match self {
            Self::Unknown(request) => request.reconcilation_request_http(),
            Self::Confirmed(_) => None,
        }
    }
}

impl<Response, Request: ETWebsocketRequest> SubmissionOutcome<Response, Request> {
    /// The websocket reconciliation request for an unknown outcome, if the
    /// exchange provides one.
    pub fn reconciliation_request_websocket(&self) -> Option<impl ETWebsocketRequest> {
        match self {
            Self::Unknown(request) => request.reconcilation_request_websocket(),
            Self::Confirmed(_) => None,
        }
    }
}

pub struct Submission<'a, Response, Request> {
    future: BoxedFuture<'a, Response, Request>,
}

type BoxedFuture<'a, Response, Request> =
    Pin<Box<dyn Future<Output = EGResult<SubmissionOutcome<Response, Request>>> + 'a>>;

impl<'a, Response, Request> Submission<'a, Response, Request> {
    pub(crate) fn new(
        future: impl Future<Output = EGResult<SubmissionOutcome<Response, Request>>> + 'a,
    ) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
    pub async fn wait(self) -> EGResult<SubmissionOutcome<Response, Request>> {
        self.future.await
    }
}

impl<Response, Request> fmt::Debug for Submission<'_, Response, Request> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submission").finish_non_exhaustive()
    }
}
