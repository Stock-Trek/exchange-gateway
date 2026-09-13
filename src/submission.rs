use crate::error::EGResult;
use std::{fmt, future::Future, pin::Pin};

#[derive(Debug)]
pub enum SubmissionOutcome<Request, Response, VerificationRequest> {
    Confirmed(Response),
    Unknown {
        retry: Option<Request>,
        verify: Option<VerificationRequest>,
    },
}

pub struct Submission<'connector, Request, Response, VerificationRequest> {
    future: BoxedFuture<'connector, Request, Response, VerificationRequest>,
}

type BoxedFuture<'connector, Request, Response, VerificationRequest> = Pin<
    Box<
        dyn Future<Output = EGResult<SubmissionOutcome<Request, Response, VerificationRequest>>>
            + 'connector,
    >,
>;

impl<'connector, Request, Response, VerificationRequest>
    Submission<'connector, Request, Response, VerificationRequest>
{
    pub(crate) fn new(
        future: impl Future<
            Output = EGResult<SubmissionOutcome<Request, Response, VerificationRequest>>,
        > + 'connector,
    ) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
    pub async fn wait(self) -> EGResult<SubmissionOutcome<Request, Response, VerificationRequest>> {
        self.future.await
    }
}

impl<Request, Response, VerificationRequest> fmt::Debug
    for Submission<'_, Request, Response, VerificationRequest>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submission").finish_non_exhaustive()
    }
}
