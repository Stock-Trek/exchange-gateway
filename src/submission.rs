use crate::error::EGResult;
use std::{fmt, future::Future, pin::Pin};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubmissionId(String);

impl SubmissionId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SubmissionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SubmissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SubmissionId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for SubmissionId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl From<SubmissionId> for String {
    fn from(id: SubmissionId) -> Self {
        id.0
    }
}

impl AsRef<str> for SubmissionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug)]
pub enum SubmissionOutcome<Response> {
    Confirmed(Response),
    Unknown(SubmissionId),
}

pub struct Submission<'a, Response> {
    id: SubmissionId,
    future: Pin<Box<dyn Future<Output = EGResult<SubmissionOutcome<Response>>> + 'a>>,
}

impl<'a, Response> Submission<'a, Response> {
    pub(crate) fn new(
        id: SubmissionId,
        future: impl Future<Output = EGResult<SubmissionOutcome<Response>>> + 'a,
    ) -> Self {
        Self {
            id,
            future: Box::pin(future),
        }
    }
    pub fn id(&self) -> &SubmissionId {
        &self.id
    }
    pub async fn wait(self) -> EGResult<SubmissionOutcome<Response>> {
        self.future.await
    }
}

impl<Response> fmt::Debug for Submission<'_, Response> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Submission")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}
