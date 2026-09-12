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
    Indeterminate(SubmissionId),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn id_is_available_before_waiting() {
        let id = SubmissionId::new();
        let expected = id.clone();
        let submission = Submission::new(id, async { Ok(SubmissionOutcome::Confirmed(7)) });
        assert_eq!(submission.id(), &expected);
        match submission.wait().await.unwrap() {
            SubmissionOutcome::Confirmed(value) => assert_eq!(value, 7),
            SubmissionOutcome::Indeterminate(_) => panic!("expected a confirmed outcome"),
        }
    }

    #[tokio::test]
    async fn indeterminate_outcome_carries_the_submission_id() {
        let id = SubmissionId::new();
        let expected = id.clone();
        let submission = Submission::new(id.clone(), async move {
            Ok(SubmissionOutcome::<()>::Indeterminate(id))
        });
        match submission.wait().await.unwrap() {
            SubmissionOutcome::Confirmed(()) => panic!("expected an indeterminate outcome"),
            SubmissionOutcome::Indeterminate(id) => assert_eq!(id, expected),
        }
    }

    #[test]
    fn submission_id_is_a_string_new_type() {
        let id = SubmissionId::from("abc-123".to_owned());
        assert_eq!(id.as_str(), "abc-123");
        assert_eq!(id.to_string(), "abc-123");
        assert_eq!(String::from(id.clone()), "abc-123");
        assert_eq!(SubmissionId::from("abc-123"), id);
    }
}
