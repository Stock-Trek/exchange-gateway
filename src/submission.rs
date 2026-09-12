use crate::error::EGResult;
use exchange_types::websocket_id::ETWebsocketId;
use std::{fmt, future::Future, pin::Pin};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubmissionId {
    Int(i64),
    Str(String),
}

impl SubmissionId {
    pub fn new() -> Self {
        Self::Str(uuid::Uuid::new_v4().to_string())
    }
}

impl Default for SubmissionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SubmissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(id) => write!(f, "{id}"),
            Self::Str(id) => write!(f, "{id}"),
        }
    }
}

impl From<SubmissionId> for ETWebsocketId {
    fn from(id: SubmissionId) -> Self {
        match id {
            SubmissionId::Int(id) => ETWebsocketId::Int(id),
            SubmissionId::Str(id) => ETWebsocketId::Str(id),
        }
    }
}

impl From<&SubmissionId> for ETWebsocketId {
    fn from(id: &SubmissionId) -> Self {
        id.clone().into()
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

    #[test]
    fn submission_id_converts_to_websocket_id() {
        assert_eq!(
            ETWebsocketId::from(SubmissionId::Int(5)),
            ETWebsocketId::Int(5)
        );
        assert_eq!(
            ETWebsocketId::from(SubmissionId::Str("abc".into())),
            ETWebsocketId::Str("abc".into())
        );
    }

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
}
