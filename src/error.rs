pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug)]
pub enum EGError {
    ListenModeMustBeOnDemand,
    Poison,
    ReceiveError(tokio::sync::oneshot::error::RecvError),
    Timeout(std::time::Duration),
    Io(std::io::Error),
    Parse(std::num::ParseIntError),
    Custom(String),
}

impl std::fmt::Display for EGError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EGError::ListenModeMustBeOnDemand => write!(f, "ListenMode requires OnDemand"),
            EGError::Poison => write!(f, "Poison error"),
            EGError::ReceiveError(e) => write!(f, "Receive error: {}", e),
            EGError::Timeout(timeout) => write!(f, "Timeout: {:?}", timeout),
            EGError::Io(e) => write!(f, "IO error: {}", e),
            EGError::Parse(e) => write!(f, "Parse error: {}", e),
            EGError::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for EGError {}

impl From<tokio::sync::oneshot::error::RecvError> for EGError {
    fn from(e: tokio::sync::oneshot::error::RecvError) -> Self {
        EGError::ReceiveError(e)
    }
}

impl From<std::io::Error> for EGError {
    fn from(e: std::io::Error) -> Self {
        EGError::Io(e)
    }
}

impl From<std::num::ParseIntError> for EGError {
    fn from(e: std::num::ParseIntError) -> Self {
        EGError::Parse(e)
    }
}
