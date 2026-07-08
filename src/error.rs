pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug)]
pub enum EGError {
    OneShotCalledTwice,
    BadResponse,
    ListenModeMustBeOnDemand,
    Poison,
    OneShotAlreadyUsed,
    ReceiveTimeout(std::sync::mpsc::RecvTimeoutError),
    Send,
    Io(std::io::Error),
    Parse(std::num::ParseIntError),
    Custom(String),
}

impl std::fmt::Display for EGError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EGError::OneShotCalledTwice => write!(f, "OneShotListener cannot be called twice"),
            EGError::BadResponse => write!(f, "Transport produced bad response"),
            EGError::ListenModeMustBeOnDemand => write!(f, "ListenMode requires OnDemand"),
            EGError::Poison => write!(f, "Poison error"),
            EGError::OneShotAlreadyUsed => write!(f, "OneShot interceptor already used"),
            EGError::ReceiveTimeout(e) => write!(f, "Receive timeout error: {}", e),
            EGError::Send => write!(f, "Send error"),
            EGError::Io(e) => write!(f, "IO error: {}", e),
            EGError::Parse(e) => write!(f, "Parse error: {}", e),
            EGError::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for EGError {}

impl From<std::sync::mpsc::RecvTimeoutError> for EGError {
    fn from(e: std::sync::mpsc::RecvTimeoutError) -> Self {
        EGError::ReceiveTimeout(e)
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
