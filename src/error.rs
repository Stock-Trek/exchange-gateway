pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug)]
pub enum EGError {
    ListenModeMustBeOnDemand,
    Poison,
    Send,
    Io(std::io::Error),
    Parse(std::num::ParseIntError),
    Custom(String),
}

impl std::fmt::Display for EGError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EGError::ListenModeMustBeOnDemand => write!(f, "ListenMode requires OnDemand"),
            EGError::Poison => write!(f, "Poison error"),
            EGError::Send => write!(f, "Send error"),
            EGError::Io(e) => write!(f, "IO error: {}", e),
            EGError::Parse(e) => write!(f, "Parse error: {}", e),
            EGError::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for EGError {}

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
