pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug)]
pub enum EGError {
    BadResponse,
    ReceiveTimeout(std::sync::mpsc::RecvTimeoutError),
    Io(std::io::Error),
    Parse(std::num::ParseIntError),
    SerdeJson(String),
    SerdeUrlencoded(String),
    CryptoKey(String),
    SignerCreation(String),
}

impl std::fmt::Display for EGError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EGError::BadResponse => write!(f, "Transport produced bad response"),
            EGError::ReceiveTimeout(e) => write!(f, "Receive timeout error: {}", e),
            EGError::Io(e) => write!(f, "IO error: {}", e),
            EGError::Parse(e) => write!(f, "Parse error: {}", e),
            EGError::SerdeJson(e) => write!(f, "JSON error: {}", e),
            EGError::SerdeUrlencoded(e) => write!(f, "URL encoding error: {}", e),
            EGError::CryptoKey(e) => write!(f, "Crypto key error: {}", e),
            EGError::SignerCreation(e) => write!(f, "Signer creation error: {}", e),
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
