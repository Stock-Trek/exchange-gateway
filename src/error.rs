pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[error("Transport produced bad response")]
    BadResponse,
    #[error("Conversion failed: {0}")]
    Convert(EGError),
    #[error("Crypto key error: {0}")]
    CryptoKey(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] std::num::ParseIntError),
    #[error("Receive timeout error: {0}")]
    ReceiveTimeout(#[from] std::sync::mpsc::RecvTimeoutError),
    #[error("JSON error: {0}")]
    SerdeJson(String),
    #[error("URL encoding error: {0}")]
    SerdeUrlencoded(String),
}
