pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[error("Received unrecognised response")]
    BadResponse,
    #[error("Request timed out waiting for a response")]
    TimedOut,
    #[error(transparent)]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Crypto key error: {0}")]
    CryptoKey(String),
    #[error("JSON error: {0}")]
    SerdeJson(String),
    #[error("URL encoding error: {0}")]
    SerdeUrlencoded(String),
}
