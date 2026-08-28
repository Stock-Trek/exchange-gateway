mod common;
#[cfg(feature = "reqwest")]
pub(crate) mod http;
#[cfg(feature = "iris")]
#[cfg(test)]
mod iris_integration_tests;
#[cfg(feature = "reqwest")]
#[cfg(test)]
mod reqwest_integration_tests;
#[cfg(feature = "iris")]
pub(crate) mod websocket;
