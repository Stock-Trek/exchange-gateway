mod common;

pub(crate) mod http;
#[cfg(feature = "iris")]
#[cfg(test)]
mod iris_integration_tests;
#[cfg(feature = "reqwest")]
#[cfg(test)]
mod reqwest_integration_tests;
pub(crate) mod websocket;
