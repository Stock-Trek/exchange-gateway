use crate::execute::executor::{Executor, ExecutorTrait};
use serde_json::Value;
use stock_trek::error::result::StockTrekResult;

pub struct StubExecutor;

impl From<StubExecutor> for Executor {
    fn from(value: StubExecutor) -> Self {
        Box::new(value)
    }
}

impl ExecutorTrait for StubExecutor {
    fn send_message(&self, message: Value) -> StockTrekResult<Value> {
        Ok(message)
    }
}
