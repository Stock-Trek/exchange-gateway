use serde_json::Value;
use stock_trek::error::result::StockTrekResult;

pub type Executor = Box<dyn ExecutorTrait>;

pub trait ExecutorTrait: Send + Sync {
    fn send_message(&self, message: Value) -> StockTrekResult<Value>;
}
