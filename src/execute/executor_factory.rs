use crate::execute::{executor::Executor, stub_executor::StubExecutor};

pub struct ExecutorFactory;

impl ExecutorFactory {
    pub fn stub() -> Executor {
        StubExecutor.into()
    }
    // TODO simulated, mocked, delayed, failing
}
