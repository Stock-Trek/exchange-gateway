use crate::{
    error::EGResult,
    listeners::{
        listener::{Listener, ListenerTrait},
        one_shot_listener::OneShotListener,
    },
};
use async_trait::async_trait;
use std::sync::Mutex;

pub struct HybridListener<T: Send> {
    delegate: Listener<T>,
    one_shots: Mutex<Vec<OneShotListener<T>>>,
}

impl<T: Send> HybridListener<T> {
    pub fn new(delegate: Listener<T>) -> Self {
        HybridListener {
            delegate,
            one_shots: Mutex::new(Vec::new()),
        }
    }
    pub async fn wait_for_message(&self) -> EGResult<T> {}
}

#[async_trait]
impl<T: Send> ListenerTrait<T> for HybridListener<T> {
    async fn on_message(&self, message: T) -> EGResult<()> {}
}
