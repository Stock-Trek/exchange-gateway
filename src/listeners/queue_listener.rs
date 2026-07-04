use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;
use std::sync::{
    Mutex,
    mpsc::{Receiver, Sender, channel},
};

pub(crate) struct QueueListener<T> {
    sender: Sender<T>,
    receiver_lock: Mutex<Receiver<T>>,
}

impl<TMessage> QueueListener<TMessage> {
    pub fn new() -> Self {
        let (sender, receiver) = channel();
        Self {
            sender,
            receiver_lock: Mutex::new(receiver),
        }
    }
    pub async fn wait_for_message(&self) -> EGResult<TMessage> {
        let receiver_guard = self.receiver_lock.lock().map_err(|_| EGError::Poison)?;
        let message = receiver_guard.recv().map_err(|_| EGError::Poison)?;
        Ok(message)
    }
}

#[async_trait]
impl<TMessage> ListenerTrait<TMessage> for QueueListener<TMessage>
where
    TMessage: Send,
{
    async fn on_message(&self, message: TMessage) -> EGResult<()> {
        self.sender.send(message).map_err(|_| EGError::Send)
    }
}
