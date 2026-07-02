use crate::{
    error::{EGError, EGResult},
    listeners::listener::{Listener, ListenerTrait},
};
use async_trait::async_trait;
use std::sync::{
    Arc, Mutex, RwLock,
    mpsc::{Receiver, Sender, channel},
};
use strum::Display;

pub struct HybridListener<T> {
    mode_lock: RwLock<ListenMode>,
    delegated_listener: Arc<Listener<T>>,
    sender: Sender<T>,
    receiver_lock: Mutex<Receiver<T>>,
}

#[derive(Debug, Display, Clone, Copy)]
pub enum ListenMode {
    EventDriven,
    OnDemand,
}

impl<TMessage> HybridListener<TMessage> {
    pub fn new(mode: ListenMode, delegated_listener: Arc<Listener<TMessage>>) -> Self {
        let (sender, receiver) = channel();
        Self {
            mode_lock: RwLock::new(mode),
            delegated_listener,
            sender,
            receiver_lock: Mutex::new(receiver),
        }
    }
    pub async fn wait_for_message(&self) -> EGResult<TMessage> {
        let mode = self.get_mode()?;
        match mode {
            ListenMode::EventDriven => Err(EGError::ListenModeMustBeOnDemand),
            ListenMode::OnDemand => {
                let receiver_guard = self.receiver_lock.lock().map_err(|_| EGError::Poison)?;
                let message = receiver_guard.recv().map_err(|_| EGError::Poison)?;
                Ok(message)
            }
        }
    }
    pub fn mode(&self, mode: ListenMode) -> EGResult<()> {
        let mut guard = self.mode_lock.write().map_err(|_| EGError::Poison)?;
        *guard = mode;
        Ok(())
    }
    fn get_mode(&self) -> EGResult<ListenMode> {
        let guard = self.mode_lock.read().map_err(|_| EGError::Poison)?;
        Ok(*guard)
    }
}

#[async_trait]
impl<TMessage> ListenerTrait<TMessage> for HybridListener<TMessage>
where
    TMessage: Send,
{
    async fn on_message(&self, message: TMessage) -> EGResult<()> {
        let mode = self.get_mode()?;
        match mode {
            ListenMode::EventDriven => self.delegated_listener.on_message(message).await,
            ListenMode::OnDemand => self.sender.send(message).map_err(|_| EGError::Send),
        }
    }
}
