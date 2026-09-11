use crate::{
    clients::client::{HttpClient, WebsocketClient},
    connector::Connector,
    error::{EGError, EGResult},
};
use exchange_types::{
    exchange::ETExchange,
    new_types::{Milliseconds, UsageCount},
    rate_limited::RateLimit,
    request::{ETHttpRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc::UnboundedSender, task::JoinHandle};

pub struct AutoResyncConnector<Exchange, Client> {
    connector: Arc<Connector<Exchange, Client>>,
    resync_handle: Mutex<Option<AutoResyncHandle>>,
}

struct AutoResyncHandle {
    sender: UnboundedSender<ClockSyncCommand>,
    join: JoinHandle<()>,
}

enum ClockSyncCommand {
    SetFrequency(Duration),
    Stop,
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange + Send + Sync + 'static,
    Client: Send + Sync + 'static,
{
    pub(crate) fn new(connector: Arc<Connector<Exchange, Client>>) -> Self {
        Self {
            connector,
            resync_handle: Mutex::new(None),
        }
    }
    pub fn duration_since_last_sync(&self) -> EGResult<Option<Duration>> {
        self.connector.duration_since_last_sync()
    }
    pub fn remaining_rate_limit_capacity(&self) -> EGResult<HashMap<RateLimit, UsageCount>> {
        self.connector.remaining_rate_limit_capacity()
    }
    pub fn server_time_estimate(&self) -> EGResult<Milliseconds> {
        self.connector.server_time_estimate()
    }
    pub async fn stop_clock_sync(&self) -> EGResult<()> {
        let handle = self
            .resync_handle
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .take();
        let Some(handle) = handle else { return Ok(()) };
        let _ = handle.sender.send(ClockSyncCommand::Stop);
        drop(handle.sender); // let the loop drain
        match handle.join.await {
            Ok(()) => Ok(()),
            Err(e) if e.is_panic() => Err(EGError::AutoResyncClockPanicked),
            Err(_) => Ok(()),
        }
    }
    async fn set_clock_sync_frequency<F, Fut>(
        &self,
        frequency: Duration,
        sync_clock_fn: F,
    ) -> EGResult<()>
    where
        F: Fn(Arc<Connector<Exchange, Client>>) -> Fut + Send + 'static,
        Fut: Future<Output = EGResult<()>> + Send + 'static,
    {
        if frequency < Duration::from_mins(1) {
            return Err(EGError::InvalidSyncFrequency);
        }
        let (first_sync_sender, first_sync_receiver) = tokio::sync::oneshot::channel();
        {
            let mut resync_handle = self
                .resync_handle
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?;
            if let Some(handle) = resync_handle.as_ref() {
                if handle.join.is_finished()
                    || handle
                        .sender
                        .send(ClockSyncCommand::SetFrequency(frequency))
                        .is_err()
                {
                    *resync_handle = None;
                    return Err(EGError::AutoResyncClockPanicked);
                }
                return Ok(());
            }
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            let connector = self.connector.clone();
            let join = tokio::spawn(async move {
                let mut current = frequency;
                let _ = sync_clock_fn(connector.clone()).await;
                let _ = first_sync_sender.send(());
                loop {
                    tokio::select! {
                        cmd = receiver.recv() => match cmd {
                            Some(ClockSyncCommand::SetFrequency(new_freq)) => {
                                current = new_freq;
                                continue;
                            }
                            Some(ClockSyncCommand::Stop) | None => break,
                        },
                        _ = tokio::time::sleep(current) => {
                            let _ = sync_clock_fn(connector.clone()).await;
                        }
                    }
                }
            });
            *resync_handle = Some(AutoResyncHandle { sender, join });
        }
        let _ = first_sync_receiver.await;
        Ok(())
    }
}

impl<Exchange, Client> Drop for AutoResyncConnector<Exchange, Client> {
    fn drop(&mut self) {
        let handle = match self.resync_handle.get_mut() {
            Ok(handle) => handle.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(handle) = handle {
            let _ = handle.sender.send(ClockSyncCommand::Stop);
            handle.join.abort();
        }
    }
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: HttpClient,
{
    pub async fn sync_clock_http(&self) -> EGResult<()> {
        self.connector.sync_clock_http().await
    }
    pub async fn set_clock_sync_frequency_http(&self, frequency: Duration) -> EGResult<()>
    where
        Exchange: Send + Sync + 'static,
        Exchange::ServerTimeRequestHttp: Send,
        Client: Send + Sync + 'static,
    {
        self.set_clock_sync_frequency(frequency, async |connector| {
            connector.sync_clock_http().await
        })
        .await
    }
    pub async fn send_http<Response>(
        &self,
        request: impl ETHttpRequest<Exchange = Exchange, Response = Response>,
    ) -> EGResult<Response>
    where
        Response: ETHttpResponse,
    {
        self.connector.send_http(request).await
    }
}

impl<Exchange, Client> AutoResyncConnector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> EGResult<()> {
        self.connector.connect().await
    }
    pub fn is_connected(&self) -> EGResult<bool> {
        self.connector.is_connected()
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        self.connector.disconnect().await
    }
    pub async fn sync_clock_websocket(&self) -> EGResult<()> {
        self.connector.sync_clock_websocket().await
    }
    pub async fn set_clock_sync_frequency_websocket(&self, frequency: Duration) -> EGResult<()>
    where
        Exchange: Send + Sync + 'static,
        Exchange::ServerTimeRequestWebsocket: Send,
        Client: Send + Sync + 'static,
    {
        self.set_clock_sync_frequency(frequency, async |connector| {
            connector.sync_clock_websocket().await
        })
        .await
    }
    pub async fn send_websocket<Response>(
        &self,
        request: impl ETWebsocketRequest<Exchange = Exchange, Response = Response>,
    ) -> EGResult<Response>
    where
        Response: ETWebsocketResponse,
    {
        self.connector.send_websocket(request).await
    }
}

impl<Exchange, Client> std::fmt::Debug for AutoResyncConnector<Exchange, Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoResyncConnector")
            .field("connector", &self.connector)
            .finish()
    }
}
