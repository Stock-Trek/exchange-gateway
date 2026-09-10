use crate::{
    clients::client::{HttpClient, WebsocketClient},
    connector::Connector,
    error::{EGError, EGResult},
};
use exchange_types::{
    exchange::ETExchange,
    new_types::Milliseconds,
    request::{ETHttpRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETWebsocketResponse},
};
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{RecvTimeoutError, Sender},
    },
    thread::JoinHandle,
    time::Duration,
};

pub struct AutoResyncConnector<Exchange, Client> {
    connector: Arc<Connector<Exchange, Client>>,
    resync_handle: Arc<Mutex<Option<AutoResyncHandle>>>,
}

struct AutoResyncHandle {
    sender: Sender<ClockSyncCommand>,
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
            resync_handle: Arc::new(Mutex::new(None)),
        }
    }
    pub fn duration_since_last_sync(&self) -> EGResult<Duration> {
        self.connector.duration_since_last_sync()
    }
    pub fn server_time_estimate(&self) -> EGResult<Milliseconds> {
        self.connector.server_time_estimate()
    }
    async fn set_clock_sync_frequency<SyncClockFn>(
        &self,
        frequency: Duration,
        sync_clock_fn: SyncClockFn,
    ) -> EGResult<()>
    where
        SyncClockFn: AsyncFn(Arc<Connector<Exchange, Client>>) -> EGResult<()> + Send + 'static,
    {
        let mut resync_handle = self
            .resync_handle
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        if let Some(handle) = resync_handle.as_ref()
            && handle
                .sender
                .send(ClockSyncCommand::SetFrequency(frequency))
                .is_ok()
        {
            return Ok(());
        }
        *resync_handle = None;
        let (sender, receiver) = std::sync::mpsc::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EGError::External(Box::new(e)))?;
        let connector_clone = self.connector.clone();
        let join = std::thread::Builder::new()
            .name("exchange-gateway-auto-resync".into())
            .spawn(move || {
                let mut current = frequency;
                loop {
                    match receiver.recv_timeout(current) {
                        Ok(ClockSyncCommand::SetFrequency(new_duration)) => current = new_duration,
                        Ok(ClockSyncCommand::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {
                            let future = sync_clock_fn(connector_clone.clone());
                            let _ = runtime.block_on(future);
                        }
                    }
                }
            })
            .map_err(|e| EGError::External(Box::new(e)))?;
        *resync_handle = Some(AutoResyncHandle { sender, join });
        Ok(())
    }
    pub fn stop_clock_sync(&self) -> EGResult<()> {
        let resync_handle = self
            .resync_handle
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .take();
        match resync_handle {
            Some(handle) => {
                let _ = handle.sender.send(ClockSyncCommand::Stop);
                drop(handle.sender);
                match handle.join.join() {
                    Ok(()) => Ok(()),
                    Err(_) => Err(EGError::AutoResyncClockPanicked),
                }
            }
            None => Ok(()),
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

impl<Client, SyncRequest> std::fmt::Debug for AutoResyncConnector<Client, SyncRequest> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorImpl")
            .field("connector", &self.connector)
            .field("resync", &"<resync>")
            .finish()
    }
}
