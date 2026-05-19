use crate::{
    auth_spec::{AuthLeg, AuthLegTrait, AuthSpec},
    credentials::credential::Credential,
    destroy::Destroy,
    signing::{
        signable_auth_spec::SignableAuthSpec,
        signing_algorithm::SigningAlgorithm,
        signing_method::SigningMethod,
    },
    transport::transport::Transport,
};
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

pub struct AuthSpecBuilder<TState, TCredentials, TTransports>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
{
    authentication: Vec<Box<dyn AuthLegTrait<TState, TCredentials, TTransports>>>,
}

impl<TState, TCredentials, TTransports> AuthSpecBuilder<TState, TCredentials, TTransports>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            authentication: Vec::new(),
        }
    }
    pub fn begin_leg<TTransport, TMessage, TReply>(
        mut self,
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
    ) -> AuthLegBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    where
        TTransport: Transport<Message = TMessage, Reply = TReply> + Send + Sync + 'static,
        TMessage: Send + Sync + 'static,
        TReply: Send + Sync + 'static,
    {
        let authentication = std::mem::take(&mut self.authentication);
        let builder = AuthSpecBuilder { authentication };
        AuthLegBuilder::new(builder, get_transport, timeout)
    }
    pub fn build_spec(&mut self) -> AuthSpec<TState, TCredentials, TTransports> {
        AuthSpec::<TState, TCredentials, TTransports>::new(std::mem::take(&mut self.authentication))
    }
    /// Transition to building signing methods for a specific message type.
    ///
    /// This creates a `SigningOrContinueBuilder` that can be used to add
    /// signing methods and/or build the final spec.
    pub fn for_message_type<TMessage>(
        &mut self,
    ) -> SigningOrContinueBuilder<TState, TCredentials, TTransports, TMessage>
    where
        TCredentials: Credential,
        TMessage: Send + Sync + 'static,
    {
        SigningOrContinueBuilder {
            authentication: std::mem::take(&mut self.authentication),
            signing_methods: Vec::new(),
        }
    }
}

pub struct AuthLegBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransport: Transport<Message = TMessage, Reply = TReply> + Send + Sync + 'static,
{
    auth_spec_builder: Option<AuthSpecBuilder<TState, TCredentials, TTransports>>,
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    gather_values: Vec<Box<dyn Fn(&TState, &TCredentials, &mut TMessage) + Send + Sync + 'static>>,
    store_values:
        Vec<Box<dyn Fn(&TReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    AuthLegBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<Message = TMessage, Reply = TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    fn new(
        auth_spec_builder: AuthSpecBuilder<TState, TCredentials, TTransports>,
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
    ) -> Self {
        Self {
            auth_spec_builder: Some(auth_spec_builder),
            get_transport,
            timeout,
            gather_values: Vec::new(),
            store_values: Vec::new(),
        }
    }
    pub fn gather_value<TValue>(
        &mut self,
        get_value: fn(state: &TState, credentials: &TCredentials) -> TValue,
        pack_value: fn(message: &mut TMessage, value: &TValue) -> (),
    ) -> &mut Self
    where
        TValue: Send + Sync + 'static,
    {
        let gather_value =
            move |state: &TState, credentials: &TCredentials, message: &mut TMessage| {
                let value = get_value(state, credentials);
                pack_value(message, &value)
            };
        self.gather_values.push(Box::new(gather_value));
        self
    }
    pub fn store_value<TValue>(
        &mut self,
        unpack_value: fn(reply: &TReply) -> StockTrekResult<TValue>,
        set_value: fn(state: &mut TState, value: &TValue),
    ) -> &mut Self
    where
        TValue: Send + Sync + 'static,
    {
        let store_value = move |reply: &TReply, state: &mut TState| -> StockTrekResult<()> {
            let value = unpack_value(reply)?;
            set_value(state, &value);
            Ok(())
        };
        self.store_values.push(Box::new(store_value));
        self
    }
    pub fn build_leg(&mut self) -> AuthSpecBuilder<TState, TCredentials, TTransports> {
        let auth_leg =
            AuthLeg::<TState, TCredentials, TTransports, TTransport, TMessage, TReply>::new(
                self.get_transport,
                self.timeout,
                std::mem::take(&mut self.gather_values),
                std::mem::take(&mut self.store_values),
            );
        let mut builder = self
            .auth_spec_builder
            .take()
            .expect("Already built AuthLegBuilder");
        builder.authentication.push(Box::new(auth_leg));
        builder
    }
}

/// Builder that allows adding signing methods and/or building the final spec.
///
/// This is the next step after calling `for_message_type` on an `AuthSpecBuilder`.
pub struct SigningOrContinueBuilder<TState, TCredentials, TTransports, TMessage>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Credential + Destroy + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
{
    authentication: Vec<Box<dyn AuthLegTrait<TState, TCredentials, TTransports>>>,
    signing_methods: Vec<SigningMethod<TState, TCredentials, TMessage>>,
}

impl<TState, TCredentials, TTransports, TMessage>
    SigningOrContinueBuilder<TState, TCredentials, TTransports, TMessage>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Credential + Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
{
    /// Add a signing method for the message type.
    ///
    /// * `algorithm` - The signing algorithm (HMAC-SHA256, Ed25519, etc.)
    /// * `message_to_bytes` - A function that extracts the bytes to sign from the message.
    ///   Receives state and credentials for context (e.g., to insert a timestamp/nonce first).
    /// * `pack_signature` - A function that writes the resulting signature bytes into the message.
    pub fn sign_message(
        mut self,
        algorithm: SigningAlgorithm,
        message_to_bytes: fn(&TState, &TCredentials, &TMessage) -> StockTrekResult<Vec<u8>>,
        pack_signature: fn(&mut TMessage, &[u8]) -> StockTrekResult<()>,
    ) -> Self {
        let method = SigningMethod::new(
            algorithm,
            Box::new(message_to_bytes),
            Box::new(pack_signature),
        );
        self.signing_methods.push(method);
        self
    }

    /// Build a `SignableAuthSpec` which includes both authentication and signing capabilities.
    pub fn build_signable(self) -> SignableAuthSpec<TState, TCredentials, TTransports, TMessage> {
        let auth_spec = AuthSpec::<TState, TCredentials, TTransports>::new(self.authentication);
        SignableAuthSpec::new(auth_spec, self.signing_methods)
    }
}
