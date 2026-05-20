use crate::{
    build::auth_spec_builder::AuthSpecBuilder,
    destroy::Destroy,
    message_leg::MessageLegGeneric,
    sign::{
        encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding},
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
    },
    transport::transport::Transport,
};
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

pub struct MessageLegBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    auth_spec_builder: Option<AuthSpecBuilder<TState, TCredentials, TTransports, TMessage, TReply>>,
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    gather_signatures: Vec<
        Box<
            dyn Fn(&TState, &TCredentials, &mut TMessage) -> StockTrekResult<()>
                + Send
                + Sync
                + 'static,
        >,
    >,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    MessageLegBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        auth_spec_builder: AuthSpecBuilder<TState, TCredentials, TTransports, TMessage, TReply>,
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
    ) -> Self {
        Self {
            auth_spec_builder: Some(auth_spec_builder),
            get_transport,
            timeout,
            gather_signatures: Vec::new(),
        }
    }
    pub fn gather_signature(
        &mut self,
        get_bytes: fn(&TState, &TMessage) -> Vec<u8>,
        get_key: fn(&TState, &TCredentials) -> Vec<u8>,
        signing_algorithm: SigningAlgorithm,
        byte_encoding: ByteEncoding,
        pack_signature: fn(String, &mut TMessage),
    ) -> &mut Self {
        let data_signer: DataSigner = signing_algorithm.into();
        let byte_encoder: ByteEncoder = byte_encoding.into();
        let gather_signature = move |state: &TState,
                                     credentials: &TCredentials,
                                     message: &mut TMessage|
              -> StockTrekResult<()> {
            let data = get_bytes(state, message);
            let key = get_key(state, credentials);
            let signature_bytes = data_signer.sign(&data, &key)?;
            let signature = byte_encoder.encode(&signature_bytes);
            pack_signature(signature, message);
            Ok(())
        };
        self.gather_signatures.push(Box::new(gather_signature));
        self
    }
    pub fn build_leg(
        &mut self,
    ) -> AuthSpecBuilder<TState, TCredentials, TTransports, TMessage, TReply> {
        let message_leg = MessageLegGeneric::<
            TState,
            TCredentials,
            TTransports,
            TTransport,
            TMessage,
            TReply,
        >::new(
            self.get_transport,
            self.timeout,
            std::mem::take(&mut self.gather_signatures),
        );
        let mut builder = self
            .auth_spec_builder
            .take()
            .expect("Already built MessageSignatureBuilder");
        builder.message = Box::new(message_leg);
        builder
    }
}
