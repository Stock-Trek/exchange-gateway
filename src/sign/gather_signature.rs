use crate::{
    destroy::Destroy,
    sign::{
        encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding},
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
    },
};
use stock_trek::error::result::StockTrekResult;

pub struct GatherSignature<TState, TCredentials, TMessage>
where
    TCredentials: Destroy + Send + Sync + 'static,
{
    get_bytes: fn(&TState, &TCredentials, &TMessage) -> Vec<u8>,
    get_key: fn(&TCredentials) -> Vec<u8>,
    data_signer: DataSigner,
    byte_encoder: ByteEncoder,
    pack_signature: fn(String, &mut TMessage),
}

impl<TState, TCredentials, TMessage> GatherSignature<TState, TCredentials, TMessage>
where
    TCredentials: Destroy + Send + Sync + 'static,
{
    pub fn new(
        get_bytes: fn(&TState, &TCredentials, &TMessage) -> Vec<u8>,
        get_key: fn(&TCredentials) -> Vec<u8>,
        signing_algorithm: SigningAlgorithm,
        byte_encoding: ByteEncoding,
        pack_signature: fn(String, &mut TMessage),
    ) -> Self {
        Self {
            get_bytes,
            get_key,
            data_signer: signing_algorithm.into(),
            byte_encoder: byte_encoding.into(),
            pack_signature,
        }
    }
    pub fn sign(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()> {
        let bytes_to_sign = (self.get_bytes)(state, credentials, message);
        let key = (self.get_key)(credentials);
        let signature_bytes = self.data_signer.sign(&bytes_to_sign, &key)?;
        let signature = self.byte_encoder.encode(&signature_bytes);
        (self.pack_signature)(signature, message);
        Ok(())
    }
}
