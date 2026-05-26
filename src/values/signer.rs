use crate::{
    credentials::credential::Credential,
    sign::{
        encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding},
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
    },
};
use stock_trek::error::result::StockTrekResult;

pub type Signer<TState, TCredentials, TUnsigned, TSigned> =
    Box<dyn SignerTrait<TState, TCredentials, TUnsigned, TSigned>>;

pub trait SignerTrait<TState, TCredentials, TUnsigned, TSigned>: Send + Sync {
    fn sign(
        &self,
        state: &TState,
        credentials: &TCredentials,
        unsigned: &TUnsigned,
    ) -> StockTrekResult<TSigned>;
}

pub struct SignatureGenerator<TState, TCredentials, TUnsigned> {
    get_credential: fn(&TCredentials) -> &dyn Credential,
    signable_fields: Vec<SignableField<TState, TUnsigned>>,
    signing_algorithm: SigningAlgorithm,
    byte_encoding: ByteEncoding,
}

pub type SignableField<TState, TUnsigned> = fn(&TState, &TUnsigned) -> Option<Vec<u8>>;

impl<TState, TCredentials, TUnsigned> SignatureGenerator<TState, TCredentials, TUnsigned> {
    pub fn new(
        get_credential: fn(&TCredentials) -> &dyn Credential,
        signable_fields: Vec<SignableField<TState, TUnsigned>>,
        signing_algorithm: SigningAlgorithm,
        byte_encoding: ByteEncoding,
    ) -> Self {
        Self {
            get_credential,
            signable_fields,
            signing_algorithm,
            byte_encoding,
        }
    }
}

impl<TState, TCredentials, TUnsigned> SignerTrait<TState, TCredentials, TUnsigned, String>
    for SignatureGenerator<TState, TCredentials, TUnsigned>
{
    fn sign(
        &self,
        state: &TState,
        credentials: &TCredentials,
        unsigned: &TUnsigned,
    ) -> StockTrekResult<String> {
        let mut data = Vec::new();
        for signable_field in &self.signable_fields {
            if let Some(signable_field_data) = signable_field(state, unsigned) {
                data.extend(signable_field_data);
            }
        }
        let credential = (self.get_credential)(credentials);
        let key = credential.credential();
        let signer: DataSigner = self.signing_algorithm.into();
        let signature_bytes = signer.sign(&data, &key)?;
        let byte_encoder: ByteEncoder = self.byte_encoding.into();
        let signature = byte_encoder.encode(&signature_bytes);
        Ok(signature)
    }
}
