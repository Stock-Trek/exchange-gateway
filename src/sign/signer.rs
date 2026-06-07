use crate::sign::{
    encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding},
    encrypt::data_signer::DataSigner,
};
use stock_trek::error::result::StockTrekResult;

pub type Signer<TState, TUnsigned, TSigned> = Box<dyn SignerTrait<TState, TUnsigned, TSigned>>;

pub trait SignerTrait<TState, TUnsigned, TSigned>: Send + Sync {
    fn sign(&self, state: &TState, unsigned: &TUnsigned) -> StockTrekResult<TSigned>;
}

pub struct SignatureGenerator<TState, TUnsigned> {
    signer: DataSigner,
    signable_fields: Vec<SignableField<TState, TUnsigned>>,
    byte_encoding: ByteEncoding,
}

pub type SignableField<TState, TUnsigned> = fn(&TState, &TUnsigned) -> Option<Vec<u8>>;

impl<TState, TUnsigned> SignatureGenerator<TState, TUnsigned> {
    pub fn new(
        signer: DataSigner,
        signable_fields: Vec<SignableField<TState, TUnsigned>>,
        byte_encoding: ByteEncoding,
    ) -> Self {
        Self {
            signer,
            signable_fields,
            byte_encoding,
        }
    }
}

impl<TState, TUnsigned> SignerTrait<TState, TUnsigned, String>
    for SignatureGenerator<TState, TUnsigned>
{
    fn sign(&self, state: &TState, unsigned: &TUnsigned) -> StockTrekResult<String> {
        let mut data = Vec::new();
        for signable_field in &self.signable_fields {
            if let Some(signable_field_data) = signable_field(state, unsigned) {
                data.extend(signable_field_data);
            }
        }
        let signature_bytes = self.signer.sign(&data)?;
        let byte_encoder: ByteEncoder = self.byte_encoding.into();
        let signature = byte_encoder.encode(&signature_bytes);
        Ok(signature)
    }
}
