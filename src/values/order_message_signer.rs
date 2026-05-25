use crate::{
    credentials::credential::Credential,
    sign::{
        encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding},
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
    },
};
use stock_trek::error::result::StockTrekResult;

pub struct OrderMessageSigner<TState, TCredentials, TMessage> {
    get_credential: fn(&TCredentials) -> &dyn Credential,
    signable_fields: Vec<OrderMessageSignableFields<TState, TMessage>>,
    signing_algorithm: SigningAlgorithm,
    byte_encoding: ByteEncoding,
    write_signature: fn(&String, &mut TMessage),
}

impl<TState, TCredentials, TMessage> OrderMessageSigner<TState, TCredentials, TMessage> {
    pub fn new(
        get_credential: fn(&TCredentials) -> &dyn Credential,
        signable_fields: Vec<OrderMessageSignableFields<TState, TMessage>>,
        signing_algorithm: SigningAlgorithm,
        byte_encoding: ByteEncoding,
        write_signature: fn(&String, &mut TMessage),
    ) -> Self {
        Self {
            get_credential,
            signable_fields,
            signing_algorithm,
            byte_encoding,
            write_signature,
        }
    }
    pub fn sign(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()> {
        let mut data = Vec::new();
        for signable_field in &self.signable_fields {
            data.extend(signable_field(state, message));
        }
        let credential = (self.get_credential)(credentials);
        let key = credential.credential();
        let signer: DataSigner = self.signing_algorithm.into();
        let signature_bytes = signer.sign(&data, &key)?;
        let byte_encoder: ByteEncoder = self.byte_encoding.into();
        let signature = byte_encoder.encode(&signature_bytes);
        (self.write_signature)(&signature, message);
        Ok(())
    }
}

pub type OrderMessageSignableFields<TState, TMessage> =
    fn(state: &TState, message: &TMessage) -> Vec<u8>;

pub type OrderMessageSignableField<TState, TMessage> = fn(&TState, &TMessage) -> Option<Vec<u8>>;

#[allow(unused)]
macro_rules! order_message_signer {
    (
      $struct_name:ident < $state_name:ident, $order_message_name:ident > ( $($field_name:ident),* $(,)? )
    ) => {

        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct $struct_name {
            $($field_name: OrderMessageSignableField<$state_name, $order_message_name>,)*
        }

        impl OrderMessageSignableFieldsTrait<$state_name, $order_message_name> for $struct_name {
            fn signable_bytes(&self, state: &$state_name, message: &$order_message_name) -> Vec<u8> {
                let mut bytes = Vec::new();

                $(
                    if let Some($field_name) = (self.$field_name)(state, message) {
                        bytes.extend($field_name);
                    }
                )*

                bytes
            }
        }
    };
}
