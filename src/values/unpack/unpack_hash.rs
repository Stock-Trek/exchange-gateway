use crate::values::{
    hash_encoder::{Encoding, HashAlgorithm, HasherEncoder},
    unpack_value::UnpackValue,
};
use stock_trek::error::result::StockTrekResult;

pub struct UnpackHash<TReply, THashableValue, TToBytes>
where
    TToBytes: Fn(&THashableValue) -> &Vec<u8>,
{
    unpack_hashable_value: Box<dyn UnpackValue<THashableValue, TReply>>,
    hash_encoder: HasherEncoder<THashableValue, TToBytes>,
}

impl<TReply, THashableValue, TToBytes> UnpackHash<TReply, THashableValue, TToBytes>
where
    TToBytes: Fn(&THashableValue) -> &Vec<u8>,
{
    pub fn new(
        unpack_hashable_value: Box<dyn UnpackValue<THashableValue, TReply>>,
        to_bytes: TToBytes,
        hash_algorithm: HashAlgorithm,
        encoding: Encoding,
    ) -> Self {
        Self {
            unpack_hashable_value,
            hash_encoder: HasherEncoder::new(to_bytes, hash_algorithm, encoding),
        }
    }
}

impl<TReply, THashableValue, TToBytes> UnpackValue<String, TReply>
    for UnpackHash<TReply, THashableValue, TToBytes>
where
    TToBytes: Fn(&THashableValue) -> &Vec<u8>,
{
    fn unpack(&self, reply: &TReply) -> StockTrekResult<String> {
        let hashable_value = self.unpack_hashable_value.unpack(reply)?;
        let encoded = self.hash_encoder.hash_encode(&hashable_value)?;
        Ok(encoded)
    }
}
