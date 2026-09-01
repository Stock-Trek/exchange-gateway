use crate::error::EGResult;
use exchange_types::signer::Signer;

pub(crate) trait IntoSigned {
    type Signed;
    fn into_signed(self, signer: &Signer) -> EGResult<Self::Signed>;
}
