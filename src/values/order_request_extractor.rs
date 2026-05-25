use crate::values::{extractor::ExtractorTrait, signer::Signer};
use rust_decimal::Decimal;
use stock_trek::{asset_id::AssetId, order::order_request::OrderRequest};

pub type OrderRequestExtractor<TOrderRequestBody> =
    Box<dyn ExtractorTrait<OrderRequest<AssetId, Decimal>, TOrderRequestBody>>;

pub trait SignedOrderRequestExtractor<TState, TCredentials, TSigned> {
    fn extract_signed(
        &self,
        state: &TState,
        credentials: &TCredentials,
        order_request: &OrderRequest<AssetId, Decimal>,
    ) -> TSigned;
}

pub struct SignedExtractor<TState, TCredentials, TUnsigned, TSigned> {
    pub(crate) to_unsigned: OrderRequestExtractor<TUnsigned>,
    pub(crate) signer: Signer<TState, TCredentials, TUnsigned, TSigned>,
}

impl<TState, TCredentials, TUnsigned, TSigned>
    SignedExtractor<TState, TCredentials, TUnsigned, TSigned>
{
    pub fn new(
        to_unsigned: OrderRequestExtractor<TUnsigned>,
        signer: Signer<TState, TCredentials, TUnsigned, TSigned>,
    ) -> Self {
        Self {
            to_unsigned,
            signer,
        }
    }
}

#[allow(unused)]
macro_rules! order_request_extractor {
    (
        $unsigned_single:ident, $unsigned_oco:ident, $unsigned_oto:ident, $unsigned_otoco:ident,
        < $state:ident, $credentials:ident >
        $signed_single:ident, $signed_oco:ident, $signed_oto:ident, $signed_otoco:ident,
    ) => {
        pub struct Extractors {
            single: crate::values::order_request_extractor::SignedExtractor<
                $state,
                $credentials,
                $unsigned_single,
                $signed_single,
            >,
            oco: crate::values::order_request_extractor::SignedExtractor<
                $state,
                $credentials,
                $unsigned_oco,
                $signed_oco,
            >,
            oto: crate::values::order_request_extractor::SignedExtractor<
                $state,
                $credentials,
                $unsigned_oto,
                $signed_oto,
            >,
            otoco: crate::values::order_request_extractor::SignedExtractor<
                $state,
                $credentials,
                $unsigned_otoco,
                $signed_otoco,
            >,
        }
        impl Extractors {
            pub fn new(
                single_to_unsigned: crate::values::order_request_extractor::OrderRequestExtractor<
                    $unsigned_single,
                >,
                single_signer: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedSingleBody,
                    SignedSingleBody,
                >,
                oco_to_unsigned: crate::values::order_request_extractor::OrderRequestExtractor<
                    $unsigned_oco,
                >,
                oco_signer: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedOcoBody,
                    SignedOcoBody,
                >,
                oto_to_unsigned: crate::values::order_request_extractor::OrderRequestExtractor<
                    $unsigned_oto,
                >,
                oto_signer: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedOtoBody,
                    SignedOtoBody,
                >,
                otoco_to_unsigned: crate::values::order_request_extractor::OrderRequestExtractor<
                    $unsigned_otoco,
                >,
                otoco_signer: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedOtOcoBody,
                    SignedOtOcoBody,
                >,
            ) -> Self {
                Self {
                    single: crate::values::order_request_extractor::SignedExtractor::new(
                        single_to_unsigned,
                        single_signer,
                    ),
                    oco: crate::values::order_request_extractor::SignedExtractor::new(
                        oco_to_unsigned,
                        oco_signer,
                    ),
                    oto: crate::values::order_request_extractor::SignedExtractor::new(
                        oto_to_unsigned,
                        oto_signer,
                    ),
                    otoco: crate::values::order_request_extractor::SignedExtractor::new(
                        otoco_to_unsigned,
                        otoco_signer,
                    ),
                }
            }
        }
        impl<TState, TCredentials>
            crate::values::order_request_extractor::SignedOrderRequestExtractor<
                TState,
                TCredentials,
                SignedOrderRequestBody,
            > for Extractors
        {
            fn extract_signed(
                &self,
                state: &TState,
                credentials: &TCredentials,
                order_request: &::stock_trek::prelude::OrderRequest<AssetId, Decimal>,
            ) -> SignedOrderRequestBody {
                match order_request {
                    ::stock_trek::prelude::OrderRequest::Single(single) => {
                        let unsigned = self.single.to_unsigned.extract(single);
                        self.single.signer.sign(state, credentials, unsigned)
                    }
                }
            }
        }
        #[derive(Debug)]
        pub enum UnsignedOrderRequestBody {
            Single($unsigned_single),
            Oco($unsigned_oco),
            Oto($unsigned_oto),
            OtOco($unsigned_otoco),
        }
        pub struct UnsignedExtractors {
            single: crate::values::single_order_extractor::UnsignedSingleOrderExtractor<
                $unsigned_single,
            >,
            oco: crate::values::oco_order_extractor::UnsignedOcoOrderExtractor<$unsigned_oco>,
            oto: crate::values::oto_order_extractor::UnsignedOtoOrderExtractor<$unsigned_oto>,
            otoco:
                crate::values::otoco_order_extractor::UnsignedOtOcoOrderExtractor<$unsigned_otoco>,
        }
        impl UnsignedExtractors {
            pub fn new(
                single: crate::values::single_order_extractor::UnsignedSingleOrderExtractor<
                    $unsigned_single,
                >,
                oco: crate::values::oco_order_extractor::UnsignedOcoOrderExtractor<$unsigned_oco>,
                oto: crate::values::oto_order_extractor::UnsignedOtoOrderExtractor<$unsigned_oto>,
                otoco: crate::values::otoco_order_extractor::UnsignedOtOcoOrderExtractor<
                    $unsigned_otoco,
                >,
            ) -> crate::values::order_request_extractor::OrderRequestExtractor<
                UnsignedOrderRequestBody,
            > {
                Box::new(Self {
                    single,
                    oco,
                    oto,
                    otoco,
                })
            }
        }
        impl
            crate::values::extractor::ExtractorTrait<
                ::stock_trek::prelude::OrderRequest<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal,
                >,
                UnsignedOrderRequestBody,
            > for UnsignedExtractors
        {
            fn extract(
                &self,
                order_request: &::stock_trek::prelude::OrderRequest<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal,
                >,
            ) -> UnsignedOrderRequestBody {
                match order_request {
                    ::stock_trek::prelude::OrderRequest::Single(single) => {
                        UnsignedOrderRequestBody::Single(self.single.extract(single))
                    }
                    ::stock_trek::prelude::OrderRequest::OneCancelsOther(oco) => {
                        UnsignedOrderRequestBody::Oco(self.oco.extract(oco))
                    }
                    ::stock_trek::prelude::OrderRequest::OneTriggersOther(oto) => {
                        UnsignedOrderRequestBody::Oto(self.oto.extract(oto))
                    }
                    ::stock_trek::prelude::OrderRequest::OneTriggersOco(otoco) => {
                        UnsignedOrderRequestBody::OtOco(self.otoco.extract(otoco))
                    }
                }
            }
        }
        #[derive(Debug)]
        pub enum SignedOrderRequestBody {
            Single($signed_single),
            Oco($signed_oco),
            Oto($signed_oto),
            OtOco($signed_otoco),
        }
        pub struct SignedExtractors {
            single: crate::values::signer::Signer<
                $state,
                $credentials,
                UnsignedSingleBody,
                SignedSingleBody,
            >,
            oco:
                crate::values::signer::Signer<$state, $credentials, UnsignedOcoBody, SignedOcoBody>,
            oto:
                crate::values::signer::Signer<$state, $credentials, UnsignedOtoBody, SignedOtoBody>,
            otoco: crate::values::signer::Signer<
                $state,
                $credentials,
                UnsignedOtOcoBody,
                SignedOtOcoBody,
            >,
        }
        impl SignedExtractors {
            pub fn new(
                single: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedSingleBody,
                    SignedSingleBody,
                >,
                oco: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedOcoBody,
                    SignedOcoBody,
                >,
                oto: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedOtoBody,
                    SignedOtoBody,
                >,
                otoco: crate::values::signer::Signer<
                    $state,
                    $credentials,
                    UnsignedOtOcoBody,
                    SignedOtOcoBody,
                >,
            ) -> crate::values::signer::Signer<
                $state,
                $credentials,
                UnsignedOrderRequestBody,
                SignedOrderRequestBody,
            > {
                Box::new(Self {
                    single,
                    oco,
                    oto,
                    otoco,
                })
            }
        }
        impl
            crate::values::signer::SignerTrait<
                $state,
                $credentials,
                UnsignedOrderRequestBody,
                SignedOrderRequestBody,
            > for SignedExtractors
        {
            fn sign(
                &self,
                state: &$state,
                credentials: &$credentials,
                unsigned: &UnsignedOrderRequestBody,
            ) -> SignedOrderRequestBody {
                match unsigned {
                    UnsignedOrderRequestBody::Single(single) => {
                        SignedOrderRequestBody::Single(self.single.sign(state, credentials, single))
                    }
                    UnsignedOrderRequestBody::Oco(oco) => {
                        SignedOrderRequestBody::Oco(self.oco.sign(state, credentials, oco))
                    }
                    UnsignedOrderRequestBody::Oto(oto) => {
                        SignedOrderRequestBody::Oto(self.oto.sign(state, credentials, oto))
                    }
                    UnsignedOrderRequestBody::OtOco(otoco) => {
                        SignedOrderRequestBody::OtOco(self.otoco.sign(state, credentials, otoco))
                    }
                }
            }
        }
    };
}

#[allow(unused)]
pub(crate) use order_request_extractor;
