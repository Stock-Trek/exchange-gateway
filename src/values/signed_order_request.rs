macro_rules! signed_order_request {
    (
        < $state:ty, $credentials:ty >,
        $single_message:ty : $single_extractor:ty,
        $oco_message:ty : $oco_extractor:ty,
        $oto_message:ty : $oto_extractor:ty,
        $otoco_message:ty : $otoco_extractor:ty,
    ) => {
        pub enum SignedOrderRequestMessage {
            Single($single_message),
            Oco($oco_message),
            Oto($oto_message),
            OtOco($otoco_message),
        }

        pub struct SignedOrderRequestExtractor {
            single: $single_extractor,
            oco: $oco_extractor,
            oto: $oto_extractor,
            otoco: $otoco_extractor,
        }

        impl SignedOrderRequestExtractor {
            pub fn new(
                single: $single_extractor,
                oco: $oco_extractor,
                oto: $oto_extractor,
                otoco: $otoco_extractor,
            ) -> crate::values::signer::Signer<
                $state,
                $credentials,
                ::stock_trek::prelude::OrderRequest<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal,
                >,
                SignedOrderRequestMessage,
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
                ::stock_trek::prelude::OrderRequest<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal,
                >,
                SignedOrderRequestMessage,
            > for SignedOrderRequestExtractor
        {
            fn sign(
                &self,
                state: &$state,
                credentials: &$credentials,
                order: &::stock_trek::prelude::OrderRequest<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal,
                >,
            ) -> ::stock_trek::error::result::StockTrekResult<SignedOrderRequestMessage> {
                Ok(match order {
                    ::stock_trek::prelude::OrderRequest::Single(single) => {
                        SignedOrderRequestMessage::Single(self.single.sign(
                            state,
                            credentials,
                            single,
                        )?)
                    }
                    ::stock_trek::prelude::OrderRequest::OneCancelsOther(oco) => {
                        SignedOrderRequestMessage::Oco(self.oco.sign(state, credentials, oco)?)
                    }
                    ::stock_trek::prelude::OrderRequest::OneTriggersOther(oto) => {
                        SignedOrderRequestMessage::Oto(self.oto.sign(state, credentials, oto)?)
                    }
                    ::stock_trek::prelude::OrderRequest::OneTriggersOco(otoco) => {
                        SignedOrderRequestMessage::OtOco(self.otoco.sign(
                            state,
                            credentials,
                            otoco,
                        )?)
                    }
                })
            }
        }
    };
}

pub(crate) use signed_order_request;
