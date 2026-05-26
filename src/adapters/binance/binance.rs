use crate::{
    adapters::binance::{
        binance_structs::{
            BinanceAuthMessage, BinanceAuthReply, BinanceCredentials, BinanceHttpTransport,
            BinanceHttpTransports, BinanceOrderReply, BinanceState,
        },
        macroable::BinanceAuthMessageExtractor,
    },
    auth_spec::AuthSpec,
    authenticate_leg::AuthenticateLegImpl,
    message_leg::MessageLeg,
    sign::{encode::byte_encoding::ByteEncoding, encrypt::signing_algorithm::SigningAlgorithm},
    values::{
        order_response_extractor::OrderResponseExtractor,
        signed_order_request_extractor::signed_order_request_extractor,
        signed_order_variant_extractor::signed_order_variant_extractor, signer::SignatureGenerator,
        store_auth_value::StoreAuthValueImpl,
    },
};
use chrono::Duration;

fn create_http_auth_spec() -> AuthSpec<
    BinanceState,
    BinanceCredentials,
    BinanceHttpTransports,
    BinanceHttpTransport,
    SignedOrderRequestMessage,
    BinanceOrderReply,
> {
    AuthSpec::<
        BinanceState,
        BinanceCredentials,
        BinanceHttpTransports,
        BinanceHttpTransport,
        SignedOrderRequestMessage,
        BinanceOrderReply,
    >::new(
        vec![AuthenticateLegImpl::<
            BinanceState,
            BinanceCredentials,
            BinanceHttpTransports,
            BinanceHttpTransport,
            BinanceAuthMessage,
            BinanceAuthReply,
        >::new(
            |t| &t.http,
            Duration::seconds(20),
            // TODO
            BinanceAuthMessageExtractor::new(|_s, _c, _t| "".to_string()),
            vec![StoreAuthValueImpl::new(
                |reply| Ok(reply.id.clone()),
                |state, value| state.id = value.clone(),
            )],
        )],
        MessageLeg::new(
            |t| &t.http,
            Duration::seconds(20),
            SignedOrderRequestExtractor::new(
                single::SignedOrderExtractor::new(
                    single::UnsignedOrderFieldExtractors::new(|o| o.base.to_string(), |o| 123),
                    single::SignedOrderFieldExtractors::new(SignatureGenerator::<
                        BinanceState,
                        BinanceCredentials,
                        single::UnsignedOrderMessage,
                    >::new(
                        |c| &c.api_key,
                        vec![|s, u| Some(u.symbol.to_string().into_bytes())],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
                oco::SignedOrderExtractor::new(
                    oco::UnsignedOrderFieldExtractors::new(|o| o.primary.base.to_string(), |o| 123),
                    oco::SignedOrderFieldExtractors::new(SignatureGenerator::new(
                        |c| &c.api_key,
                        vec![],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
                oto::SignedOrderExtractor::new(
                    oto::UnsignedOrderFieldExtractors::new(|o| o.primary.base.to_string(), |o| 123),
                    oto::SignedOrderFieldExtractors::new(SignatureGenerator::new(
                        |c| &c.api_key,
                        vec![],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
                otoco::SignedOrderExtractor::new(
                    otoco::UnsignedOrderFieldExtractors::new(
                        |o| o.primary.base.to_string(),
                        |o| 123,
                    ),
                    otoco::SignedOrderFieldExtractors::new(SignatureGenerator::new(
                        |c| &c.api_key,
                        vec![],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
            ),
            OrderResponseExtractor::new(|response| {
                response.id.clone().unwrap_or("Missing".to_string())
            }),
        ),
    )
}

// impl AdapterCreatorTrait<BinanceCredentials, BinanceHttpTransports> for BinanceHttpAdapterCreator {
//     fn create_adapter(
//         &self,
//         credentials: BinanceCredentials,
//         transports: BinanceHttpTransports,
//     ) -> Adapter {
//         let id = ExchangeId("Binance".to_string());
//         let capabilities = vec![
//             Capability::QuoteQuantity(QuoteQuantityCapability::AllowLimitPricing),
//             Capability::QuoteQuantity(QuoteQuantityCapability::AllowTriggeredTiming),
//             Capability::MultiLeg(MultiLegCapability::OneCancelsOther),
//             Capability::MultiLeg(MultiLegCapability::OneTriggersOther),
//             Capability::MultiLeg(MultiLegCapability::OneTriggersOco),
//         ];
//         let increments = IncrementSizesBuilder::new()
//             .with(
//                 AssetId::bitcoin_native(),
//                 AssetId::base_usdc(),
//                 Decimal::from_i128_with_scale(1, 3),
//                 Decimal::from_i128_with_scale(1, 3),
//             )
//             .build();
//     let mut tickers = HashMap::new();
//     tickers.insert(AssetId::base_usdc(), "APT".to_string());
//     tickers.insert(AssetId::bitcoin_native(), "APT".to_string());
//     tickers
//         let auth_spec = create_http_auth_spec();
//         Adapter {
//             id,
//             capabilities,
//             increments,
//             symbol_ticker_divider: None,
//             tickers,
//             exchange_connector: ExchangeConnectorImpl::new(auth_spec, credentials, transports),
//         }
//     }
// }

signed_order_variant_extractor! {
    single,
    ::stock_trek::order::orders::single::SingleOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::binance_structs::BinanceState, crate::adapters::binance::binance_structs::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_variant_extractor! {
    oco,
    ::stock_trek::order::orders::one_cancels_other::OneCancelsOtherOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::binance_structs::BinanceState, crate::adapters::binance::binance_structs::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_variant_extractor! {
    oto,
    ::stock_trek::order::orders::one_triggers_other::OneTriggersOtherOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::binance_structs::BinanceState, crate::adapters::binance::binance_structs::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_variant_extractor! {
    otoco,
    ::stock_trek::order::orders::one_triggers_oco::OneTriggersOcoOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::binance_structs::BinanceState, crate::adapters::binance::binance_structs::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_request_extractor! {
    <crate::adapters::binance::binance_structs::BinanceState, crate::adapters::binance::binance_structs::BinanceCredentials>,
    single::SignedOrderMessage: single::SignedOrderExtractor,
    oco::SignedOrderMessage: oco::SignedOrderExtractor,
    oto::SignedOrderMessage: oto::SignedOrderExtractor,
    otoco::SignedOrderMessage: otoco::SignedOrderExtractor,
}
