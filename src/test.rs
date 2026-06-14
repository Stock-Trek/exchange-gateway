#[cfg(test)]
mod test {
    use crate::{
        authenticate_leg::AuthenticateLegImpl,
        cex::{
            cex_spec::CexSpec,
            increment_sizes::IncrementSizesBuilder,
            rate_limits_weights::{RateLimits, RequestWeights},
        },
        credentials::api_key_credential::ApiKeyCredentials,
        exchange_connector::ExchangeConnector,
        message_leg::MessageLegImpl,
        rate_limit::multi_rate_limiter::MultiRateLimiter,
        transports::{
            http_transport::{HttpMessageDto, HttpTransportTrait},
            transport::TransportTrait,
        },
    };
    use async_trait::async_trait;
    use chrono::Duration;
    use rust_decimal::Decimal;
    use secrecy::SecretString;
    use std::collections::HashMap;
    use stock_trek::{
        cex::{
            asset_id::AssetId,
            capability::{CexCapability, MultiLegCexCapability, QuoteQuantityCexCapability},
            order_id::OrderId,
            order_request::OrderRequest,
            order_response::OrderResponse,
        },
        error::result::StockTrekResult,
    };

    #[test]
    pub fn test() {
        let capabilities = vec![
            CexCapability::QuoteQuantity(QuoteQuantityCexCapability::AllowLimitPricing),
            CexCapability::QuoteQuantity(QuoteQuantityCexCapability::AllowTriggeredTiming),
            CexCapability::MultiLeg(MultiLegCexCapability::OneCancelsOther),
            CexCapability::MultiLeg(MultiLegCexCapability::OneTriggersOther),
            CexCapability::MultiLeg(MultiLegCexCapability::OneTriggersOco),
        ];
        let increments = IncrementSizesBuilder::new()
            .with(
                AssetId::bitcoin(),
                AssetId::usdc(),
                Decimal::from_i128_with_scale(1, 3),
                Decimal::from_i128_with_scale(1, 3),
            )
            .build();
        let rate_limits = RateLimits {
            send_order_request: MultiRateLimiter::new(vec![]),
        };
        let request_weights = RequestWeights {
            send_order_request: 1,
        };
        let mut tickers = HashMap::new();
        tickers.insert(AssetId::usdc(), "USDC".to_string());
        tickers.insert(AssetId::bitcoin(), "BTC".to_string());
        let spec = CexSpec::<MyTransports, ApiKeyCredentials, MyState>::new(
            capabilities,
            increments,
            rate_limits,
            request_weights,
            vec![AuthenticateLegImpl::<
                MyTransports,
                ApiKeyCredentials,
                MyState,
                MyHttpTransport,
            >::new(
                |t| &t.http,
                Duration::seconds(20),
                |_t, _c, _s| HttpMessageDto {
                    headers: HashMap::new(),
                    body_json: "{}".to_string(),
                },
                |_m, _s| Ok(MyState { _abc: 123 }),
            )],
            MessageLegImpl::new(
                |t| &t.http,
                Duration::seconds(20),
                |_c, _s, order_request| {
                    let body = match order_request {
                        OrderRequest::Single(_single) => "",
                        OrderRequest::OneCancelsOther(_oco) => "",
                        OrderRequest::OneTriggersOther(_oto) => "",
                        OrderRequest::OneTriggersOco(_otoco) => "",
                    };
                    Ok(HttpMessageDto {
                        headers: HashMap::new(),
                        body_json: body.to_string(),
                    })
                },
                |_r| {
                    Ok(OrderResponse {
                        id: OrderId("".to_string()),
                    })
                },
            ),
        );

        let transports = MyTransports {
            http: MyHttpTransport {},
        };
        let credentials = ApiKeyCredentials::new(
            SecretString::from("my-api-key"),
            SecretString::from("my-secret"),
        );
        let unauthenticated_exchange_connector: ExchangeConnector<
            CexSpec<MyTransports, ApiKeyCredentials, MyState>,
            _,
        > = ExchangeConnector::new(Box::new(spec), transports, credentials);
        let _authenticated_exchange_connector = unauthenticated_exchange_connector.authenticate();
    }

    struct MyState {
        _abc: i64,
    }

    struct MyTransports {
        pub http: MyHttpTransport,
    }

    struct MyHttpTransport;
    impl HttpTransportTrait for MyHttpTransport {}
    #[async_trait]
    impl TransportTrait for MyHttpTransport {
        type TransportMessage = HttpMessageDto;
        type MessageDto = HttpMessageDto;
        async fn send(
            &self,
            _message: HttpMessageDto,
            _timeout: Duration,
        ) -> StockTrekResult<HttpMessageDto> {
            Ok(HttpMessageDto {
                headers: HashMap::new(),
                body_json: "fdsfds".to_string(),
            })
        }
    }

    impl Default for MyState {
        fn default() -> Self {
            Self { _abc: 123 }
        }
    }
}
