#[cfg(test)]
mod test {
    use crate::{
        adapt::{adapter::Adapter, increment_sizes::IncrementSizesBuilder},
        authenticate_leg::AuthenticateLegImpl,
        credentials::api_key_credential::ApiKeyCredentials,
        destroy::Destroy,
        exchange_connector::ExchangeConnectorImpl,
        exchange_protocol::ExchangeProtocol,
        message_leg::MessageLegImpl,
        transport::{
            http_transport::{HttpMessageDto, HttpTransportTrait},
            transport::TransportTrait,
        },
    };
    use async_trait::async_trait;
    use chrono::Duration;
    use rust_decimal::Decimal;
    use std::collections::HashMap;
    use stock_trek::{
        asset_id::AssetId,
        capability::{Capability, MultiLegCapability, QuoteQuantityCapability},
        error::result::StockTrekResult,
        exchange_id::ExchangeId,
        order::{order_id::OrderId, order_request::OrderRequest, order_response::OrderResponse},
    };

    #[test]
    pub fn test() {
        let protocol = ExchangeProtocol::<MyTransports, MyCredentials, MyState>::new(
            vec![AuthenticateLegImpl::new(
                |t| &t.http,
                Duration::seconds(20),
                |_t, _c, _s| HttpMessageDto {
                    headers: HashMap::new(),
                    body_json: "{}".to_string(),
                },
                |_m, s| {
                    s.abc = 123;
                    Ok(())
                },
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
        let id = ExchangeId("Binance".to_string());
        let capabilities = vec![
            Capability::QuoteQuantity(QuoteQuantityCapability::AllowLimitPricing),
            Capability::QuoteQuantity(QuoteQuantityCapability::AllowTriggeredTiming),
            Capability::MultiLeg(MultiLegCapability::OneCancelsOther),
            Capability::MultiLeg(MultiLegCapability::OneTriggersOther),
            Capability::MultiLeg(MultiLegCapability::OneTriggersOco),
        ];
        let increments = IncrementSizesBuilder::new()
            .with(
                AssetId::bitcoin_native(),
                AssetId::base_usdc(),
                Decimal::from_i128_with_scale(1, 3),
                Decimal::from_i128_with_scale(1, 3),
            )
            .build();
        let mut tickers = HashMap::new();
        tickers.insert(AssetId::base_usdc(), "APT".to_string());
        tickers.insert(AssetId::bitcoin_native(), "APT".to_string());
        let transports = MyTransports {
            http: MyHttpTransport {},
        };
        let credentials = MyCredentials {
            api_key: ApiKeyCredentials::new("fdsfdsd".to_string(), Vec::new()),
        };
        let exchange_connector = ExchangeConnectorImpl::new(protocol, transports, credentials);
        let _adapter = Adapter::new(
            id,
            capabilities,
            increments,
            None,
            tickers,
            exchange_connector,
        );
    }

    struct MyCredentials {
        pub api_key: ApiKeyCredentials,
    }

    struct MyState {
        abc: i64,
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

    impl Destroy for MyCredentials {
        fn destroy(&mut self) {
            self.api_key.destroy();
        }
    }

    impl Default for MyState {
        fn default() -> Self {
            Self { abc: 123 }
        }
    }
}
