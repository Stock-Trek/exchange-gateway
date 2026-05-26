#[cfg(test)]
mod test {
    use crate::{
        credentials::api_key_credential::ApiKeyCredentials,
        destroy::Destroy,
        transport::{http_transport::HttpTransportTrait, transport::Transport},
    };
    use async_trait::async_trait;
    use chrono::Duration;
    use std::{collections::HashMap, fmt::Display};
    use stock_trek::error::result::StockTrekResult;

    #[test]
    pub fn test() {
        // TODO
        // let protocol = AuthSpecBuilder::<
        //     MyState,
        //     MyCredentials,
        //     MyTransports,
        //     MyHttpTransport,
        //     Req,
        //     Res,
        // >::new(|t| &t.http, Duration::seconds(30))
        // .begin_authenticate_leg(|t| &t.http, Duration::seconds(20))
        // .gather_value(
        //     |state, _credentials| state.abc,
        //     |message, value| {
        //         message
        //             .headers
        //             .insert("HEADER".to_string(), value.to_string());
        //     },
        // )
        // .store_value(
        //     |reply| Ok(reply.body.clone()),
        //     |state, value| state.abc = value.len() as i64,
        // )
        // .build_leg()
        // .build_spec();
        // let credentials = MyCredentials {
        //     api_key: ApiKeyCredentials::new("fdnskfndjks".to_string(), Vec::new()),
        // };
        // let transports = MyTransports::new(MyHttpTransport);
        // let mut session = Session::new();
        // block_on(protocol.authenticate(&credentials, &transports, &mut session))
        //     .expect("Failed to authenticate");
        // let order_request = OrderRequest::Single(SingleOrderGeneric {
        //     activation: OrderActivation::Immediate,
        //     base: AssetId::bitcoin_native(),
        //     constraints: vec![],
        //     intent: OrderIntent::Open,
        //     pricing: OrderPricing::Market,
        //     quantity: OrderQuantity::OfBase(Decimal::ONE),
        //     quote: AssetId::ethereum_usdt(),
        //     side: OrderSide::Buy,
        // });
        // let response = block_on(protocol.send_order_request(
        //     &credentials,
        //     &transports,
        //     &session,
        //     order_request,
        // ))
        // .expect("Failed to sign message");
        // println!("{:?}", response);
    }

    // TODO
    // #[allow(dead_code)]
    // struct MyListener;
    // impl ExchangeListenerTrait for MyListener {
    //     fn on_order_placed(&self, _order_response: OrderResponse) {}
    // }

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
    impl HttpTransportTrait<Req, Res> for MyHttpTransport {}

    struct Req {
        headers: HashMap<String, String>,
    }
    struct Res {
        body: String,
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

    impl MyTransports {
        pub fn new(http: MyHttpTransport) -> Self {
            Self { http }
        }
    }

    impl Default for Req {
        fn default() -> Self {
            Self {
                headers: HashMap::new(),
            }
        }
    }

    #[async_trait]
    impl Transport<Req, Res> for MyHttpTransport {
        fn new(_url: String) -> Self {
            Self {}
        }
        async fn send_and_wait_for_reply(
            &self,
            _message: &Req,
            _timeout: Duration,
        ) -> StockTrekResult<Res> {
            Ok(Res {
                body: "fdsfds".to_string(),
            })
        }
    }
    impl Display for Res {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Response")
        }
    }
}
