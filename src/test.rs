#[cfg(test)]
mod test {
    use crate::{
        auth_spec_builder::AuthSpecBuilder,
        authenticator::Authenticator,
        credentials::api_key_credential::ApiKeyCredentials,
        destroy::Destroy,
        exchange_listener::ExchangeListener,
        transport::{http_transport::HttpTransport, transport::Transport},
    };
    use async_trait::async_trait;
    use chrono::Duration;
    use std::collections::HashMap;
    use stock_trek::{error::result::StockTrekResult, order::order_response::OrderResponse};

    #[allow(dead_code)]
    pub async fn test() {
        let auth_spec = AuthSpecBuilder::<MyState, MyCredentials, MyTransports>::new()
            .begin_leg(|t| &t.http, Duration::seconds(20))
            .gather_value(
                |state, _credentials| state.abc,
                |message, value| {
                    message
                        .headers
                        .insert("HEADER".to_string(), value.to_string());
                },
            )
            .store_value(
                |reply| Ok(reply.body.clone()),
                |state, value| state.abc = value.len() as i64,
            )
            .build_leg()
            .build_spec();
        let credentials = MyCredentials {
            api_key: ApiKeyCredentials::new("fdnskfndjks".to_string(), Vec::new()),
        };
        let transports = MyTransports::new(MyHttpTransport);
        let listener = MyListener;
        let authenticator = Authenticator::new(auth_spec, listener, credentials, transports);
        if let Ok(_) = authenticator.start().await {
            // TODO
        }
    }

    struct MyListener;
    impl ExchangeListener for MyListener {
        fn on_order_placed(&self, _order_response: OrderResponse) {}
    }

    #[allow(dead_code)]
    struct MyCredentials {
        pub api_key: ApiKeyCredentials,
    }

    #[allow(dead_code)]
    struct MyState {
        abc: i64,
    }

    struct MyTransports {
        #[allow(dead_code)]
        pub http: MyHttpTransport,
    }

    struct MyHttpTransport;
    impl HttpTransport<Req, Res> for MyHttpTransport {}

    struct Req {
        #[allow(dead_code)]
        headers: HashMap<String, String>,
    }
    struct Res {
        #[allow(dead_code)]
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
        #[allow(dead_code)]
        pub fn new(http: MyHttpTransport) -> Self {
            Self { http }
        }
    }

    #[async_trait]
    impl Transport for MyHttpTransport {
        type Message = Req;
        type Reply = Res;

        fn new(_url: String) -> Self {
            Self {}
        }
        fn new_message(&self) -> StockTrekResult<Self::Message> {
            Ok(Self::Message {
                headers: HashMap::new(),
            })
        }
        async fn send_and_wait_for_reply(
            &self,
            _message: Self::Message,
            _timeout: Duration,
        ) -> StockTrekResult<Self::Reply> {
            Ok(Self::Reply {
                body: "fdsfds".to_string(),
            })
        }
    }
}
