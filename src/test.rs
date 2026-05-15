#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        auth_spec_builder::AuthSpecBuilder,
        credentials::api_key_credential::ApiKeyCredentials,
        destroy::Destroy,
        transport::{
            http_transport::{HttpPart, HttpTransport},
            transport::Transport,
        },
    };
    use async_trait::async_trait;
    use stock_trek::error::result::StockTrekResult;

    pub async fn test() {
        let auth_spec = AuthSpecBuilder::<MyState, MyCredentials, MyTransports>::new()
            .begin_leg::<MyHttpTransport, HttpPart, Req, Res>(|t| &t.http)
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
        let a = auth_spec.auth(&credentials, &transports).await;
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
    impl HttpTransport<Req, Res> for MyHttpTransport {}

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

    #[async_trait]
    impl Transport<HttpPart, Req, Res> for MyHttpTransport {
        fn new(_url: String) -> Self {
            Self {}
        }
        fn new_message(&self) -> StockTrekResult<Req> {
            Ok(Req {
                headers: HashMap::new(),
            })
        }
        async fn send_and_wait_for_reply(&self, _message: Req) -> StockTrekResult<Res> {
            Ok(Res {
                body: "dsfdsfds".to_string(),
            })
        }
    }
}
