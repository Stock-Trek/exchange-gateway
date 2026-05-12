#[cfg(test)]
mod test {
    use crate::{
        auth_spec::AuthSpec,
        authenticator::Authenticator,
        credentials::api_key_credential::ApiKeyCredentials,
        destroy::Destroy,
        exchange_client::ExchangeClient,
        transport::{
            http_transport::{HttpPart, HttpTransport},
            transport::Transport,
        },
        values::{get::get_uuid::GetUuid, set::set_state_value::SetStateValue},
    };
    use stock_trek::error::result::StockTrekResult;

    pub fn test() {
        let client = MyClient {};
        let auth_spec = AuthSpec::<Creds, Rest, State>::new(
            vec![MoveValue(|c, t, s| GetUuid, |c, t, s| SetStateValue)],
            vec![],
        );
        let credentials = Creds {
            api_key: ApiKeyCredentials::new("dsffsd".to_string(), "fdsfds".as_bytes().to_vec()),
        };
        let transport = Rest {};
        let authenticator = Authenticator::new(client, auth_spec, credentials, transport);
        authenticator.start();
    }

    struct MyClient {}

    impl ExchangeClient for MyClient {
        fn on_order_accepted(&self, order: Order) {}
        fn send_order_request(
            &self,
            order: stock_trek::prelude::OrderRequest<AssetId, rust_decimal::prelude::Decimal>,
        ) {
        }
    }
    struct Creds {
        api_key: ApiKeyCredentials,
    }

    struct State {}

    struct Rest {}

    struct Req {}
    struct Res {}

    impl Destroy for Creds {
        fn destroy(&mut self) {
            self.api_key.destroy();
        }
    }

    impl Default for State {
        fn default() -> Self {
            Self {}
        }
    }

    impl HttpTransport<Req, Res> for Rest {
        async fn send_message(&self, message: Req) -> StockTrekResult<Res> {
            Ok(Res {})
        }
    }

    impl Transport<HttpPart, Req, Res> for Rest {
        fn new(url: String) -> Self {
            Self {}
        }
        fn new_message(&self) -> StockTrekResult<Req> {
            Ok(Req {})
        }
    }
}
