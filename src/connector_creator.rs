use crate::{
    connector::Connector, error::EGResult, listeners::listener::Listener, urls::ExchangeNetType,
};

pub(crate) trait ConnectorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
> where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    fn into_connector(
        self,
        exchange_net_type: ExchangeNetType,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            TUnsignedMessageToExchange,
            TCredentials,
            TMessageToExchange,
            TMessageFromExchange,
            TResponse,
        >,
    >;
}
