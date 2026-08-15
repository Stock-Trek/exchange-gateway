use crate::{
    connector::Connector, error::EGResult, listeners::listener::Listener, urls::TradingMode,
};

pub(crate) trait ConnectorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TTransportBody,
    TMessageFromExchange,
    TResponse,
> where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    fn into_connector(
        self,
        trading_mode: TradingMode,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            TUnsignedMessageToExchange,
            TCredentials,
            TMessageToExchange,
            TTransportBody,
            TMessageFromExchange,
            TResponse,
        >,
    >;
}
