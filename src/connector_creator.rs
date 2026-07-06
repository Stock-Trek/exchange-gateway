use crate::{
    connector::Connector, error::EGResult, listeners::listener::Listener,
    transports::transport::TransportTrait,
};

pub(crate) trait ConnectorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TTransport,
    TMessageFromExchange,
    TResponse,
> where
    TTransport: TransportTrait,
{
    fn into_connector(
        self,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            TUnsignedMessageToExchange,
            TCredentials,
            TMessageToExchange,
            TTransport,
            TMessageFromExchange,
            TResponse,
        >,
    >;
}
