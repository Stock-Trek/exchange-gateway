use crate::{connector::Connector, error::EGResult, transports::transport::TransportTrait};

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
