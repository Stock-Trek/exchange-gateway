use crate::{connector::Connector, error::EGResult, listeners::listener::Listener};

pub(crate) trait ConnectorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
>
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
            TMessageFromExchange,
            TResponse,
        >,
    >;
}
