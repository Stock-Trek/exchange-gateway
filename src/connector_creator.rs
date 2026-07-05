use crate::{connector::Connector, error::EGResult};

pub(crate) trait ConnectorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TMessageDto,
    TMessageFromExchange,
    TResponse,
>
{
    fn into_connector(
        self,
    ) -> EGResult<
        Connector<
            TRequest,
            TUnsignedMessageToExchange,
            TCredentials,
            TMessageToExchange,
            TResponse,
        >,
    >;
}
