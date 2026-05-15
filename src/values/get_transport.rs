use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub type GetTransport<TTransports, TTransport, TMessage, TReply> = (
    fn(transports: &TTransports) -> StockTrekResult<TTransport>,
    PhantomData<(TMessage, TReply)>,
);
