use crate::transport::transport::Transport;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub type GetTransport<TTransports, TTransport, TMessagePart, TMessage, TReply>
where
    TTransport: Transport<TMessagePart, TMessage, TReply>,
= (
    fn(transports: &TTransports) -> StockTrekResult<TTransport>,
    PhantomData<(TMessagePart, TMessage, TReply)>,
);
