use crate::transport::transport::Transport;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub type GetTransport<TTransports, TTransport, TMessage, TReply>
where
    TTransport: Transport<TMessage, TReply>,
= (
    fn(transports: &TTransports) -> StockTrekResult<TTransport>,
    PhantomData<(TMessage, TReply)>,
);
