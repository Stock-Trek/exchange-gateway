use stock_trek::error::result::StockTrekResult;

pub type StoreAuthValue<TReply, TState> = Box<dyn StoreAuthValueTrait<TReply, TState>>;

pub trait StoreAuthValueTrait<TReply, TState>: Send + Sync
where
    TReply: 'static,
    TState: 'static,
{
    fn store_auth_value(&self, reply: &TReply, state: &mut TState) -> StockTrekResult<()>;
}

pub struct StoreAuthValueImpl<TValue, TReply, TState>
where
    TValue: 'static,
    TReply: 'static,
    TState: 'static,
{
    unpack_value: fn(reply: &TReply) -> StockTrekResult<TValue>,
    set_value: fn(state: &mut TState, value: &TValue),
}

impl<TValue, TReply, TState> StoreAuthValueImpl<TValue, TReply, TState>
where
    TValue: 'static,
    TReply: 'static,
    TState: 'static,
{
    pub fn new(
        unpack_value: fn(reply: &TReply) -> StockTrekResult<TValue>,
        set_value: fn(state: &mut TState, value: &TValue),
    ) -> StoreAuthValue<TReply, TState> {
        Box::new(Self {
            unpack_value,
            set_value,
        })
    }
}

impl<TValue, TReply, TState> StoreAuthValueTrait<TReply, TState>
    for StoreAuthValueImpl<TValue, TReply, TState>
{
    fn store_auth_value(&self, reply: &TReply, state: &mut TState) -> StockTrekResult<()> {
        let value = (self.unpack_value)(reply)?;
        (self.set_value)(state, &value);
        Ok(())
    }
}
