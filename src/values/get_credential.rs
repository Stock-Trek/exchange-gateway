use crate::credentials::credential::Credential;
use stock_trek::error::result::StockTrekResult;

pub type GetCredential<TCredentials> =
    fn(credentials: TCredentials) -> StockTrekResult<dyn Credential>;
