use crate::{
    error::EGResult,
    functions::{TryConvertRequestTo, TryConvertResponseFrom},
};

pub struct Converter<TReq, TReqOut, TResIn, TRes> {
    pub(crate) convert_request: TryConvertRequestTo<TReq, TReqOut>,
    pub(crate) convert_response: TryConvertResponseFrom<TResIn, TRes>,
}

impl<TReq, TReqOut, TResIn, TRes> Converter<TReq, TReqOut, TResIn, TRes> {
    pub fn convert_req(&self, req: &TReq) -> EGResult<TReqOut> {
        (self.convert_request)(req)
    }
    pub fn convert_res(&self, res: TResIn) -> EGResult<TRes> {
        (self.convert_response)(res)
    }
}
