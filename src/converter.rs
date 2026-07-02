use crate::{
    error::EGResult,
    functions::{TryConvertRequestTo, TryConvertResponseFrom},
};

pub struct Converter<TReq, TReqOut, TResIn, TRes> {
    pub(crate) convert_req: TryConvertRequestTo<TReq, TReqOut>,
    pub(crate) convert_res: TryConvertResponseFrom<TResIn, TRes>,
}

impl<TReq, TReqOut, TResIn, TRes> Converter<TReq, TReqOut, TResIn, TRes> {
    pub fn convert_req(&self, req: &TReq) -> EGResult<TReqOut> {
        (self.convert_req)(req)
    }
    pub fn convert_res(&self, res: TResIn) -> EGResult<TRes> {
        (self.convert_res)(res)
    }
}
