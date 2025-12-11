use crate::server::call::{HandlerCallOptions, Incoming, StreamingRequest, StreamingResponseWriter};
use crate::server::stream::PushStreamProducer;
use crate::Status;
use bytes::Buf;

/// A method handler that processes raw bytes.
#[trait_variant::make(Send)]
pub trait GenericByteStreamMethodHandler: Send + Sync {
    type RespB: Buf + Send;

    async fn call<ReqB, P>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: impl StreamingResponseWriter<Self::RespB>,
    ) -> Result<(), Status>
    where
        ReqB: Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send;
}
