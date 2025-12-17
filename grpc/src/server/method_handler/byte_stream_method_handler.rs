use crate::server::call::{HandlerCallOptions, Incoming, StreamingRequest, StreamingResponseWriter};
use crate::server::method_handler::GenericByteStreamMethodHandler;
use crate::server::stream::PushStreamProducer;
use crate::Status;
use bytes::Buf;

/// A method handler that processes raw bytes.
#[trait_variant::make(Send)]
#[dynosaur::dynosaur(pub DynByteStreamMethodHandler = dyn(box) ByteStreamMethodHandler)]
pub trait ByteStreamMethodHandler<ReqB, W, P>: Send + Sync {
    type RespB: Buf + Send;

    async fn call(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: W,
    ) -> Result<(), Status>
    where
        ReqB: Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
        W: StreamingResponseWriter<Self::RespB>;
}

/// Adapter to bridge `GenericByteStreamMethodHandler` to `ByteStreamMethodHandler`.
pub struct GenericByteStreamAdapter<T>(pub T);

impl<T, ReqB, W, P> ByteStreamMethodHandler<ReqB, W, P> for GenericByteStreamAdapter<T>
where
    T: GenericByteStreamMethodHandler,
    W: StreamingResponseWriter<T::RespB> + Send,
{
    type RespB = T::RespB;

    async fn call(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: W,
    ) -> Result<(), Status>
    where
        ReqB: Buf + Send,
        W: StreamingResponseWriter<Self::RespB>,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
    {
        <T as GenericByteStreamMethodHandler>::call(&self.0, options, req, resp).await
    }
}
