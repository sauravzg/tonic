use bytes::Buf;

use super::{ByteStreamInterceptor, ByteStreamInterceptorFactory, Interceptor, InterceptorFactory};
use crate::server::call::{
    HandlerCallOptions, Incoming, Outgoing, StreamingRequest, StreamingResponseWriter,
};
use crate::server::message::AsMut;
use crate::server::method_handler::GenericByteStreamMethodHandler;
use crate::server::stream::PushStreamProducer;
use crate::Status;

use crate::server::call::Lazy;
use crate::server::method_handler::MessageStreamHandler;

/// An interceptor that does nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopInterceptor;

impl Interceptor for NoopInterceptor {
    async fn intercept<H, Req, Resp, P, W, L>(
        &self,
        handler: &H,
        options: HandlerCallOptions,
        req: StreamingRequest<L, P>,
        writer: W,
    ) -> Result<(), Status>
    where
        Req: Send + AsMut,
        Resp: Send + AsMut,
        H: MessageStreamHandler<Req, Resp> + Sync,
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<Outgoing<H::ResponseHolder>> + Send,
        L: Lazy<Req>,
    {
        handler.call(options, req, writer).await
    }
}

impl InterceptorFactory for NoopInterceptor {
    type Interceptor = NoopInterceptor;

    fn create(&self) -> Self::Interceptor {
        NoopInterceptor
    }
}

impl ByteStreamInterceptorFactory for NoopInterceptor {
    type Interceptor = NoopInterceptor;

    fn create(&self) -> Self::Interceptor {
        NoopInterceptor
    }
}

impl ByteStreamInterceptor for NoopInterceptor {
    async fn intercept<H, ReqB, P>(
        &self,
        handler: &H,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: impl StreamingResponseWriter<H::RespB>,
    ) -> Result<(), Status>
    where
        H: GenericByteStreamMethodHandler,
        ReqB: Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
    {
        handler.call(options, req, resp).await
    }
}
