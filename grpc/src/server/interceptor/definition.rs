use crate::server::call::{HandlerCallOptions, Outgoing, StreamingRequest, StreamingResponseWriter};
use crate::server::message::AsMut;
use crate::server::stream::PushStreamProducer;
use crate::Status;

use crate::server::call::Lazy;
use crate::server::method_handler::MessageStreamHandler;

/// A trait for intercepting gRPC calls.
/// A unified trait for intercepting gRPC calls using the MessageStreamHandler API.
#[trait_variant::make(Send)]
pub trait Interceptor: Send + Sync {
    /// Intercepts a streaming call.
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
        L: Lazy<Req>;
}

/// A factory for creating interceptors.
pub trait InterceptorFactory: Send + Sync + 'static {
    /// The interceptor type created by this factory.
    type Interceptor: Interceptor;

    /// Creates a new interceptor.
    fn create(&self) -> Self::Interceptor;
}

use crate::server::call::Incoming;
use crate::server::method_handler::GenericByteStreamMethodHandler;

/// A trait for intercepting raw byte stream calls.
#[trait_variant::make(Send)]
pub trait ByteStreamInterceptor: Send + Sync + 'static {
    /// Intercepts a byte stream call.
    async fn intercept<H, ReqB, P>(
        &self,
        handler: &H,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: impl StreamingResponseWriter<H::RespB>,
    ) -> Result<(), Status>
    where
        H: GenericByteStreamMethodHandler,
        ReqB: bytes::Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send;
}

/// A factory for creating byte stream interceptors.
pub trait ByteStreamInterceptorFactory: Send + Sync + 'static {
    /// The interceptor type created by this factory.
    type Interceptor: ByteStreamInterceptor;

    /// Creates a new byte stream interceptor.
    fn create(&self) -> Self::Interceptor;
}
