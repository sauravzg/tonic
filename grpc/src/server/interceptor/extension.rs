use super::chain::ChainedInterceptor;
use super::{ByteStreamInterceptor, Interceptor};

/// Extension trait for `ByteStreamInterceptor`.
pub trait ByteStreamInterceptorExt: ByteStreamInterceptor {
    /// Chains this interceptor with another interceptor.
    fn chain<B>(self, other: B) -> impl ByteStreamInterceptor
    where
        Self: Sized,
        B: ByteStreamInterceptor,
    {
        ChainedInterceptor::new(self, other)
    }
}

impl<T: ByteStreamInterceptor> ByteStreamInterceptorExt for T {}

/// Extension trait for `Interceptor`.
pub trait InterceptorExt: Interceptor {
    /// Chains this interceptor with another interceptor.
    fn chain<B>(self, other: B) -> impl Interceptor
    where
        Self: Sized,
        B: Interceptor,
    {
        ChainedInterceptor::new(self, other)
    }
}

impl<T: Interceptor> InterceptorExt for T {}

#[cfg(test)]
mod tests {
    use super::{ByteStreamInterceptor, ByteStreamInterceptorExt, InterceptorExt};
    use crate::server::call::{HandlerCallOptions, Incoming, StreamingRequest, StreamingResponseWriter};

    use crate::server::method_handler::GenericByteStreamMethodHandler;
    use crate::server::stream::PushStreamProducer;
    use crate::Status;
    use bytes::Buf;

    struct MockInterceptor;

    impl crate::server::interceptor::Interceptor for MockInterceptor {
        async fn intercept<H, Req, Resp, P, W, L>(
            &self,
            handler: &H,
            options: HandlerCallOptions,
            req: StreamingRequest<L, P>,
            writer: W,
        ) -> Result<(), Status>
        where
            Req: Send + crate::server::message::AsMut,
            Resp: Send + crate::server::message::AsMut,
            H: crate::server::method_handler::MessageStreamHandler<Req, Resp> + Sync,
            P: PushStreamProducer<Item = L> + Send,
            W: StreamingResponseWriter<crate::server::call::Outgoing<H::ResponseHolder>> + Send,
            L: crate::server::call::Lazy<Req>,
        {
            handler.call(options, req, writer).await
        }
    }

    #[test]
    fn test_chain_compiles() {
        let a = MockInterceptor;
        let b = MockInterceptor;
        let _chained = a.chain(b);
    }

    struct MockByteStreamInterceptor;

    impl ByteStreamInterceptor for MockByteStreamInterceptor {
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

    #[test]
    fn test_byte_stream_chain_compiles() {
        let a = MockByteStreamInterceptor;
        let b = MockByteStreamInterceptor;
        let _chained = a.chain(b);
    }
}
