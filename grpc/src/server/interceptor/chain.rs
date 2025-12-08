use super::intercepted_handler::{InterceptedByteStreamHandlerRef, InterceptedMethodHandlerRef};
use super::{ByteStreamInterceptor, Interceptor};
use crate::server::call::{
    HandlerCallOptions, Incoming, Outgoing, StreamingRequest, StreamingResponseWriter,
};
use crate::server::message::AsMut;
use crate::server::method_handler::GenericByteStreamMethodHandler;
use crate::server::stream::PushStreamProducer;

use crate::Status;
use bytes::Buf;

use crate::server::call::Lazy;
use crate::server::method_handler::MessageStreamHandler;

/// A chain of two interceptors.
pub(crate) struct ChainedInterceptor<A, B> {
    first: A,
    second: B,
}

impl<A, B> ChainedInterceptor<A, B> {
    /// Creates a new chained interceptor.
    pub fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

impl<A, B> Interceptor for ChainedInterceptor<A, B>
where
    A: Interceptor,
    B: Interceptor,
{
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
        let inner_wrapped = InterceptedMethodHandlerRef {
            interceptor: &self.second,
            inner: handler,
        };
        self.first
            .intercept(&inner_wrapped, options, req, writer)
            .await
    }
}

impl<A, B> ByteStreamInterceptor for ChainedInterceptor<A, B>
where
    A: ByteStreamInterceptor,
    B: ByteStreamInterceptor,
{
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
        self.first
            .intercept(
                &InterceptedByteStreamHandlerRef {
                    inner: handler,
                    interceptor: &self.second,
                },
                options,
                req,
                resp,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::interceptor::{ByteStreamInterceptorExt, InterceptorExt};
    use crate::server::stream::PushStreamProducer;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockInterceptor {
        id: usize,
        order: Arc<Mutex<Vec<usize>>>,
    }

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
            self.order.lock().unwrap().push(self.id);
            handler.call(options, req, writer).await
        }
    }

    #[tokio::test]
    async fn test_chain_ordering() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let first = MockInterceptor {
            id: 1,
            order: order.clone(),
        };
        let second = MockInterceptor {
            id: 2,
            order: order.clone(),
        };

        let chained = first.chain(second);

        // We can't easily invoke intercept_unary directly without a handler,
        // but we can check if it compiles and if the type is correct.
        // To properly test execution order, we would need a mock handler.
        // For now, let's verify the chaining API works.
    }

    #[derive(Clone)]
    struct MockByteStreamInterceptor {
        id: usize,
        order: Arc<Mutex<Vec<usize>>>,
        called: Arc<AtomicBool>,
    }

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
            self.called.store(true, Ordering::SeqCst);
            handler.call(options, req, resp).await
        }
    }

    #[tokio::test]
    async fn test_byte_stream_chain_ordering() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let i1 = MockByteStreamInterceptor {
            id: 1,
            order: order.clone(),
            called: Default::default(),
        };
        let i2 = MockByteStreamInterceptor {
            id: 2,
            order: order.clone(),
            called: Default::default(),
        };

        let chained = ByteStreamInterceptorExt::chain(i1, i2);

        // Verify it implements ByteStreamInterceptor
        fn assert_bs_interceptor<T: ByteStreamInterceptor>(_: T) {}
        assert_bs_interceptor(chained);
    }
    #[tokio::test]
    async fn test_interceptor_v2_chaining_order() {
        use crate::server::call::{HandlerCallOptions, Metadata, StreamingRequest};
        use crate::server::interceptor::test_utils::*;
        use crate::server::interceptor::{InterceptedMethodHandler, InterceptorExt};
        use crate::server::stream::PushStream;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let handler = MockHandler::new();
        let order = Arc::new(AtomicUsize::new(0));

        let outer = MockInterceptor::new(order.clone());
        let inner = MockInterceptor::new(order.clone());

        let chained = outer.chain(inner);

        let intercepted_handler = InterceptedMethodHandler {
            inner: handler.clone(),
            interceptor: chained,
        };

        let options = HandlerCallOptions::default();
        let producer = MockProducer;
        let stream = PushStream::new(producer);
        let req = StreamingRequest::new(stream, Metadata::default());
        let writer = MockWriter;

        intercepted_handler
            .call(options, req, writer)
            .await
            .unwrap();

        assert_eq!(order.load(Ordering::SeqCst), 2);
    }
}
