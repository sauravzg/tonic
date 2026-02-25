use super::{ByteStreamInterceptor, Interceptor};
use crate::server::call::Lazy;
use crate::server::call::{
    HandlerCallOptions, Incoming, Outgoing, StreamingRequest, StreamingResponseWriter,
};
use crate::server::message::AsMut;
use crate::server::method_handler::{GenericByteStreamMethodHandler, MessageStreamHandler};
use crate::server::stream::PushStreamProducer;
use crate::Status;

/// A method handler wrapped with an interceptor.
pub struct InterceptedMethodHandler<I, H> {
    pub interceptor: I,
    pub inner: H,
}

impl<I, H, Req, Resp> MessageStreamHandler<Req, Resp> for InterceptedMethodHandler<I, H>
where
    I: Interceptor,
    H: MessageStreamHandler<Req, Resp> + Sync,
    Req: Send + AsMut,
    Resp: Send + AsMut,
{
    type ResponseHolder = H::ResponseHolder;

    async fn call<P, W, L>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<L, P>,
        writer: W,
    ) -> Result<(), Status>
    where
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<Outgoing<Self::ResponseHolder>> + Send,
        L: Lazy<Req>,
    {
        self.interceptor
            .intercept(&self.inner, options, req, writer)
            .await
    }
}

// Helper struct for chaining with references
pub(crate) struct InterceptedMethodHandlerRef<'a, I, H> {
    pub(crate) interceptor: &'a I,
    pub(crate) inner: &'a H,
}

impl<'a, I, H, Req, Resp> MessageStreamHandler<Req, Resp> for InterceptedMethodHandlerRef<'a, I, H>
where
    I: Interceptor,
    H: MessageStreamHandler<Req, Resp> + Sync,
    Req: Send + AsMut,
    Resp: Send + AsMut,
{
    type ResponseHolder = H::ResponseHolder;

    async fn call<P, W, L>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<L, P>,
        writer: W,
    ) -> Result<(), Status>
    where
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<Outgoing<Self::ResponseHolder>> + Send,
        L: Lazy<Req>,
    {
        self.interceptor
            .intercept(self.inner, options, req, writer)
            .await
    }
}

/// A byte stream method handler wrapped with an interceptor.
pub struct InterceptedByteStreamHandler<I, H> {
    pub interceptor: I,
    pub inner: H,
}

impl<H, I> GenericByteStreamMethodHandler for InterceptedByteStreamHandler<I, H>
where
    H: GenericByteStreamMethodHandler,
    I: ByteStreamInterceptor,
{
    type RespB = H::RespB;

    async fn call<ReqB, P>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: impl StreamingResponseWriter<Self::RespB>,
    ) -> Result<(), Status>
    where
        ReqB: bytes::Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
    {
        self.interceptor
            .intercept(&self.inner, options, req, resp)
            .await
    }
}

// Helper struct for chaining with references
pub(crate) struct InterceptedByteStreamHandlerRef<'a, I, H> {
    pub(crate) interceptor: &'a I,
    pub(crate) inner: &'a H,
}

impl<'a, H, I> GenericByteStreamMethodHandler for InterceptedByteStreamHandlerRef<'a, I, H>
where
    H: GenericByteStreamMethodHandler,
    I: ByteStreamInterceptor,
{
    type RespB = H::RespB;

    async fn call<ReqB, P>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: impl StreamingResponseWriter<Self::RespB>,
    ) -> Result<(), Status>
    where
        ReqB: bytes::Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
    {
        self.interceptor
            .intercept(self.inner, options, req, resp)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::{HandlerCallOptions, Incoming, StreamingRequest, StreamingResponseWriter};
    use crate::server::stream::{PushStreamProducer, PushStreamWriter};
    use crate::Status;
    use std::sync::{Arc, Mutex};

    struct MockByteStreamHandler;
    impl GenericByteStreamMethodHandler for MockByteStreamHandler {
        type RespB = bytes::Bytes;
        async fn call<ReqB, P>(
            &self,
            _options: HandlerCallOptions,
            _req: StreamingRequest<Incoming<ReqB>, P>,
            _resp: impl StreamingResponseWriter<Self::RespB>,
        ) -> Result<(), Status>
        where
            ReqB: bytes::Buf + Send,
            P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
        {
            Ok(())
        }
    }

    struct MockByteStreamInterceptor {
        called: Arc<Mutex<bool>>,
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
            ReqB: bytes::Buf + Send,
            P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
        {
            *self.called.lock().unwrap() = true;
            handler.call(options, req, resp).await
        }
    }

    #[tokio::test]
    async fn test_byte_stream_interceptor() {
        let called = Arc::new(Mutex::new(false));
        let interceptor = MockByteStreamInterceptor {
            called: called.clone(),
        };
        let handler = MockByteStreamHandler;
        let intercepted = InterceptedByteStreamHandler {
            interceptor,
            inner: handler,
        };

        // We need a RawMessage to pass to call
        // But RawMessage constructor might be private or we can construct it if it's pub
        // RawMessage is in crate::server::call::raw_message
        // It has new(b: B)
        // We need to import RawMessage in tests? It is imported in super.

        // We need a StreamingResponseWriter mock.
        // We can use a simple struct that implements it.
        struct MockStreamingWriter;
        impl StreamingResponseWriter<bytes::Bytes> for MockStreamingWriter {
            type BodyWriter = MockBodyWriter;

            async fn send_initial_metadata(
                self,
                _metadata: crate::server::call::Metadata,
            ) -> Result<Self::BodyWriter, Status> {
                Ok(MockBodyWriter)
            }
        }

        struct MockBodyWriter;
        impl crate::server::call::StreamingResponseBodyWriter<bytes::Bytes> for MockBodyWriter {
            async fn write(&mut self, _message: bytes::Bytes) -> Result<(), Status> {
                Ok(())
            }
            async fn send_trailing_metadata(
                self,
                _metadata: crate::server::call::Metadata,
            ) -> Result<(), Status> {
                Ok(())
            }
        }

        struct MockProducer;
        impl PushStreamProducer for MockProducer {
            type Item = Incoming<Box<dyn bytes::Buf + Send>>;
            async fn produce(
                self,
                _writer: PushStreamWriter<
                    Self::Item,
                    impl crate::server::stream::PushStreamConsumer<Item = Self::Item>,
                >,
            ) -> Result<(), Status> {
                Ok(())
            }
        }

        // Construct a dummy request
        let stream = crate::server::stream::PushStream::new(MockProducer);
        let req = StreamingRequest::new(stream, crate::server::call::Metadata::default());

        intercepted
            .call(HandlerCallOptions::default(), req, MockStreamingWriter)
            .await
            .unwrap();

        assert!(*called.lock().unwrap());
    }
    #[tokio::test]
    async fn test_interceptor_v2_call() {
        use crate::server::call::{HandlerCallOptions, Metadata, StreamingRequest};
        use crate::server::interceptor::test_utils::*;
        use crate::server::stream::PushStream;
        use std::sync::atomic::Ordering;
        use std::sync::Arc;

        let handler = MockHandler::new();
        let order = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let interceptor = MockInterceptor::new(order.clone());

        let intercepted_handler = InterceptedMethodHandler {
            inner: handler.clone(),
            interceptor: interceptor.clone(),
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

        assert_eq!(interceptor.call_count.load(Ordering::SeqCst), 1);
        assert_eq!(handler.call_count.load(Ordering::SeqCst), 1);
    }
}
