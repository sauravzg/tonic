use crate::codec::serialization::{Deserialize, Serialize};
use crate::server::call::{
    HandlerCallOptions, Incoming, StreamingRequest, StreamingResponseWriter,
};
use crate::server::interceptor::{
    ByteStreamInterceptorFactory, InterceptedByteStreamHandler, InterceptedMethodHandler,
    InterceptorFactory, NoopInterceptor,
};
use crate::server::message::{AsMut, AsView};
use crate::server::method_handler::{
    BidiStreamingAdapter, ByteStreamMethodHandler, ClientStreamingAdapter,
    CodecMessageStreamHandler, DynByteStreamMethodHandler, GenericByteStreamAdapter,
    HeapMessageAllocator, ServerStreamingAdapter, UnaryMethodAdapter,
};
use crate::server::method_handler::{CodecRespB, MessageStreamHandler};
use crate::server::service::ServiceRegistrar;
use crate::server::stream::PushStreamProducer;
use crate::server::{
    BidiStreamingMethod, ClientStreamingMethod, ServerStreamingMethod, UnaryMethod,
};
use crate::Status;
use bytes::Buf;

use std::collections::HashMap;

/// A router that maps method names to handlers.
pub struct Router<ReqB, RespB, W, P, F = NoopInterceptor, BSF = NoopInterceptor>
where
    RespB: Buf + Send,
{
    methods: HashMap<String, Box<DynByteStreamMethodHandler<'static, ReqB, W, P, RespB>>>,
    interceptor_factory: F,
    byte_stream_interceptor_factory: BSF,
}

impl<ReqB, RespB: Buf + Send, W, P> Router<ReqB, RespB, W, P, NoopInterceptor, NoopInterceptor> {
    pub fn new() -> Self {
        Self {
            methods: HashMap::new(),
            interceptor_factory: NoopInterceptor,
            byte_stream_interceptor_factory: NoopInterceptor,
        }
    }

    pub fn with_interceptor_factories<F, BSF>(
        interceptor_factory: F,
        byte_stream_interceptor_factory: BSF,
    ) -> Router<ReqB, RespB, W, P, F, BSF> {
        Router {
            methods: HashMap::new(),
            interceptor_factory,
            byte_stream_interceptor_factory,
        }
    }
}

impl<ReqB, W, P, F, BSF> Router<ReqB, CodecRespB, W, P, F, BSF>
where
    ReqB: Buf + Send + 'static,
    W: StreamingResponseWriter<CodecRespB> + Send + 'static,
    P: PushStreamProducer<Item = Incoming<ReqB>> + Send + 'static,
    F: InterceptorFactory,
    BSF: ByteStreamInterceptorFactory,
{
    fn add_message_streaming_handler<H, Req, Resp>(&mut self, path: &str, handler: H)
    where
        H: MessageStreamHandler<Req, Resp> + Send + Sync + 'static,
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
    {
        let intercepted = InterceptedMethodHandler {
            inner: handler,
            interceptor: self.interceptor_factory.create(),
        };
        let codec = CodecMessageStreamHandler::new(intercepted);
        let byte_stream_intercepted = InterceptedByteStreamHandler {
            inner: codec,
            interceptor: self.byte_stream_interceptor_factory.create(),
        };
        self.methods.insert(
            path.to_string(),
            DynByteStreamMethodHandler::new_box(GenericByteStreamAdapter(byte_stream_intercepted)),
        );
    }
}

impl<ReqB, RespB, W, P> Default for Router<ReqB, RespB, W, P, NoopInterceptor, NoopInterceptor>
where
    RespB: Buf + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<ReqB, W, P, F, BSF> ServiceRegistrar for Router<ReqB, CodecRespB, W, P, F, BSF>
where
    ReqB: Buf + Send + 'static,
    W: StreamingResponseWriter<CodecRespB> + Send + 'static,
    P: PushStreamProducer<Item = Incoming<ReqB>> + Send + 'static,
    F: InterceptorFactory,
    BSF: ByteStreamInterceptorFactory,
{
    fn register_unary<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: UnaryMethod<Req, Resp> + Send + Sync + 'static,
    {
        self.add_message_streaming_handler(
            path,
            UnaryMethodAdapter::new(handler, HeapMessageAllocator::new()),
        );
    }

    fn register_server_streaming<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: ServerStreamingMethod<Req, Resp> + Send + Sync + 'static,
    {
        self.add_message_streaming_handler(path, ServerStreamingAdapter(handler));
    }

    fn register_client_streaming<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: ClientStreamingMethod<Req, Resp> + Send + Sync + 'static,
    {
        self.add_message_streaming_handler(path, ClientStreamingAdapter(handler));
    }

    fn register_bidi_streaming<Req, Resp, H>(&mut self, path: &str, handler: H)
    where
        Req: AsView + AsMut + Default + Send + Deserialize + 'static,
        Resp: AsView + AsMut + Default + Send + Serialize + 'static,
        for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
        for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
        H: BidiStreamingMethod<Req, Resp> + Send + Sync + 'static,
    {
        self.add_message_streaming_handler(path, BidiStreamingAdapter(handler));
    }
}

impl<ReqB, RespB, W, P, I, BSI> ByteStreamMethodHandler<ReqB, W, P>
    for Router<ReqB, RespB, W, P, I, BSI>
where
    ReqB: Buf + Send + 'static,
    RespB: Buf + Send + 'static,
    W: StreamingResponseWriter<RespB> + Send + 'static,
    P: PushStreamProducer<Item = Incoming<ReqB>> + Send + 'static,
    I: Send + Sync + 'static,
    BSI: Send + Sync + 'static,
{
    type RespB = RespB;

    async fn call(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp: W,
    ) -> Result<(), Status> {
        let path = req.initial_metadata().method_name().unwrap_or("");

        if let Some(handler) = self.methods.get(path) {
            handler.call(options, req, resp).await
        } else {
            Err(Status::new(
                crate::StatusCode::Unimplemented,
                format!("Method not found: {}", path),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::{Metadata, Outgoing};
    use crate::server::message::{AsMut, AsView};
    use crate::status::ServerStatus;
    use bytes::{Buf, Bytes};

    use protobuf_well_known_types::Timestamp;

    struct MockProducer;
    impl crate::server::stream::PushStreamProducer for MockProducer {
        type Item = Incoming<Bytes>;
        async fn produce(
            self,
            _writer: crate::server::stream::PushStreamWriter<
                Self::Item,
                impl crate::server::stream::PushStreamConsumer<Item = Self::Item>,
            >,
        ) -> Result<(), Status> {
            Ok(())
        }
    }

    struct MockUnaryHandler;

    impl UnaryMethod<Timestamp, Timestamp> for MockUnaryHandler {
        async fn unary(
            &self,
            _req: <Timestamp as AsView>::View<'_>,
            _resp: <Timestamp as AsMut>::Mut<'_>,
        ) -> Result<(), ServerStatus> {
            Ok(())
        }
    }

    struct MockWriter;
    impl StreamingResponseWriter<Bytes> for MockWriter {
        type BodyWriter = MockBodyWriter;
        async fn send_initial_metadata(
            self,
            _metadata: Metadata,
        ) -> Result<Self::BodyWriter, Status> {
            Ok(MockBodyWriter)
        }
    }
    struct MockBodyWriter;
    impl crate::server::call::StreamingResponseBodyWriter<Bytes> for MockBodyWriter {
        async fn write(&mut self, _message: Bytes) -> Result<(), Status> {
            Ok(())
        }
        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    use crate::server::interceptor::InterceptorFactory;
    use crate::server::interceptor::{ByteStreamInterceptor, Interceptor};
    use crate::server::method_handler::GenericByteStreamMethodHandler;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct MockInterceptor {
        called: Arc<AtomicBool>,
    }

    impl Interceptor for MockInterceptor {
        async fn intercept<H, Req, Resp, P, W, L>(
            &self,
            handler: &H,
            options: HandlerCallOptions,
            req: crate::server::call::StreamingRequest<L, P>,
            writer: W,
        ) -> Result<(), Status>
        where
            Req: Send + AsMut,
            Resp: Send + AsMut,
            H: crate::server::method_handler::MessageStreamHandler<Req, Resp> + Sync,
            P: PushStreamProducer<Item = L> + Send,
            W: StreamingResponseWriter<Outgoing<H::ResponseHolder>> + Send,
            L: crate::server::call::Lazy<Req>,
        {
            self.called.store(true, Ordering::SeqCst);
            handler.call(options, req, writer).await
        }
    }

    #[derive(Clone)]
    struct MockByteStreamInterceptor {
        called: Arc<AtomicBool>,
    }

    impl ByteStreamInterceptor for MockByteStreamInterceptor {
        async fn intercept<H, ReqB, P>(
            &self,
            handler: &H,
            options: HandlerCallOptions,
            req: crate::server::call::StreamingRequest<Incoming<ReqB>, P>,
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
    async fn test_router_registration() {
        let mut router: Router<Bytes, Bytes, MockWriter, MockProducer> = Router::new();
        router.register_unary("/test", MockUnaryHandler);
        assert!(router.methods.contains_key("/test"));
    }

    #[derive(Clone)]
    struct MockInterceptorFactory {
        called: Arc<AtomicBool>,
    }

    impl InterceptorFactory for MockInterceptorFactory {
        type Interceptor = MockInterceptor;

        fn create(&self) -> Self::Interceptor {
            MockInterceptor {
                called: self.called.clone(),
            }
        }
    }

    #[derive(Clone)]
    struct MockByteStreamInterceptorFactory {
        called: Arc<AtomicBool>,
    }

    impl ByteStreamInterceptorFactory for MockByteStreamInterceptorFactory {
        type Interceptor = MockByteStreamInterceptor;

        fn create(&self) -> Self::Interceptor {
            MockByteStreamInterceptor {
                called: self.called.clone(),
            }
        }
    }

    #[tokio::test]
    async fn test_router_with_interceptors() {
        let interceptor_called = Arc::new(AtomicBool::new(false));
        let bs_interceptor_called = Arc::new(AtomicBool::new(false));

        let interceptor_factory = MockInterceptorFactory {
            called: interceptor_called.clone(),
        };
        let bs_interceptor_factory = MockByteStreamInterceptorFactory {
            called: bs_interceptor_called.clone(),
        };

        let mut router =
            Router::<Bytes, Bytes, MockWriter, MockProducer, _, _>::with_interceptor_factories(
                interceptor_factory,
                bs_interceptor_factory,
            );
        router.register_unary("/test", MockUnaryHandler);

        // For now, let's just assert registration succeeded and types matched.
        assert!(router.methods.contains_key("/test"));
    }
}
