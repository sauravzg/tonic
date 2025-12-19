use crate::server::call::StreamingResponseWriter;
use crate::server::interceptor::{ByteStreamInterceptorFactory, NoopInterceptor};
use crate::server::router::Router;
use crate::server::transport::ServerTransport;
use bytes::Buf;

/// A builder for a gRPC server.
pub struct ServerBuilder<F, BSF = NoopInterceptor> {
    interceptor_factory: F,
    byte_stream_interceptor_factory: BSF,
}

impl ServerBuilder<NoopInterceptor, NoopInterceptor> {
    /// Creates a new server builder.
    pub fn new() -> Self {
        Self {
            interceptor_factory: NoopInterceptor,
            byte_stream_interceptor_factory: NoopInterceptor,
        }
    }
}

impl Default for ServerBuilder<NoopInterceptor, NoopInterceptor> {
    fn default() -> Self {
        Self::new()
    }
}

impl<F, BSF> ServerBuilder<F, BSF> {
    /// Sets the interceptor factory for the server.
    pub fn interceptor_factory<NewF>(self, interceptor_factory: NewF) -> ServerBuilder<NewF, BSF>
    where
        NewF: crate::server::interceptor::InterceptorFactory,
    {
        ServerBuilder {
            interceptor_factory,
            byte_stream_interceptor_factory: self.byte_stream_interceptor_factory,
        }
    }

    /// Sets the byte stream interceptor factory for the server.
    pub fn byte_stream_interceptor_factory<NewBSF>(
        self,
        byte_stream_interceptor_factory: NewBSF,
    ) -> ServerBuilder<F, NewBSF>
    where
        NewBSF: ByteStreamInterceptorFactory,
    {
        ServerBuilder {
            interceptor_factory: self.interceptor_factory,
            byte_stream_interceptor_factory,
        }
    }

    /// Configures the transport for the server.
    pub fn with_transport<T, RespB>(self, transport: T) -> RouterBuilder<T, RespB, F, BSF>
    where
        T: ServerTransport,
        T::ReqB: Buf + Send + 'static,
        T::Writer<RespB>: StreamingResponseWriter<RespB> + Send + 'static,
        RespB: Buf + Send + 'static,
        F: crate::server::interceptor::InterceptorFactory,
        BSF: ByteStreamInterceptorFactory,
    {
        let router = Router::with_interceptor_factories(
            self.interceptor_factory,
            self.byte_stream_interceptor_factory,
        );
        RouterBuilder { transport, router }
    }
}

/// A builder for the router, allowing service registration.
pub struct RouterBuilder<T, RespB, F, BSF>
where
    T: ServerTransport,
    T::Writer<RespB>: StreamingResponseWriter<RespB>,
    RespB: Buf + Send + 'static,
{
    transport: T,
    router: Router<T::ReqB, RespB, T::Writer<RespB>, T::Producer, F, BSF>,
}

impl<T, RespB, F, BSF> RouterBuilder<T, RespB, F, BSF>
where
    T: ServerTransport,
    T::Writer<RespB>: StreamingResponseWriter<RespB> + Send + 'static,
    T::ReqB: Buf + Send + 'static,
    RespB: Buf + Send + 'static,
    F: crate::server::interceptor::InterceptorFactory,
    BSF: ByteStreamInterceptorFactory,
{
    /// Adds a service to the router.
    pub fn add_service<S>(mut self, service: S) -> Self
    where
        S: crate::server::service::Service,
        Router<T::ReqB, RespB, T::Writer<RespB>, T::Producer, F, BSF>:
            crate::server::service::ServiceRegistrar,
    {
        service.register_methods(&mut self.router);
        self
    }

    /// Builds the server.
    pub fn build(self) -> crate::server::ServerV2<T, RespB, F, BSF> {
        crate::server::ServerV2 {
            transport: self.transport,
            router: self.router,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::streaming_response_writer::{
        StreamingResponseBodyWriter, StreamingResponseWriter,
    };
    use crate::server::call::{HandlerCallOptions, Metadata, Outgoing, StreamingRequest};
    use crate::server::method_handler::ByteStreamMethodHandler;
    use crate::Status;
    use bytes::{Buf, Bytes};

    use crate::server::message::AsMut;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct MockProducer;
    impl crate::server::stream::PushStreamProducer for MockProducer {
        type Item = crate::server::call::Incoming<Bytes>;
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

    struct MockTransport;

    struct MockWriter<RespB>(std::marker::PhantomData<fn() -> RespB>);
    impl<RespB: Send + 'static> StreamingResponseWriter<RespB> for MockWriter<RespB> {
        type BodyWriter = MockBodyWriter<RespB>;
        async fn send_initial_metadata(
            self,
            _metadata: Metadata,
        ) -> Result<Self::BodyWriter, Status> {
            Ok(MockBodyWriter(std::marker::PhantomData))
        }
    }
    struct MockBodyWriter<RespB>(std::marker::PhantomData<fn() -> RespB>);
    impl<RespB: Send + 'static> StreamingResponseBodyWriter<RespB> for MockBodyWriter<RespB> {
        async fn write(&mut self, _message: RespB) -> Result<(), Status> {
            Ok(())
        }
        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    impl ServerTransport for MockTransport {
        type ReqB = Bytes;
        type Writer<RespB>
            = MockWriter<RespB>
        where
            RespB: Buf + Send + 'static;
        type Producer = MockProducer;

        async fn serve<L, H, RespB>(self, _listener: L, _handler: H) -> Result<(), Status>
        where
            L: crate::server::transport::listener::Listener + Send,
            H: ByteStreamMethodHandler<
                    Self::ReqB,
                    Self::Writer<RespB>,
                    Self::Producer,
                    RespB = RespB,
                > + Send
                + Sync
                + 'static,
            RespB: Buf + Send + 'static,
        {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockInterceptor {
        called: Arc<AtomicBool>,
    }

    struct MockListener;

    impl crate::server::transport::listener::Listener for MockListener {
        type IO = tokio::net::TcpStream;
        type Addr = std::net::SocketAddr;

        async fn accept(&mut self) -> Result<(Self::IO, Self::Addr), std::io::Error> {
            Err(std::io::Error::other("mock"))
        }
    }

    struct MockService;
    impl crate::server::service::Service for MockService {
        fn register_methods<R>(self, _registrar: &mut R)
        where
            R: crate::server::service::ServiceRegistrar,
        {
        }
    }

    #[tokio::test]
    async fn test_server_builder_flow_noop() {
        let server = ServerBuilder::new()
            .with_transport::<_, Bytes>(MockTransport)
            .add_service(MockService)
            .build();

        let listener = MockListener;
        let result = server.serve(listener).await;
        assert!(result.is_ok());
    }
    use crate::server::interceptor::Interceptor;
    use crate::server::interceptor::InterceptorFactory;

    impl Interceptor for MockInterceptor {
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
            H: crate::server::method_handler::MessageStreamHandler<Req, Resp> + Sync,
            P: crate::server::stream::PushStreamProducer<Item = L> + Send,
            W: StreamingResponseWriter<Outgoing<H::ResponseHolder>> + Send,
            L: crate::server::call::Lazy<Req>,
        {
            self.called.store(true, Ordering::SeqCst);
            handler.call(options, req, writer).await
        }
    }

    impl InterceptorFactory for MockInterceptor {
        type Interceptor = MockInterceptor;

        fn create(&self) -> Self::Interceptor {
            self.clone()
        }
    }

    #[tokio::test]
    async fn test_server_builder_flow() {
        let interceptor = MockInterceptor {
            called: Arc::new(AtomicBool::new(false)),
        };

        let server = ServerBuilder::new()
            .interceptor_factory(interceptor)
            .with_transport::<_, Bytes>(MockTransport)
            .add_service(MockService)
            .build();

        let listener = MockListener;
        let result = server.serve(listener).await;
        assert!(result.is_ok());
    }
    #[derive(Clone)]
    struct MockByteStreamInterceptor {
        called: Arc<AtomicBool>,
    }

    impl crate::server::interceptor::ByteStreamInterceptor for MockByteStreamInterceptor {
        async fn intercept<H, ReqB, P>(
            &self,
            handler: &H,
            options: HandlerCallOptions,
            req: StreamingRequest<crate::server::call::Incoming<ReqB>, P>,
            resp: impl StreamingResponseWriter<H::RespB>,
        ) -> Result<(), Status>
        where
            H: crate::server::method_handler::GenericByteStreamMethodHandler,
            ReqB: Buf + Send,
            P: crate::server::stream::PushStreamProducer<
                    Item = crate::server::call::Incoming<ReqB>,
                > + Send,
        {
            self.called.store(true, Ordering::SeqCst);
            handler.call(options, req, resp).await
        }
    }

    struct MockByteStreamInterceptorFactory {
        interceptor: MockByteStreamInterceptor,
    }

    impl crate::server::interceptor::ByteStreamInterceptorFactory for MockByteStreamInterceptorFactory {
        type Interceptor = MockByteStreamInterceptor;

        fn create(&self) -> Self::Interceptor {
            self.interceptor.clone()
        }
    }

    #[tokio::test]
    async fn test_server_builder_flow_with_bs_interceptor() {
        let interceptor = MockByteStreamInterceptor {
            called: Arc::new(AtomicBool::new(false)),
        };
        let factory = MockByteStreamInterceptorFactory {
            interceptor: interceptor.clone(),
        };

        let server = ServerBuilder::new()
            .byte_stream_interceptor_factory(factory)
            .with_transport::<_, Bytes>(MockTransport)
            .add_service(MockService)
            .build();

        let listener = MockListener;
        let result = server.serve(listener).await;
        assert!(result.is_ok());
    }

    #[derive(Clone)]
    struct MyBytes(Bytes);
    impl Buf for MyBytes {
        fn remaining(&self) -> usize {
            self.0.remaining()
        }
        fn chunk(&self) -> &[u8] {
            self.0.chunk()
        }
        fn advance(&mut self, cnt: usize) {
            self.0.advance(cnt)
        }
    }

    #[tokio::test]
    async fn test_server_builder_custom_resp_type() {
        let server = ServerBuilder::new()
            .with_transport::<_, MyBytes>(MockTransport)
            // .add_service(MockService) // Cannot add service for custom RespB as Router doesn't implement ServiceRegistrar for it
            .build();

        // compile-time check that server is ServerV2<_, MyBytes, _, _>
        fn assert_type<T, RespB, F, BSF>(_: &crate::server::ServerV2<T, RespB, F, BSF>)
        where
            T: ServerTransport,
            RespB: Buf + Send + 'static,
        {
        }
        assert_type(&server);

        let listener = MockListener;
        let result = server.serve(listener).await;
        assert!(result.is_ok());
    }
}
