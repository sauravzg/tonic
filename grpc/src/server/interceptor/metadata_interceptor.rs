use crate::server::call::{HandlerCallOptions, Incoming, StreamingRequest, StreamingResponseWriter};
use crate::server::interceptor::ByteStreamInterceptor;
use crate::server::method_handler::GenericByteStreamMethodHandler;
use crate::server::stream::PushStreamProducer;
use crate::Status;
use bytes::Buf;
use std::marker::PhantomData;

// Re-export Metadata for convenience
pub use crate::server::call::Metadata;

/// A result of intercepting a client request.
pub struct InterceptionResult<S> {
    /// The (possibly modified) client initial metadata.
    pub metadata: Metadata,
    /// The handler for server metadata.
    pub server_metadata_handler: S,
}

/// A trait for intercepting metadata (headers) in gRPC calls.
#[trait_variant::make(Send)]
pub trait MetadataInterceptor: Send + Sync + 'static {
    /// The handler type for server metadata.
    type Handler: ServerMetadataHandler;

    /// Intercepts the request metadata.
    async fn intercept(
        &self,
        metadata: Metadata,
    ) -> Result<InterceptionResult<Self::Handler>, Status>;
}

/// A trait for handling server metadata (headers and trailers).
#[trait_variant::make(Send)]
pub trait ServerMetadataHandler: Send + 'static {
    /// Intercepts server initial metadata.
    async fn on_initial_metadata(&mut self, metadata: Metadata) -> Result<Metadata, Status>;

    /// Intercepts server trailing metadata.
    async fn on_trailing_metadata(&mut self, metadata: Metadata) -> Result<Metadata, Status>;
}

/// A no-op interceptor.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopInterceptor;

impl MetadataInterceptor for NoopInterceptor {
    type Handler = NoopHandler;

    async fn intercept(
        &self,
        metadata: Metadata,
    ) -> Result<InterceptionResult<Self::Handler>, Status> {
        Ok(InterceptionResult {
            metadata,
            server_metadata_handler: NoopHandler,
        })
    }
}

/// A no-op metadata handler.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopHandler;

impl ServerMetadataHandler for NoopHandler {
    async fn on_initial_metadata(&mut self, metadata: Metadata) -> Result<Metadata, Status> {
        Ok(metadata)
    }

    async fn on_trailing_metadata(&mut self, metadata: Metadata) -> Result<Metadata, Status> {
        Ok(metadata)
    }
}

/// A chain of two interceptors.
#[derive(Debug, Clone)]
pub struct Chain<A, B> {
    inner_a: A,
    inner_b: B,
}

impl<A, B> Chain<A, B> {
    /// Creates a new chain.
    pub fn new(inner_a: A, inner_b: B) -> Self {
        Self { inner_a, inner_b }
    }
}

impl<A, B> MetadataInterceptor for Chain<A, B>
where
    A: MetadataInterceptor,
    B: MetadataInterceptor,
{
    type Handler = ChainHandler<A::Handler, B::Handler>;

    async fn intercept(
        &self,
        metadata: Metadata,
    ) -> Result<InterceptionResult<Self::Handler>, Status> {
        let res_a = self.inner_a.intercept(metadata).await?;
        let res_b = self.inner_b.intercept(res_a.metadata).await?;

        Ok(InterceptionResult {
            metadata: res_b.metadata,
            server_metadata_handler: ChainHandler {
                handler_a: res_a.server_metadata_handler,
                handler_b: res_b.server_metadata_handler,
            },
        })
    }
}

/// A chain of two handlers.
pub struct ChainHandler<A, B> {
    handler_a: A,
    handler_b: B,
}

impl<A, B> ServerMetadataHandler for ChainHandler<A, B>
where
    A: ServerMetadataHandler,
    B: ServerMetadataHandler,
{
    async fn on_initial_metadata(&mut self, metadata: Metadata) -> Result<Metadata, Status> {
        // LIFO order for response: B then A
        let md = self.handler_b.on_initial_metadata(metadata).await?;
        self.handler_a.on_initial_metadata(md).await
    }

    async fn on_trailing_metadata(&mut self, metadata: Metadata) -> Result<Metadata, Status> {
        // LIFO order for response: B then A
        let md = self.handler_b.on_trailing_metadata(metadata).await?;
        self.handler_a.on_trailing_metadata(md).await
    }
}

/// Adapter to use a `MetadataInterceptor` as a `ByteStreamInterceptor`.
pub struct MetadataInterceptorAdapter<I>(pub I);

impl<I> ByteStreamInterceptor for MetadataInterceptorAdapter<I>
where
    I: MetadataInterceptor,
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
        let (metadata, stream) = req.into_parts();
        let res = self.0.intercept(metadata).await?;

        let new_req = StreamingRequest::new(stream, res.metadata);
        let wrapped_resp = MetadataResponseWriterWrapper {
            inner: resp,
            handler: res.server_metadata_handler,
            _pd: PhantomData,
        };

        handler.call(options, new_req, wrapped_resp).await
    }
}

struct MetadataResponseWriterWrapper<W, H, T> {
    inner: W,
    handler: H,
    _pd: PhantomData<T>,
}

impl<W, H, T> StreamingResponseWriter<T> for MetadataResponseWriterWrapper<W, H, T>
where
    W: StreamingResponseWriter<T>,
    H: ServerMetadataHandler,
    T: Send,
{
    type BodyWriter = MetadataBodyWriterWrapper<W::BodyWriter, H, T>;

    async fn send_initial_metadata(
        mut self,
        metadata: Metadata,
    ) -> Result<Self::BodyWriter, Status> {
        let metadata = self.handler.on_initial_metadata(metadata).await?;
        let body_writer = self.inner.send_initial_metadata(metadata).await?;
        Ok(MetadataBodyWriterWrapper {
            inner: body_writer,
            handler: self.handler,
            _pd: PhantomData,
        })
    }
}

struct MetadataBodyWriterWrapper<W, H, T> {
    inner: W,
    handler: H,
    _pd: PhantomData<T>,
}

impl<W, H, T> crate::server::call::StreamingResponseBodyWriter<T> for MetadataBodyWriterWrapper<W, H, T>
where
    W: crate::server::call::StreamingResponseBodyWriter<T>,
    H: ServerMetadataHandler,
    T: Send,
{
    async fn write(&mut self, item: T) -> Result<(), Status> {
        self.inner.write(item).await
    }

    async fn send_trailing_metadata(mut self, metadata: Metadata) -> Result<(), Status> {
        let metadata = self.handler.on_trailing_metadata(metadata).await?;
        self.inner.send_trailing_metadata(metadata).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::streaming_response_writer::StreamingResponseBodyWriter;
    use crate::server::stream::PushStream;
    use crate::server::stream::PushStreamWriter;
    use std::sync::Arc;
    use std::sync::Mutex;

    // --- Mocks ---
    struct MockHandler;
    impl GenericByteStreamMethodHandler for MockHandler {
        type RespB = bytes::Bytes;
        async fn call<ReqB, P>(
            &self,
            _options: HandlerCallOptions,
            _req: StreamingRequest<Incoming<ReqB>, P>,
            resp: impl StreamingResponseWriter<Self::RespB>,
        ) -> Result<(), Status>
        where
            ReqB: Buf + Send,
            P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
        {
            let mut writer = resp
                .send_initial_metadata(Metadata::new(http::HeaderMap::new()))
                .await?;
            writer.write(bytes::Bytes::from("hello")).await?;
            writer
                .send_trailing_metadata(Metadata::new(http::HeaderMap::new()))
                .await
        }
    }

    struct MockProducer;
    impl PushStreamProducer for MockProducer {
        type Item = Incoming<Box<dyn Buf + Send>>;
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

    struct MockResponseWriter;
    impl StreamingResponseWriter<bytes::Bytes> for MockResponseWriter {
        type BodyWriter = MockBodyWriter;
        async fn send_initial_metadata(
            self,
            _metadata: Metadata,
        ) -> Result<Self::BodyWriter, Status> {
            Ok(MockBodyWriter)
        }
    }

    struct MockBodyWriter;
    impl StreamingResponseBodyWriter<bytes::Bytes> for MockBodyWriter {
        async fn write(&mut self, _item: bytes::Bytes) -> Result<(), Status> {
            Ok(())
        }
        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    // --- Tests ---

    #[derive(Clone)]
    struct TestInterceptor {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    struct TestHandler {
        name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl MetadataInterceptor for TestInterceptor {
        type Handler = TestHandler;
        async fn intercept(
            &self,
            mut metadata: Metadata,
        ) -> Result<InterceptionResult<Self::Handler>, Status> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:intercept", self.name));
            metadata
                .inner
                .insert("intercepted-by", self.name.parse().unwrap());
            Ok(InterceptionResult {
                metadata,
                server_metadata_handler: TestHandler {
                    name: self.name,
                    log: self.log.clone(),
                },
            })
        }
    }

    impl ServerMetadataHandler for TestHandler {
        async fn on_initial_metadata(
            &mut self,
            mut metadata: Metadata,
        ) -> Result<Metadata, Status> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:initial", self.name));
            metadata
                .inner
                .insert("handled-initial-by", self.name.parse().unwrap());
            Ok(metadata)
        }

        async fn on_trailing_metadata(
            &mut self,
            mut metadata: Metadata,
        ) -> Result<Metadata, Status> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}:trailing", self.name));
            metadata
                .inner
                .insert("handled-trailing-by", self.name.parse().unwrap());
            Ok(metadata)
        }
    }

    #[tokio::test]
    async fn test_interceptor_flow() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let interceptor = TestInterceptor {
            name: "test",
            log: log.clone(),
        };
        let adapter = MetadataInterceptorAdapter(interceptor);

        let handler = MockHandler;
        let options = HandlerCallOptions::default();
        let req = StreamingRequest::new(
            PushStream::new(MockProducer),
            Metadata::new(http::HeaderMap::new()),
        );
        let resp = MockResponseWriter;

        adapter
            .intercept(&handler, options, req, resp)
            .await
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "test:intercept".to_string(),
                "test:initial".to_string(),
                "test:trailing".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_chain_flow() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let a = TestInterceptor {
            name: "a",
            log: log.clone(),
        };
        let b = TestInterceptor {
            name: "b",
            log: log.clone(),
        };
        let chain = Chain::new(a, b);
        let adapter = MetadataInterceptorAdapter(chain);

        let handler = MockHandler;
        let options = HandlerCallOptions::default();
        let req = StreamingRequest::new(
            PushStream::new(MockProducer),
            Metadata::new(http::HeaderMap::new()),
        );
        let resp = MockResponseWriter;

        adapter
            .intercept(&handler, options, req, resp)
            .await
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "a:intercept".to_string(),
                "b:intercept".to_string(),
                "b:initial".to_string(),
                "a:initial".to_string(),
                "b:trailing".to_string(),
                "a:trailing".to_string(),
            ]
        );
    }
}
