use super::Interceptor;
use crate::server::call::Lazy;
use crate::server::call::{
    HandlerCallOptions, Metadata, StreamingRequest, StreamingResponseBodyWriter,
    StreamingResponseWriter,
};
use crate::server::message::AsMut;
use crate::server::method_handler::{HeapResponseHolder, MessageStreamHandler};
use crate::server::stream::{PushStreamConsumer, PushStreamProducer, PushStreamWriter};
use crate::Status;
use protobuf_well_known_types::Timestamp;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct MockHandler {
    pub call_count: Arc<AtomicUsize>,
}

impl MockHandler {
    pub fn new() -> Self {
        Self {
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl MessageStreamHandler<Timestamp, Timestamp> for MockHandler {
    type ResponseHolder = HeapResponseHolder<Timestamp>;

    async fn call<P, W, L>(
        &self,
        _options: HandlerCallOptions,
        _req: StreamingRequest<L, P>,
        _writer: W,
    ) -> Result<(), Status>
    where
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<crate::server::call::Outgoing<Self::ResponseHolder>> + Send,
        L: Lazy<Timestamp>,
    {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone)]
pub struct MockInterceptor {
    pub order: Arc<AtomicUsize>,
    pub call_count: Arc<AtomicUsize>,
}

impl MockInterceptor {
    pub fn new(order: Arc<AtomicUsize>) -> Self {
        Self {
            order,
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

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
        H: MessageStreamHandler<Req, Resp> + Sync,
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<crate::server::call::Outgoing<H::ResponseHolder>> + Send,
        L: Lazy<Req>,
    {
        // Record execution order
        self.order.fetch_add(1, Ordering::SeqCst);
        self.call_count.fetch_add(1, Ordering::SeqCst);

        // Call inner handler
        handler.call(options, req, writer).await
    }
}

pub struct MockLazy {
    pub inner: Option<Timestamp>,
}

impl Lazy<Timestamp> for MockLazy {
    async fn resolve(mut self, mut target: <Timestamp as AsMut>::Mut<'_>) -> Result<(), Status> {
        if let Some(val) = self.inner.take() {
            target.set_seconds(val.seconds());
            target.set_nanos(val.nanos());
        }
        Ok(())
    }
}

pub struct MockProducer;
impl PushStreamProducer for MockProducer {
    type Item = MockLazy;

    async fn produce(
        self,
        _writer: PushStreamWriter<Self::Item, impl PushStreamConsumer<Item = Self::Item>>,
    ) -> Result<(), Status> {
        Ok(())
    }
}

pub struct MockBodyWriter;
impl StreamingResponseBodyWriter<crate::server::call::Outgoing<HeapResponseHolder<Timestamp>>>
    for MockBodyWriter
{
    async fn write(
        &mut self,
        _item: crate::server::call::Outgoing<HeapResponseHolder<Timestamp>>,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn send_trailing_metadata(self, _trailers: Metadata) -> Result<(), Status> {
        Ok(())
    }
}

pub struct MockWriter;
impl StreamingResponseWriter<crate::server::call::Outgoing<HeapResponseHolder<Timestamp>>> for MockWriter {
    type BodyWriter = MockBodyWriter;

    async fn send_initial_metadata(self, _metadata: Metadata) -> Result<Self::BodyWriter, Status> {
        Ok(MockBodyWriter)
    }
}
