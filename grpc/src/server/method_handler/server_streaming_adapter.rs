use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::server::ServerStreamingMethod;
use crate::server::call::Lazy;
use crate::server::call::{
    HandlerCallOptions, Metadata, Outgoing, StreamingRequest, StreamingResponseBodyWriter,
    StreamingResponseWriter,
};
use crate::server::message::AsMut;
use crate::server::message::AsView;
use crate::server::method_handler::message_allocator::HeapResponseHolder;
use crate::server::method_handler::MessageStreamHandler;
use crate::server::stream::{PushStreamConsumer, PushStreamExt, PushStreamProducer, PushStreamWriter};
use crate::Status;
use send_future::SendFuture;

/// Adapter for `ServerStreamingMethod`.
pub struct ServerStreamingAdapter<T>(pub T);

struct ResponseMessageConsumer<'a, W, Resp> {
    writer: &'a mut W,
    _marker: PhantomData<fn(Resp) -> ()>,
}

impl<'a, W, Resp> ResponseMessageConsumer<'a, W, Resp> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            _marker: PhantomData,
        }
    }
}

impl<'a, W, Resp> PushStreamConsumer for ResponseMessageConsumer<'a, W, Resp>
where
    W: StreamingResponseBodyWriter<Outgoing<HeapResponseHolder<Resp>>> + Send,
    Resp: Send,
{
    type Item = Resp;

    async fn write(&mut self, item: Self::Item) -> Result<(), Status> {
        self.writer
            .write(Outgoing::new(HeapResponseHolder::new(item)))
            .await
    }
}

struct SingleMessageStreamConsumer<'a, M, W, Resp, Req> {
    method: &'a M,
    writer: &'a mut W,
    called: AtomicBool,
    _marker: PhantomData<fn(Req, Resp) -> ()>,
}

impl<'a, M, W, Resp, Req> SingleMessageStreamConsumer<'a, M, W, Resp, Req> {
    fn new(writer: &'a mut W, method: &'a M) -> Self {
        Self {
            method,
            writer,
            called: AtomicBool::new(false),
            _marker: PhantomData,
        }
    }
}

impl<'a, M, W, Resp, Req> PushStreamConsumer for SingleMessageStreamConsumer<'a, M, W, Resp, Req>
where
    M: ServerStreamingMethod<Req, Resp> + Sync,
    W: StreamingResponseBodyWriter<Outgoing<HeapResponseHolder<Resp>>> + Send,
    Resp: AsMut + Send,
    Req: AsMut + AsView + Send,
{
    type Item = Req;

    async fn write(&mut self, req: Self::Item) -> Result<(), Status> {
        if self.called.swap(true, Ordering::SeqCst) {
            return Err(Status::new(
                crate::status::StatusCode::Internal,
                "Unary request must have exactly one message",
            ));
        }
        // Resolve request.
        self.method
            .server_streaming(
                req.as_view(),
                PushStreamWriter::new(ResponseMessageConsumer::new(self.writer)),
            )
            .send()
            .await
            .map_err(|s| s.into_status())?;
        Ok(())
    }
}

impl<T, Req, Resp> MessageStreamHandler<Req, Resp> for ServerStreamingAdapter<T>
where
    T: ServerStreamingMethod<Req, Resp> + Sync,
    Req: AsMut + AsView + Default + Send + 'static,
    Resp: AsMut + Default + Send + 'static,
{
    type ResponseHolder = HeapResponseHolder<Resp>;

    async fn call<P, W, L>(
        &self,
        _options: HandlerCallOptions,
        req: StreamingRequest<L, P>,
        writer: W,
    ) -> Result<(), Status>
    where
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<Outgoing<Self::ResponseHolder>> + Send,
        L: Lazy<Req>,
    {
        // 1. Send Initial Metadata
        let send_initial_metadata = writer.send_initial_metadata(Metadata::default()).await?;
        let mut body_writer = send_initial_metadata;

        // 2. Adapt input stream (L -> Req)
        let (_, stream) = req.into_parts();
        let req_stream = stream.then(|lazy_req| async move {
            let mut req = Req::default();
            lazy_req.resolve(req.as_mut()).send().await?;
            Ok(req)
        });

        // 3. Call method
        req_stream
            .run(PushStreamWriter::new(SingleMessageStreamConsumer::new(
                &mut body_writer,
                &self.0,
            )))
            .await?;

        // 4. Send Trailers
        body_writer
            .send_trailing_metadata(Metadata::default())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::ServerStreamingMethod;
    use crate::server::call::metadata_writer::{InitialMetadataWriter, TrailingMetadataWriter};
    use crate::server::call::test_util::StreamingResponseImpl;
    use crate::server::call::{Metadata, StreamingRequest};
    use crate::server::message::AsView;
    use crate::server::stream::{PushStream, PushStreamConsumer, PushStreamProducer, PushStreamWriter};
    use crate::{ServerStatus, Status, StatusCode};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use protobuf_well_known_types::Timestamp;

    struct MockServerStreamingMethod {
        expected_req: i32,
    }

    impl ServerStreamingMethod<Timestamp, i32> for MockServerStreamingMethod {
        async fn server_streaming<C>(
            &self,
            req: <Timestamp as AsView>::View<'_>,
            _writer: PushStreamWriter<i32, C>,
        ) -> Result<(), ServerStatus>
        where
            C: PushStreamConsumer<Item = i32> + Send,
        {
            assert_eq!(req.seconds(), self.expected_req as i64);
            // We can write to writer here if we want, but for now just return Ok
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockMetadataWriter {
        sent_initial: Arc<AtomicBool>,
        sent_trailing: Arc<AtomicBool>,
    }

    impl InitialMetadataWriter for MockMetadataWriter {
        async fn send_initial_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            self.sent_initial.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl TrailingMetadataWriter for MockMetadataWriter {
        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            self.sent_trailing.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    struct MockProducer {
        item: Timestamp,
    }
    impl PushStreamProducer for MockProducer {
        type Item = Timestamp;
        async fn produce(
            self,
            mut writer: PushStreamWriter<
                Self::Item,
                impl crate::server::stream::PushStreamConsumer<Item = Self::Item>,
            >,
        ) -> Result<(), Status> {
            writer.write(self.item).await
        }
    }

    #[tokio::test]
    async fn test_server_streaming_adapter_v2_success() {
        struct MockServerStreamingMethodV2 {
            expected_req: Timestamp,
        }
        impl ServerStreamingMethod<Timestamp, Timestamp> for MockServerStreamingMethodV2 {
            async fn server_streaming<C>(
                &self,
                req: <Timestamp as AsView>::View<'_>,
                mut writer: PushStreamWriter<Timestamp, C>,
            ) -> Result<(), crate::ServerStatus>
            where
                C: PushStreamConsumer<Item = Timestamp> + Send,
            {
                if req.seconds() != self.expected_req.seconds() {
                    return Err(ServerStatus::new(StatusCode::Internal, "req mismatch"));
                }
                // Need to convert View back to Owned or clone data if we want to write it back?
                // But we are writing `Timestamp` (owned).
                // View has accessors.
                let mut resp = Timestamp::new();
                resp.set_seconds(req.seconds());
                writer.write(resp).await.unwrap();
                Ok(())
            }
        }

        let mut expected = Timestamp::new();
        expected.set_seconds(42);
        let method = MockServerStreamingMethodV2 {
            expected_req: expected.clone(),
        };
        let adapter = ServerStreamingAdapter(method);

        struct MockLazy(Timestamp);
        impl Lazy<Timestamp> for MockLazy {
            async fn resolve(self, mut dest: <Timestamp as AsMut>::Mut<'_>) -> Result<(), Status> {
                dest.set_seconds(self.0.seconds());
                Ok(())
            }
        }

        struct MockLazyProducer {
            item: Timestamp,
        }
        impl PushStreamProducer for MockLazyProducer {
            type Item = MockLazy;
            async fn produce(
                self,
                mut writer: PushStreamWriter<
                    Self::Item,
                    impl PushStreamConsumer<Item = Self::Item>,
                >,
            ) -> Result<(), Status> {
                writer.write(MockLazy(self.item)).await
            }
        }

        let producer = MockLazyProducer {
            item: expected.clone(),
        };
        let stream = PushStream::new(producer);
        let req = StreamingRequest::new(stream, Metadata::default());

        struct MockV2Consumer;
        impl PushStreamConsumer for MockV2Consumer {
            type Item = Outgoing<HeapResponseHolder<Timestamp>>;
            async fn write(&mut self, _item: Self::Item) -> Result<(), Status> {
                Ok(())
            }
        }

        let stream_writer = PushStreamWriter::new(MockV2Consumer);

        let sent_initial = Arc::new(AtomicBool::new(false));
        let sent_trailing = Arc::new(AtomicBool::new(false));

        let initial_writer = MockMetadataWriter {
            sent_initial: sent_initial.clone(),
            sent_trailing: sent_trailing.clone(),
        };
        let trailing_writer = MockMetadataWriter {
            sent_initial: sent_initial.clone(),
            sent_trailing: sent_trailing.clone(),
        };

        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        let result = adapter
            .call(HandlerCallOptions::default(), req, writer)
            .await;

        assert!(result.is_ok());
        assert!(sent_initial.load(Ordering::SeqCst));
        assert!(sent_trailing.load(Ordering::SeqCst));
    }
}
