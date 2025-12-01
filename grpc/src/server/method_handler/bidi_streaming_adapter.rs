use send_future::SendFuture;

use crate::server::BidiStreamingMethod;
use crate::server::call::HandlerCallOptions;
use crate::server::call::{
    Metadata, Outgoing, StreamingRequest, StreamingResponseBodyWriter, StreamingResponseWriter,
};
use crate::server::method_handler::MessageStreamHandler;
use crate::server::stream::{PushStreamConsumer, PushStreamExt, PushStreamProducer, PushStreamWriter};
use crate::Status;

use crate::server::call::Lazy;
use crate::server::message::AsMut;
use crate::server::method_handler::message_allocator::HeapResponseHolder;

/// Helper struct to adapt `StreamingResponseBodyWriter<Outgoing<HeapMessageHolder>>` to `PushStreamConsumer<Item=Resp>`.
struct ResponseConsumerAdapter<'a, W, Resp> {
    writer: &'a mut W,
    _phantom: std::marker::PhantomData<Resp>,
}

impl<'a, W, Resp> ResponseConsumerAdapter<'a, W, Resp>
where
    W: StreamingResponseBodyWriter<Outgoing<HeapResponseHolder<Resp>>> + Send,
    Resp: Send + 'static,
{
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, W, Resp> PushStreamConsumer for ResponseConsumerAdapter<'a, W, Resp>
where
    W: StreamingResponseBodyWriter<Outgoing<HeapResponseHolder<Resp>>> + Send,
    Resp: Send,
{
    type Item = Resp;

    async fn write(&mut self, item: Self::Item) -> Result<(), Status> {
        let holder = HeapResponseHolder::new(item);
        self.writer.write(Outgoing::new(holder)).await
    }
}

/// Adapter for `BidiStreamingMethod`.
pub struct BidiStreamingAdapter<T>(pub T);

impl<T, Req, Resp> MessageStreamHandler<Req, Resp> for BidiStreamingAdapter<T>
where
    T: BidiStreamingMethod<Req, Resp> + Sync,
    Req: AsMut + Default + Send + 'static,
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
        let mut body_writer = writer.send_initial_metadata(Metadata::default()).await?;

        // 2. Adapt input stream (L -> Req)
        let (_, stream) = req.into_parts();
        let req_stream = stream.then(|lazy_req| async move {
            let mut req = Req::default();
            lazy_req.resolve(req.as_mut()).send().await?;
            Ok(req)
        });

        // 3. Call method
        {
            let adapter = ResponseConsumerAdapter::new(&mut body_writer);
            let stream_writer = PushStreamWriter::new(adapter);
            self.0
                .bidi_streaming(req_stream, stream_writer)
                .await
                .map_err(|s| s.into_status())?;
        }

        // 4. Send Trailers
        body_writer
            .send_trailing_metadata(Metadata::default())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::BidiStreamingMethod;
    use crate::server::call::metadata_writer::{InitialMetadataWriter, TrailingMetadataWriter};
    use crate::server::call::test_util::StreamingResponseImpl;
    use crate::server::stream::{PushStream, PushStreamConsumer, PushStreamProducer, PushStreamWriter};
    use crate::Status;

    struct MockBidiStreamingMethod {
        resp_to_return: i32,
    }

    impl BidiStreamingMethod<i32, i32> for MockBidiStreamingMethod {
        async fn bidi_streaming<P, C>(
            &self,
            _req: PushStream<i32, P>,
            writer: PushStreamWriter<i32, C>,
        ) -> Result<(), crate::ServerStatus>
        where
            P: PushStreamProducer<Item = i32> + Send,
            C: PushStreamConsumer<Item = i32> + Send,
        {
            Ok(())
        }
    }

    struct MockMetadataWriter {
        sent_initial: bool,
        sent_trailing: bool,
    }

    impl InitialMetadataWriter for MockMetadataWriter {
        async fn send_initial_metadata(mut self, _metadata: Metadata) -> Result<(), Status> {
            self.sent_initial = true;
            Ok(())
        }
    }

    impl TrailingMetadataWriter for MockMetadataWriter {
        async fn send_trailing_metadata(mut self, _metadata: Metadata) -> Result<(), Status> {
            self.sent_trailing = true;
            Ok(())
        }
    }

    struct MockProducer;
    impl PushStreamProducer for MockProducer {
        type Item = i32;
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

    #[tokio::test]
    async fn test_bidi_streaming_adapter_v2_success() {
        use protobuf_well_known_types::Timestamp;

        struct MockBidiStreamingMethodV2;
        impl BidiStreamingMethod<Timestamp, Timestamp> for MockBidiStreamingMethodV2 {
            async fn bidi_streaming<P, C>(
                &self,
                _req: PushStream<Timestamp, P>,
                mut writer: PushStreamWriter<Timestamp, C>,
            ) -> Result<(), crate::ServerStatus>
            where
                P: PushStreamProducer<Item = Timestamp> + Send,
                C: PushStreamConsumer<Item = Timestamp> + Send,
            {
                // Write one item
                let mut msg = Timestamp::new();
                msg.set_seconds(100);
                writer.write(msg).await.unwrap();
                Ok(())
            }
        }

        let method = MockBidiStreamingMethodV2;
        let adapter = BidiStreamingAdapter(method);

        // V2 expects Lazy<Req>
        struct MockLazy(Timestamp);
        impl Lazy<Timestamp> for MockLazy {
            async fn resolve(self, mut dest: <Timestamp as AsMut>::Mut<'_>) -> Result<(), Status> {
                dest.set_seconds(self.0.seconds());
                Ok(())
            }
        }

        struct MockLazyProducer;
        impl PushStreamProducer for MockLazyProducer {
            type Item = MockLazy;
            async fn produce(
                self,
                writer: PushStreamWriter<Self::Item, impl PushStreamConsumer<Item = Self::Item>>,
            ) -> Result<(), Status> {
                // Just close
                Ok(())
            }
        }

        let producer = MockLazyProducer;
        let stream = PushStream::new(producer);
        let req = StreamingRequest::new(stream, Metadata::default());

        // Consumer for Outgoing<HeapResponseHolder<Timestamp>>
        struct MockV2Consumer;
        impl PushStreamConsumer for MockV2Consumer {
            type Item = Outgoing<HeapResponseHolder<Timestamp>>;
            async fn write(&mut self, _item: Self::Item) -> Result<(), Status> {
                Ok(())
            }
        }

        let stream_writer = PushStreamWriter::new(MockV2Consumer);

        let initial_writer = MockMetadataWriter {
            sent_initial: false,
            sent_trailing: false,
        };
        let trailing_writer = MockMetadataWriter {
            sent_initial: false,
            sent_trailing: false,
        };
        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        let result = adapter
            .call(HandlerCallOptions::default(), req, writer)
            .await;

        assert!(result.is_ok());
    }
}
