use crate::server::call::Metadata;
use crate::Status;

/// A trait representing a gRPC response.
///
/// This trait enforces the correct state transitions:
/// 1. Send initial metadata -> transitions to body writing.
#[trait_variant::make(Send)]
pub trait StreamingResponseWriter<T>: Send {
    /// The type of the body writer returned after sending initial metadata.
    type BodyWriter: StreamingResponseBodyWriter<T>;

    /// Sends initial metadata and transitions to the body writing state.
    async fn send_initial_metadata(self, metadata: Metadata) -> Result<Self::BodyWriter, Status>;
}

/// A trait for writing the body of a gRPC response.
#[trait_variant::make(Send)]
pub trait StreamingResponseBodyWriter<T>: Send {
    /// Writes a message to the stream.
    async fn write(&mut self, item: T) -> Result<(), Status>;

    /// Sends trailing metadata to be sent when the stream closes.
    /// These will be merged with the final Status by the framework.
    async fn send_trailing_metadata(self, trailers: Metadata) -> Result<(), Status>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::metadata_writer::{InitialMetadataWriter, TrailingMetadataWriter};
    use crate::server::call::test_util::StreamingResponseImpl;
    use crate::server::call::Metadata;
    use crate::server::stream::PushStreamWriter;
    use crate::Status;

    struct MockMetadataWriter;

    impl InitialMetadataWriter for MockMetadataWriter {
        async fn send_initial_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    impl TrailingMetadataWriter for MockMetadataWriter {
        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    struct MockPushStreamConsumer;

    impl crate::server::stream::PushStreamConsumer for MockPushStreamConsumer {
        type Item = i32;

        async fn write(&mut self, _item: Self::Item) -> Result<(), Status> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_streaming_response_writer_flow() {
        let consumer = MockPushStreamConsumer;
        let stream_writer = PushStreamWriter::new(consumer);
        let initial_writer = MockMetadataWriter;
        let trailing_writer = MockMetadataWriter;
        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        // 1. Send initial metadata
        let mut body_writer = writer
            .send_initial_metadata(Metadata::default())
            .await
            .unwrap();

        // 2. Write body
        body_writer.write(1).await.unwrap();
        body_writer.write(2).await.unwrap();

        // 3. Send trailing metadata
        body_writer
            .send_trailing_metadata(Metadata::default())
            .await
            .unwrap();
    }

    struct Interceptor<T>(T);

    impl<T, Item> StreamingResponseWriter<Item> for Interceptor<T>
    where
        T: StreamingResponseWriter<Item> + Send,
        Item: Send + 'static,
    {
        type BodyWriter = InterceptorBodyWriter<T::BodyWriter>;

        async fn send_initial_metadata(
            self,
            metadata: Metadata,
        ) -> Result<Self::BodyWriter, Status> {
            // Intercept headers here
            let inner_writer = self.0.send_initial_metadata(metadata).await?;
            Ok(InterceptorBodyWriter(inner_writer))
        }
    }

    struct InterceptorBodyWriter<T>(T);

    impl<T, Item> StreamingResponseBodyWriter<Item> for InterceptorBodyWriter<T>
    where
        T: StreamingResponseBodyWriter<Item> + Send,
        Item: Send + 'static,
    {
        async fn write(&mut self, item: Item) -> Result<(), Status> {
            // Intercept body here
            self.0.write(item).await
        }

        async fn send_trailing_metadata(self, trailers: Metadata) -> Result<(), Status> {
            // Intercept trailers here
            self.0.send_trailing_metadata(trailers).await
        }
    }

    #[tokio::test]
    async fn test_interceptor_composition() {
        let consumer = MockPushStreamConsumer;
        let stream_writer = PushStreamWriter::new(consumer);
        let initial_writer = MockMetadataWriter;
        let trailing_writer = MockMetadataWriter;
        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);
        let interceptor = Interceptor(writer);

        let mut body_writer = interceptor
            .send_initial_metadata(Metadata::default())
            .await
            .unwrap();
        body_writer.write(1).await.unwrap();
        body_writer
            .send_trailing_metadata(Metadata::default())
            .await
            .unwrap();
    }
}
