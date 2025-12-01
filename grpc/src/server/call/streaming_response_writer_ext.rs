use crate::server::call::{Metadata, StreamingResponseBodyWriter, StreamingResponseWriter};
use crate::Status;
use std::future::Future;
use std::marker::PhantomData;

/// Extension trait for `StreamingResponseWriter`.
pub trait StreamingResponseWriterExt<T>: Sized {
    /// Maps items to be written to the stream using the provided async function.
    fn map_message<U, F, Fut>(self, f: F) -> MapMessageStreamingResponseWriter<Self, F, U>
    where
        U: Send,
        F: FnMut(U) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T, Status>> + Send;

    /// Maps initial metadata using the provided async function.
    fn map_initial_metadata<F, Fut>(self, f: F) -> MapInitialMetadata<Self, F>
    where
        F: FnMut(Metadata) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Metadata, Status>> + Send;

    /// Maps trailing metadata using the provided async function.
    fn map_trailing_metadata<F, Fut>(self, f: F) -> MapTrailingMetadata<Self, F>
    where
        F: FnMut(Metadata) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Metadata, Status>> + Send;
}

impl<T, W> StreamingResponseWriterExt<T> for W
where
    W: StreamingResponseWriter<T>,
{
    fn map_message<U, F, Fut>(self, f: F) -> MapMessageStreamingResponseWriter<Self, F, U>
    where
        U: Send,
        F: FnMut(U) -> Fut + Send + Sync,
        Fut: Future<Output = Result<T, Status>> + Send,
    {
        MapMessageStreamingResponseWriter {
            inner: self,
            f,
            _phantom: PhantomData,
        }
    }

    fn map_initial_metadata<F, Fut>(self, f: F) -> MapInitialMetadata<Self, F>
    where
        F: FnMut(Metadata) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Metadata, Status>> + Send,
    {
        MapInitialMetadata { inner: self, f }
    }

    fn map_trailing_metadata<F, Fut>(self, f: F) -> MapTrailingMetadata<Self, F>
    where
        F: FnMut(Metadata) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Metadata, Status>> + Send,
    {
        MapTrailingMetadata { inner: self, f }
    }
}

/// A wrapper that maps items before writing them to the inner writer.
pub struct MapMessageStreamingResponseWriter<W, F, U> {
    inner: W,
    f: F,
    _phantom: PhantomData<fn(U)>,
}

impl<T, U, W, F, Fut> StreamingResponseWriter<U> for MapMessageStreamingResponseWriter<W, F, U>
where
    T: Send,
    U: Send,
    W: StreamingResponseWriter<T> + Send,
    F: FnMut(U) -> Fut + Send + Sync,
    Fut: Future<Output = Result<T, Status>> + Send,
{
    type BodyWriter = MapMessageStreamingResponseBodyWriter<W::BodyWriter, F, U>;

    async fn send_initial_metadata(self, metadata: Metadata) -> Result<Self::BodyWriter, Status> {
        let inner_body_writer = self.inner.send_initial_metadata(metadata).await?;
        Ok(MapMessageStreamingResponseBodyWriter {
            inner: inner_body_writer,
            f: self.f,
            _phantom: PhantomData,
        })
    }
}

/// A wrapper that maps items before writing them to the inner body writer.
pub struct MapMessageStreamingResponseBodyWriter<W, F, U> {
    inner: W,
    f: F,
    _phantom: PhantomData<fn(U)>,
}

impl<T, U, W, F, Fut> StreamingResponseBodyWriter<U>
    for MapMessageStreamingResponseBodyWriter<W, F, U>
where
    T: Send,
    U: Send,
    W: StreamingResponseBodyWriter<T> + Send,
    F: FnMut(U) -> Fut + Send + Sync,
    Fut: Future<Output = Result<T, Status>> + Send,
{
    async fn write(&mut self, item: U) -> Result<(), Status> {
        let mapped_item = (self.f)(item).await?;
        self.inner.write(mapped_item).await
    }

    async fn send_trailing_metadata(self, trailers: Metadata) -> Result<(), Status> {
        self.inner.send_trailing_metadata(trailers).await
    }
}

/// A wrapper that maps initial metadata.
pub struct MapInitialMetadata<W, F> {
    inner: W,
    f: F,
}

impl<T, W, F, Fut> StreamingResponseWriter<T> for MapInitialMetadata<W, F>
where
    T: Send,
    W: StreamingResponseWriter<T> + Send,
    F: FnMut(Metadata) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Metadata, Status>> + Send,
{
    type BodyWriter = W::BodyWriter;

    async fn send_initial_metadata(
        mut self,
        metadata: Metadata,
    ) -> Result<Self::BodyWriter, Status> {
        let mapped_metadata = (self.f)(metadata).await?;
        self.inner.send_initial_metadata(mapped_metadata).await
    }
}

/// A wrapper that maps trailing metadata.
pub struct MapTrailingMetadata<W, F> {
    inner: W,
    f: F,
}

impl<T, W, F, Fut> StreamingResponseWriter<T> for MapTrailingMetadata<W, F>
where
    T: Send,
    W: StreamingResponseWriter<T> + Send,
    F: FnMut(Metadata) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Metadata, Status>> + Send,
{
    type BodyWriter = MapTrailingMetadataBodyWriter<W::BodyWriter, F>;

    async fn send_initial_metadata(self, metadata: Metadata) -> Result<Self::BodyWriter, Status> {
        let inner_body_writer = self.inner.send_initial_metadata(metadata).await?;
        Ok(MapTrailingMetadataBodyWriter {
            inner: inner_body_writer,
            f: self.f,
        })
    }
}

/// A wrapper that maps trailing metadata on the body writer.
pub struct MapTrailingMetadataBodyWriter<W, F> {
    inner: W,
    f: F,
}

impl<T, W, F, Fut> StreamingResponseBodyWriter<T> for MapTrailingMetadataBodyWriter<W, F>
where
    T: Send,
    W: StreamingResponseBodyWriter<T> + Send,
    F: FnMut(Metadata) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Metadata, Status>> + Send,
{
    async fn write(&mut self, item: T) -> Result<(), Status> {
        self.inner.write(item).await
    }

    async fn send_trailing_metadata(mut self, trailers: Metadata) -> Result<(), Status> {
        let mapped_trailers = (self.f)(trailers).await?;
        self.inner.send_trailing_metadata(mapped_trailers).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::metadata_writer::{InitialMetadataWriter, TrailingMetadataWriter};
    use crate::server::call::test_util::StreamingResponseImpl;
    use crate::server::call::Metadata;
    use crate::server::stream::stream_writer::PushStreamWriter;
    use crate::server::stream::PushStreamConsumer;
    use crate::Status;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockMetadataWriter {
        metadata: Arc<Mutex<Option<Metadata>>>,
    }

    impl MockMetadataWriter {
        fn new() -> Self {
            Self {
                metadata: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl InitialMetadataWriter for MockMetadataWriter {
        async fn send_initial_metadata(self, metadata: Metadata) -> Result<(), Status> {
            *self.metadata.lock().unwrap() = Some(metadata);
            Ok(())
        }
    }

    impl TrailingMetadataWriter for MockMetadataWriter {
        async fn send_trailing_metadata(self, metadata: Metadata) -> Result<(), Status> {
            *self.metadata.lock().unwrap() = Some(metadata);
            Ok(())
        }
    }

    struct MockPushStreamConsumer {
        items: Arc<Mutex<Vec<i32>>>,
    }

    impl MockPushStreamConsumer {
        fn new() -> Self {
            Self {
                items: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl PushStreamConsumer for MockPushStreamConsumer {
        type Item = i32;

        async fn write(&mut self, item: Self::Item) -> Result<(), Status> {
            self.items.lock().unwrap().push(item);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_map_message() {
        let consumer = MockPushStreamConsumer::new();
        let items = consumer.items.clone();
        let stream_writer = PushStreamWriter::new(consumer);
        let initial_writer = MockMetadataWriter::new();
        let trailing_writer = MockMetadataWriter::new();
        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        let writer = writer.map_message(|x| async move { Ok(x * 2) });

        let mut body_writer = writer
            .send_initial_metadata(Metadata::default())
            .await
            .unwrap();
        body_writer.write(10).await.unwrap();

        assert_eq!(*items.lock().unwrap(), vec![20]);
    }

    #[tokio::test]
    async fn test_map_initial_metadata() {
        let consumer = MockPushStreamConsumer::new();
        let stream_writer = PushStreamWriter::new(consumer);
        let initial_writer = MockMetadataWriter::new();
        let captured_metadata = initial_writer.metadata.clone();
        let trailing_writer = MockMetadataWriter::new();
        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        let writer = writer.map_initial_metadata(|mut md| async move {
            md.inner.insert(
                http::header::HeaderName::from_static("test-key"),
                "test-val".parse().unwrap(),
            );
            Ok(md)
        });

        writer
            .send_initial_metadata(Metadata::default())
            .await
            .unwrap();

        let md = captured_metadata.lock().unwrap().take().unwrap();
        assert_eq!(md.inner.get("test-key").unwrap(), "test-val");
    }

    #[tokio::test]
    async fn test_map_trailing_metadata() {
        let consumer = MockPushStreamConsumer::new();
        let stream_writer = PushStreamWriter::new(consumer);
        let initial_writer = MockMetadataWriter::new();
        let trailing_writer = MockMetadataWriter::new();
        let captured_trailers = trailing_writer.metadata.clone();
        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        let writer = writer.map_trailing_metadata(|mut md| async move {
            md.inner.insert(
                http::header::HeaderName::from_static("trailer-key"),
                "trailer-val".parse().unwrap(),
            );
            Ok(md)
        });

        let body_writer = writer
            .send_initial_metadata(Metadata::default())
            .await
            .unwrap();
        body_writer
            .send_trailing_metadata(Metadata::default())
            .await
            .unwrap();

        let md = captured_trailers.lock().unwrap().take().unwrap();
        assert_eq!(md.inner.get("trailer-key").unwrap(), "trailer-val");
    }

    #[tokio::test]
    async fn test_composition() {
        let consumer = MockPushStreamConsumer::new();
        let items = consumer.items.clone();
        let stream_writer = PushStreamWriter::new(consumer);

        let initial_writer = MockMetadataWriter::new();
        let captured_initial = initial_writer.metadata.clone();

        let trailing_writer = MockMetadataWriter::new();
        let captured_trailing = trailing_writer.metadata.clone();

        let writer = StreamingResponseImpl::new(stream_writer, initial_writer, trailing_writer);

        let writer = writer
            .map_initial_metadata(|mut md| async move {
                md.inner.insert(
                    http::header::HeaderName::from_static("init"),
                    "1".parse().unwrap(),
                );
                Ok(md)
            })
            .map_message(|x| async move { Ok(x + 1) })
            .map_trailing_metadata(|mut md| async move {
                md.inner.insert(
                    http::header::HeaderName::from_static("trail"),
                    "2".parse().unwrap(),
                );
                Ok(md)
            });

        let mut body_writer = writer
            .send_initial_metadata(Metadata::default())
            .await
            .unwrap();
        body_writer.write(10).await.unwrap();
        body_writer
            .send_trailing_metadata(Metadata::default())
            .await
            .unwrap();

        assert_eq!(
            captured_initial
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .inner
                .get("init")
                .unwrap(),
            "1"
        );
        assert_eq!(*items.lock().unwrap(), vec![11]);
        assert_eq!(
            captured_trailing
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .inner
                .get("trail")
                .unwrap(),
            "2"
        );
    }
}
