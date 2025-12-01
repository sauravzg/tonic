use crate::server::call::metadata_writer::{InitialMetadataWriter, TrailingMetadataWriter};
use crate::server::call::{Metadata, StreamingResponseBodyWriter, StreamingResponseWriter};
// use crate::server::message::AsMut;
use crate::server::stream::{PushStreamConsumer, PushStreamWriter};
use crate::Status;

/// A wrapper around PushStreamWriter that also supports sending metadata.
pub struct StreamingResponseImpl<T, C, I, Tr> {
    writer: PushStreamWriter<T, C>,
    initial_metadata_writer: I,
    trailing_metadata_writer: Tr,
}

impl<T: Send, C, I, Tr> StreamingResponseImpl<T, C, I, Tr> {
    /// Creates a new StreamingResponseImpl.
    pub fn new(
        writer: PushStreamWriter<T, C>,
        initial_metadata_writer: I,
        trailing_metadata_writer: Tr,
    ) -> Self {
        Self {
            writer,
            initial_metadata_writer,
            trailing_metadata_writer,
        }
    }
}

impl<T, C, I, Tr> StreamingResponseWriter<T> for StreamingResponseImpl<T, C, I, Tr>
where
    T: Send,
    C: PushStreamConsumer<Item = T> + Send,
    I: InitialMetadataWriter + Send,
    Tr: TrailingMetadataWriter + Send,
{
    type BodyWriter = StreamingResponseBodyImpl<T, C, Tr>;

    async fn send_initial_metadata(self, metadata: Metadata) -> Result<Self::BodyWriter, Status> {
        self.initial_metadata_writer
            .send_initial_metadata(metadata)
            .await?;
        Ok(StreamingResponseBodyImpl {
            writer: self.writer,
            trailing_metadata_writer: self.trailing_metadata_writer,
        })
    }
}

/// A writer for the body of a gRPC response.
pub struct StreamingResponseBodyImpl<T, C, Tr> {
    writer: PushStreamWriter<T, C>,
    trailing_metadata_writer: Tr,
}

impl<T, C, Tr> StreamingResponseBodyImpl<T, C, Tr> {
    /// Consumes the StreamingResponseBodyImpl and returns the inner PushStreamWriter.
    pub fn into_inner(self) -> PushStreamWriter<T, C> {
        self.writer
    }
}

impl<T, C, Tr> StreamingResponseBodyWriter<T> for StreamingResponseBodyImpl<T, C, Tr>
where
    T: Send,
    C: PushStreamConsumer<Item = T> + Send,
    Tr: TrailingMetadataWriter + Send,
{
    async fn write(&mut self, item: T) -> Result<(), Status> {
        self.writer.write(item).await
    }

    async fn send_trailing_metadata(self, trailers: Metadata) -> Result<(), Status> {
        self.trailing_metadata_writer
            .send_trailing_metadata(trailers)
            .await
    }
}
