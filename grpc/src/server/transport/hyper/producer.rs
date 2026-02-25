use crate::server::call::{Metadata, Incoming, StreamingResponseBodyWriter};
use crate::status::StatusCode;
use crate::server::stream::{PushStreamConsumer, PushStreamProducer, PushStreamWriter};
use crate::Status;
use bytes::Bytes;
use http::HeaderMap;
use http_body::Body;
use std::pin::Pin;

use super::buffer::BufList;
use super::decoder::HyperBodyDecoder;

/// A producer that reads from a Hyper body and produces gRPC messages.
pub struct HyperStreamProducer<B> {
    body: B,
    decoder: HyperBodyDecoder,
}

impl<B> HyperStreamProducer<B> {
    pub fn new(body: B) -> Self {
        Self {
            body,
            decoder: HyperBodyDecoder::new(),
        }
    }

    /// Produces messages to a `StreamingResponseBodyWriter`.
    ///
    /// This method handles both data frames (decoding them into messages) and
    /// trailer frames (extracting status and metadata).
    pub async fn produce_to_writer<W>(mut self, mut writer: W) -> Result<(), Status>
    where
        B: Body<Data = Bytes> + Send + Unpin + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
        W: StreamingResponseBodyWriter<Incoming<BufList<Bytes>>>,
    {
        loop {
            // 1. Drain any complete messages from the decoder
            while let Some(msg) = self.decoder.decode() {
                writer.write(msg).await?;
            }

            // 2. Read more data from the body
            let frame_res =
                std::future::poll_fn(|cx| Pin::new(&mut self.body).poll_frame(cx)).await;

            match frame_res {
                Some(Ok(frame)) => {
                    match frame.into_data() {
                        Ok(data) => {
                            self.decoder.push(data);
                        }
                        Err(frame) => {
                            if let Ok(trailers) = frame.into_trailers() {
                                // Handle trailers
                                let (status, metadata) = extract_status_and_trailers(trailers)?;

                                // Send trailing metadata
                                writer.send_trailing_metadata(metadata).await?;

                                if let Some(status) = status {
                                    if status.code() != StatusCode::Ok {
                                        return Err(status);
                                    }
                                }
                                return Ok(());
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    return Err(Status::new(StatusCode::Internal, e.into().to_string()));
                }
                None => {
                    // End of stream
                    // Check if we have any partial data left
                    if self.decoder.buffer_len() > 0 {
                        return Err(Status::new(StatusCode::Internal, "Partial gRPC message"));
                    }
                    // Send empty trailers if none received
                    writer.send_trailing_metadata(Metadata::default()).await?;
                    return Ok(());
                }
            }
        }
    }
}

impl<B> PushStreamProducer for HyperStreamProducer<B>
where
    B: Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    type Item = Incoming<BufList<Bytes>>;

    async fn produce(
        self,
        writer: PushStreamWriter<Self::Item, impl PushStreamConsumer<Item = Self::Item>>,
    ) -> Result<(), Status> {
        let adapter = RequestBodyWriter { inner: writer };
        self.produce_to_writer(adapter).await
    }
}

struct RequestBodyWriter<T, C> {
    inner: PushStreamWriter<T, C>,
}

impl<T, C> StreamingResponseBodyWriter<T> for RequestBodyWriter<T, C>
where
    T: Send + 'static,
    C: PushStreamConsumer<Item = T>,
{
    async fn write(&mut self, item: T) -> Result<(), Status> {
        self.inner.write(item).await
    }

    async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
        // No-op for requests
        Ok(())
    }
}

fn extract_status_and_trailers(
    mut trailers: HeaderMap,
) -> Result<(Option<Status>, Metadata), Status> {
    let mut status = None;

    if let Some(s) = trailers.remove("grpc-status") {
        let code_str = s
            .to_str()
            .map_err(|_| Status::new(StatusCode::Internal, "Invalid grpc-status header value"))?;
        let code_int: i32 = code_str
            .parse()
            .map_err(|_| Status::new(StatusCode::Internal, "Invalid grpc-status header value"))?;
        // Map HTTP status codes to gRPC status codes
        // https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md
        let code = super::status::infer_grpc_status_from_http_status(code_int);

        let message = if let Some(m) = trailers.remove("grpc-message") {
            m.to_str()
                .map_err(|_| {
                    Status::new(StatusCode::Internal, "Invalid grpc-message header value")
                })?
                .to_string()
        } else {
            String::new()
        };

        status = Some(Status::new(code, message));
    }

    Ok((status, Metadata::new(trailers)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::Metadata;
    use bytes::Bytes;
    use http::HeaderMap;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    struct MockBody {
        frames: Vec<Result<hyper::body::Frame<Bytes>, Status>>,
    }

    impl Body for MockBody {
        type Data = Bytes;
        type Error = Status;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            if self.frames.is_empty() {
                return Poll::Ready(None);
            }
            Poll::Ready(Some(self.frames.remove(0)))
        }
    }

    #[derive(Clone)]
    struct MockWriter {
        state: Arc<Mutex<MockWriterState>>,
    }

    struct MockWriterState {
        items: Vec<Incoming<BufList<Bytes>>>,
        trailers: Option<Metadata>,
    }

    impl MockWriter {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(MockWriterState {
                    items: Vec::new(),
                    trailers: None,
                })),
            }
        }
    }

    impl StreamingResponseBodyWriter<Incoming<BufList<Bytes>>> for MockWriter {
        async fn write(&mut self, item: Incoming<BufList<Bytes>>) -> Result<(), Status> {
            self.state.lock().unwrap().items.push(item);
            Ok(())
        }

        async fn send_trailing_metadata(self, trailers: Metadata) -> Result<(), Status> {
            self.state.lock().unwrap().trailers = Some(trailers);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_produce_response_success() {
        let mut frames = Vec::new();
        // Header + Body: 5 bytes header + "hello"
        let mut data = Vec::new();
        data.extend_from_slice(&[0, 0, 0, 0, 5]);
        data.extend_from_slice(b"hello");
        frames.push(Ok(hyper::body::Frame::data(Bytes::from(data))));

        // Trailers: grpc-status: 200 (HTTP OK -> gRPC OK)
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "200".parse().unwrap());
        frames.push(Ok(hyper::body::Frame::trailers(trailers)));

        let body = MockBody { frames };
        let producer = HyperStreamProducer::new(body);
        let writer = MockWriter::new();
        let state = writer.state.clone();

        let res = producer.produce_to_writer(writer).await;
        assert!(res.is_ok());

        let state = state.lock().unwrap();
        assert_eq!(state.items.len(), 1);
        assert!(state.trailers.is_some());
    }

    #[tokio::test]
    async fn test_produce_response_error() {
        let mut frames = Vec::new();
        // Trailers: grpc-status: 404 (HTTP Not Found -> gRPC Unimplemented)
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "404".parse().unwrap());
        frames.push(Ok(hyper::body::Frame::trailers(trailers)));

        let body = MockBody { frames };
        let producer = HyperStreamProducer::new(body);
        let writer = MockWriter::new();

        let res = producer.produce_to_writer(writer).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), StatusCode::Unimplemented);
    }

    #[tokio::test]
    async fn test_produce_request_no_trailers() {
        let mut frames = Vec::new();
        // Just data, then None
        let mut data = Vec::new();
        data.extend_from_slice(&[0, 0, 0, 0, 5]);
        data.extend_from_slice(b"hello");
        frames.push(Ok(hyper::body::Frame::data(Bytes::from(data))));

        let body = MockBody { frames };
        let producer = HyperStreamProducer::new(body);
        let writer = MockWriter::new();
        let state = writer.state.clone();

        let res = producer.produce_to_writer(writer).await;
        assert!(res.is_ok());

        let state = state.lock().unwrap();
        assert_eq!(state.items.len(), 1);
        // Should have default trailers (empty)
        assert!(state.trailers.is_some());
    }

    #[tokio::test]
    async fn test_produce_partial_data() {
        let mut frames = Vec::new();
        // Incomplete header (3 bytes)
        let mut data = Vec::new();
        data.extend_from_slice(&[0, 0, 0]);
        frames.push(Ok(hyper::body::Frame::data(Bytes::from(data))));

        let body = MockBody { frames };
        let producer = HyperStreamProducer::new(body);
        let writer = MockWriter::new();

        let res = producer.produce_to_writer(writer).await;
        assert!(res.is_err());
        let err = res.unwrap_err();
        assert_eq!(err.code(), StatusCode::Internal);
        assert_eq!(err.message(), "Partial gRPC message");
    }
}
