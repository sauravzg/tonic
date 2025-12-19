use super::buffer::GrpcLengthEncodedBuf;
use crate::server::call::{Metadata, StreamingResponseBodyWriter, StreamingResponseWriter};
use crate::Status;
use bytes::Buf;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{mpsc, oneshot};

struct TrailersJoinState {
    status_rx: oneshot::Receiver<Status>,
    trailers_rx: oneshot::Receiver<Metadata>,
    status: Option<Status>,
    trailers: Option<Metadata>,
}

/// A `hyper::body::Body` implementation that polls a channel for gRPC messages.
pub struct HyperEncoderBody<B> {
    rx: mpsc::Receiver<B>,
    join_state: TrailersJoinState,
}

impl<B> HyperEncoderBody<B> {
    /// Creates a new `HyperEncoderBody`.
    pub fn new(
        rx: mpsc::Receiver<B>,
        status_rx: oneshot::Receiver<Status>,
        trailers_rx: oneshot::Receiver<Metadata>,
    ) -> Self {
        Self {
            rx,
            join_state: TrailersJoinState {
                status_rx,
                trailers_rx,
                status: None,
                trailers: None,
            },
        }
    }
}

impl<B: Buf + Send + 'static> hyper::body::Body for HyperEncoderBody<B> {
    type Data = GrpcLengthEncodedBuf<B>;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        // 1. Poll Body
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                let len = msg.remaining();
                let mut header = [0u8; 5];
                // Compression flag (0 for now)
                header[0] = 0;
                // Message length (big endian)
                let len_bytes = (len as u32).to_be_bytes();
                header[1..5].copy_from_slice(&len_bytes);

                let buf = GrpcLengthEncodedBuf::new(header, msg);
                return Poll::Ready(Some(Ok(hyper::body::Frame::data(buf))));
            }
            Poll::Ready(None) => {
                // Body exhausted, proceed to trailers
            }
            Poll::Pending => return Poll::Pending,
        }

        // 2. Poll Trailers (Join)
        let state = &mut self.join_state;

        // Poll status
        if state.status.is_none() {
            if let Poll::Ready(status_res) = Pin::new(&mut state.status_rx).poll(cx) {
                // If the sender dropped, we default to unknown/internal error, but ideally service should always send.
                state.status = Some(status_res.unwrap_or_else(|_| {
                    Status::new(
                        crate::status::StatusCode::Internal,
                        "Handler dropped status channel without waiting",
                    )
                }));
            }
        }

        // Poll trailers
        if state.trailers.is_none() {
            if let Poll::Ready(trailers_res) = Pin::new(&mut state.trailers_rx).poll(cx) {
                // If sender dropped, it means no custom trailers.
                state.trailers = Some(trailers_res.unwrap_or_default());
            }
        }

        if let (Some(status), Some(trailers)) = (&state.status, &state.trailers) {
            match status_to_header_map(status, trailers) {
                Ok(headers) => Poll::Ready(Some(Ok(hyper::body::Frame::trailers(headers)))),
                Err(e) => Poll::Ready(Some(Err(e))),
            }
        } else {
            // Still waiting for either status or trailers
            Poll::Pending
        }
    }
}

fn status_to_header_map(status: &Status, trailers: &Metadata) -> Result<http::HeaderMap, Status> {
    let mut map = trailers.clone().inner;
    map.insert(
        "grpc-status",
        http::HeaderValue::from_str(&(status.code() as i32).to_string())
            .map_err(|_| Status::new(crate::status::StatusCode::Internal, "Invalid status code"))?,
    );
    if !status.message().is_empty() {
        // TODO: Handle percent encoding for non-ASCII characters
        map.insert(
            "grpc-message",
            http::HeaderValue::from_str(status.message()).map_err(|_| {
                Status::new(
                    crate::status::StatusCode::Internal,
                    "Invalid status message",
                )
            })?,
        );
    }
    Ok(map)
}

/// A `StreamingResponseWriter` that sends gRPC messages to a channel.
pub struct HyperResponseWriter<B> {
    headers_tx: Option<oneshot::Sender<Metadata>>,
    body_tx: mpsc::Sender<B>,
    trailers_tx: Option<oneshot::Sender<Metadata>>,
}

impl<B> HyperResponseWriter<B> {
    /// Creates a new `HyperResponseWriter`.
    pub fn new(
        headers_tx: oneshot::Sender<Metadata>,
        body_tx: mpsc::Sender<B>,
        trailers_tx: oneshot::Sender<Metadata>,
    ) -> Self {
        Self {
            headers_tx: Some(headers_tx),
            body_tx,
            trailers_tx: Some(trailers_tx),
        }
    }
}

impl<B: Buf + Send + 'static> StreamingResponseWriter<B> for HyperResponseWriter<B> {
    type BodyWriter = HyperBodyWriter<B>;

    async fn send_initial_metadata(
        mut self,
        metadata: Metadata,
    ) -> Result<Self::BodyWriter, Status> {
        if let Some(tx) = self.headers_tx.take() {
            let _ = tx.send(metadata);
        }

        Ok(HyperBodyWriter {
            body_tx: self.body_tx,
            trailers_tx: self.trailers_tx,
        })
    }
}

pub struct HyperBodyWriter<B> {
    body_tx: mpsc::Sender<B>,
    trailers_tx: Option<oneshot::Sender<Metadata>>,
}

impl<B: Buf + Send + 'static> StreamingResponseBodyWriter<B> for HyperBodyWriter<B> {
    async fn write(&mut self, message: B) -> Result<(), Status> {
        self.body_tx.send(message).await.map_err(|_| {
            Status::new(
                crate::status::StatusCode::Internal,
                "Response receiver dropped",
            )
        })
    }

    async fn send_trailing_metadata(mut self, metadata: Metadata) -> Result<(), Status> {
        if let Some(tx) = self.trailers_tx.take() {
            let _ = tx.send(metadata);
        }
        Ok(())
    }
}
