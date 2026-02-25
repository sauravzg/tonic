use super::buffer::BufList;
use super::encoder::{HyperEncoderBody, HyperResponseWriter};
use super::producer::HyperStreamProducer;
use crate::server::call::{HandlerCallOptions, Metadata, StreamingRequest};
use crate::server::method_handler::ByteStreamMethodHandler;
use crate::server::stream::PushStream;
use crate::Status;
use bytes::{Buf, Bytes};
use hyper::body::Incoming;
use hyper::{Request, Response};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// A `hyper::service::Service` implementation that delegates to a Tonic handler.
pub struct HyperGrpcService<H, RespB> {
    handler: Arc<H>,
    _phantom: PhantomData<RespB>,
}

impl<H, RespB> HyperGrpcService<H, RespB> {
    /// Creates a new `HyperService`.
    pub fn new(handler: Arc<H>) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<H, RespB> hyper::service::Service<Request<Incoming>> for HyperGrpcService<H, RespB>
where
    H: ByteStreamMethodHandler<
            BufList<Bytes>,
            HyperResponseWriter<RespB>,
            HyperStreamProducer<Incoming>,
            RespB = RespB,
        > + Send
        + Sync
        + 'static,
    RespB: Buf + Send + 'static,
{
    type Response = Response<HyperEncoderBody<RespB>>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let handler = self.handler.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();
            let mut metadata = Metadata::new(parts.headers);
            if let Ok(path) = parts.uri.path().parse() {
                metadata.inner.insert("path", path);
            }

            let options = HandlerCallOptions::default();

            let producer = HyperStreamProducer::new(body);
            let push_stream = PushStream::new(producer);
            let streaming_req = StreamingRequest::new(push_stream, metadata);

            let (headers_tx, headers_rx) = oneshot::channel();
            let (body_tx, body_rx) = mpsc::channel(1);
            let (trailers_tx, trailers_rx) = oneshot::channel();
            let (status_tx, status_rx) = oneshot::channel(); // Handler status

            let writer = HyperResponseWriter::new(headers_tx, body_tx, trailers_tx);

            // Spawn the handler call
            tokio::spawn(async move {
                let result = handler.call(options, streaming_req, writer).await;
                let status = match result {
                    Ok(_) => Status::new(crate::status::StatusCode::Ok, ""),
                    Err(e) => e,
                };
                let _ = status_tx.send(status);
            });

            // Wait for headers (or implicit default)
            let headers = headers_rx.await.unwrap_or_default();

            let response_body = HyperEncoderBody::new(body_rx, status_rx, trailers_rx);
            let mut res = Response::new(response_body);
            *res.headers_mut() = headers.inner;
            res.headers_mut().insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/grpc"),
            );
            Ok(res)
        })
    }
}
