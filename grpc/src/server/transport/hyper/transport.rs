use super::buffer::BufList;
use super::encoder::HyperResponseWriter;

use crate::server::method_handler::ByteStreamMethodHandler;
use crate::server::transport::listener::Listener;
use crate::server::transport::server_transport::ServerTransport;
use crate::Status;
use bytes::{Buf, Bytes};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::sync::Arc;

/// A `ServerTransport` implementation based on `hyper`.
#[derive(Debug, Clone, Default)]
pub struct HyperTransport;

impl HyperTransport {
    /// Creates a new `HyperTransport`.
    pub fn new() -> Self {
        Self
    }
}

impl ServerTransport for HyperTransport {
    type ReqB = BufList<Bytes>;
    type Writer<RespB: Buf + Send + 'static> = HyperResponseWriter<RespB>;
    type Producer = super::producer::HyperStreamProducer<hyper::body::Incoming>;

    async fn serve<L, H, RespB>(self, mut listener: L, handler: H) -> Result<(), Status>
    where
        L: Listener + Send,
        H: ByteStreamMethodHandler<Self::ReqB, Self::Writer<RespB>, Self::Producer, RespB = RespB>
            + Send
            + Sync
            + 'static,
        RespB: Buf + Send + 'static,
    {
        let handler = Arc::new(handler);
        loop {
            match listener.accept().await {
                Ok((io, _addr)) => {
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        use super::service::HyperGrpcService;
                        let service = HyperGrpcService::new(handler);
                        let io = TokioIo::new(io);
                        let builder = Builder::new(TokioExecutor::new());

                        if let Err(e) = builder.serve_connection(io, service).await {
                            // TODO: Log error
                            let _ = e;
                        }
                    });
                }
                Err(e) => {
                    // TODO: Handle accept error (backoff, log, etc.)
                    return Err(Status::new(
                        crate::status::StatusCode::Internal,
                        e.to_string(),
                    ));
                }
            }
        }
    }
}
