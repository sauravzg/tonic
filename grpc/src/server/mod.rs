/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use std::sync::Arc;

use tokio::sync::oneshot;

use crate::core::RecvMessage;
use crate::core::RequestHeaders;
use crate::core::ServerResponseStreamItem;
use crate::service::Request;
use crate::service::Response;
use crate::service::Service;

pub mod message;
pub mod stream;

mod codegen_api;
pub use codegen_api::{
    BidiStreamingMethod, ClientStreamingMethod, ServerStreamingMethod, UnaryMethod,
};

pub(crate) mod call;

pub(crate) mod interceptor;
pub(crate) mod method_handler;
pub(crate) mod transport;
pub(crate) mod router;
pub mod service;

mod builder;
pub use builder::{RouterBuilder, ServerBuilder};

use crate::server::router::Router;

use transport::ServerTransport;

use crate::Status;
use std::future::Future;

/// A gRPC server (V2).
pub struct ServerV2<
    T,
    RespB,
    F = crate::server::interceptor::NoopInterceptor,
    BSF = crate::server::interceptor::NoopInterceptor,
> where
    T: ServerTransport,
    RespB: bytes::Buf + Send + 'static,
{
    transport: T,
    router: Router<T::ReqB, RespB, T::Writer<RespB>, T::Producer, F, BSF>,
}

impl<T, RespB, F, BSF> ServerV2<T, RespB, F, BSF>
where
    T: ServerTransport,
    T::Writer<RespB>: crate::server::call::StreamingResponseWriter<RespB>,
    RespB: bytes::Buf + Send + 'static,
    F: crate::server::interceptor::InterceptorFactory,
    BSF: crate::server::interceptor::ByteStreamInterceptorFactory,
{
    /// Serves the gRPC server.
    pub fn serve<L>(self, listener: L) -> impl Future<Output = Result<(), Status>> + Send
    where
        L: crate::server::transport::listener::Listener + Send,
        T::ReqB: bytes::Buf + Send + 'static,
        T::Writer<RespB>: Send + 'static,
        T::Producer: Send + Sync + 'static,
    {
        self.transport.serve(listener, self.router)
    }
}

pub struct Server {
    handler: Option<Arc<dyn Service>>,
}

pub type Call = (String, Request, oneshot::Sender<Response>);

#[tonic::async_trait]
pub trait Listener {
    async fn accept(&self) -> Option<Call>;
}

impl Server {
    pub fn new() -> Self {
        Self { handler: None }
    }

    pub fn set_handler(&mut self, f: impl Service + 'static) {
        self.handler = Some(Arc::new(f))
    }

    pub async fn serve(&self, l: &impl Listener) {
        while let Some((method, req, reply_on)) = l.accept().await {
            reply_on
                .send(self.handler.as_ref().unwrap().call(method, req).await)
                .ok(); // TODO: log error
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

/// A trait which may be implemented by types to handle server-side logic of
/// RPCs (Remote Procedure Calls, often shortened to "call").
#[trait_variant::make(Send)]
pub trait Handle: Send + Sync {
    /// Handles an RPC, accepting the send and receive streams that are used to
    /// interact with the call.  Note that `tx` is not static, so it cannot be
    /// sent to another task, meaning the RPC must end before handle returns.
    async fn handle(
        &self,
        _method: String,
        _headers: RequestHeaders,
        tx: &impl SendStream,
        rx: impl RecvStream + 'static,
    );
}

/// Represents the sending side of a server stream.  See `ResponseStream`
/// documentation for information about the different types of items and the
/// order in which they must be sent.
#[trait_variant::make(Send)]
pub trait SendStream {
    /// Sends the next item on the stream.
    ///
    /// # Cancel safety
    ///
    /// This method is not intended to be cancellation safe.  If the returned
    /// future is not polled to completion, the behavior of any subsequent calls
    /// to the SendStream are undefined and data may be lost.
    async fn send(
        &mut self,
        item: ServerResponseStreamItem,
        options: SendOptions,
    ) -> Result<(), ()>;
}

/// Contains settings to configure a send operation on a SendStream.
#[derive(Default)]
#[non_exhaustive]
pub struct SendOptions {
    /// Delays sending the message until the trailers are provided on the stream
    /// and batches the two items together if possible.
    pub final_msg: bool,
    /// If set, compression will be disabled for this message.
    pub disable_compression: bool,
}

/// Represents the receiving side of a server stream.
#[trait_variant::make(Send)]
pub trait RecvStream {
    /// Returns the next message on the stream.  If an error is returned, the
    /// stream ended or the client closed the send side of the request stream.
    ///
    /// # Cancel safety
    ///
    /// This method is not intended to be cancellation safe.  If the returned
    /// future is not polled to completion, the behavior of any subsequent calls
    /// to the RecvStream are undefined and data may be lost.
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Result<(), ()>;
}
