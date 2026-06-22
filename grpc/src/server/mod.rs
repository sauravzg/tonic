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

use std::future::Future;
use std::sync::Arc;
use tonic::async_trait;

use crate::client::CallOptions;
use crate::core::RecvMessage;
use crate::core::RequestHeaders;
use crate::core::ServerResponseStreamItem;
use crate::core::Trailers;
use crate::rt::GrpcRuntime;

pub mod builder;
pub mod descriptor;
pub mod interceptor;
pub(crate) mod router;
pub mod service;
pub mod transport;

pub struct Server {
    handler: Option<Arc<dyn DynHandle>>,
    runtime: GrpcRuntime,
    // Future: shutdown_signal, max_connection_age, etc.
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for crate::server::transport::hyper::HyperTransport {}
    impl Sealed for crate::server::transport::hyper::HyperConnection {}
    impl Sealed for crate::inmemory::InMemoryListener {}
    impl Sealed for crate::inmemory::InMemoryServerCall {}
}

/// A bound listening socket that yields incoming connections.
///
/// **Sealed** — external crates cannot implement this trait.
#[trait_variant::make(Send)]
pub trait Listener: sealed::Sealed {
    /// The concrete transport type yielded by this listener.
    type Transport: Transport + 'static;

    /// Accepts the next incoming connection.
    async fn accept(&self) -> Option<Self::Transport>;

    /// Returns the local address this listener is bound to.
    ///
    /// Returns `Err` if the underlying transport does not use IP/port addresses
    /// (e.g., Unix domain sockets or In-Memory transport).
    fn local_addr(&self) -> Result<std::net::SocketAddr, String>;
}

/// A connection accepted by a [`Listener`] that can serve RPCs.
///
/// **Sealed** — external crates cannot implement this trait.
#[trait_variant::make(Send)]
pub trait Transport: sealed::Sealed {
    /// Wires the connection to the application handler and drives the execution.
    ///
    /// If `shutdown` is provided and its value changes, the connection should
    /// initiate graceful shutdown (e.g., send HTTP/2 GOAWAY) and finish
    /// processing in-flight RPCs before returning.
    async fn serve(
        self,
        handler: Arc<dyn DynHandle>,
        runtime: GrpcRuntime,
        shutdown: Option<tokio::sync::watch::Receiver<()>>,
    );
}

impl Server {
    /// Creates a [`ServerBuilder`](builder::ServerBuilder) with no interceptors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let server = Server::builder()
    ///     .add_service(greeter_service)
    ///     .build();
    /// ```
    pub fn builder() -> builder::ServerBuilder {
        builder::ServerBuilder::new()
    }

    /// Creates a [`ServerBuilder`](builder::ServerBuilder) with the given
    /// interceptor chain.
    ///
    /// All services registered through the builder will have their method
    /// handlers wrapped with `interceptor` before type erasure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use grpc::server::interceptor::ComposedIntercept;
    /// let chain = ComposedIntercept::new(logging, auth);
    /// let server = Server::builder_with(chain)
    ///     .add_service(greeter_service)
    ///     .build();
    /// ```
    pub fn builder_with<I: interceptor::Intercept + Clone + Send + Sync + 'static>(
        interceptor: I,
    ) -> builder::ServerBuilder<I> {
        builder::ServerBuilder::new_with_interceptor(interceptor)
    }

    /// Creates a new server with no handler.
    ///
    /// For most use cases, prefer [`Server::builder()`] which provides
    /// a fluent API with interceptor support.
    pub fn new() -> Self {
        Self {
            handler: None,
            runtime: crate::rt::default_runtime(),
        }
    }

    /// Sets the RPC handler for this server.
    ///
    /// For most use cases, prefer [`Server::builder()`] which handles
    /// handler registration via [`add_service()`](builder::ServerBuilder::add_service).
    pub fn set_handler<H>(&mut self, h: H)
    where
        H: Handle + Send + Sync + 'static,
    {
        self.handler = Some(Arc::new(h))
    }

    /// Serves on the given listener until it stops producing connections.
    ///
    /// After the accept loop ends, waits for all in-flight connections to
    /// drain before returning. Connections are not notified to shut down
    /// gracefully; they run until completion or client disconnect.
    pub async fn serve(&self, listener: &impl Listener) {
        let (drain_tx, drain_rx) = tokio::sync::watch::channel(());

        while let Some(connection) = listener.accept().await {
            let handler = match self.handler.as_ref() {
                Some(h) => h.clone(),
                None => continue,
            };
            let rx = drain_rx.clone();
            let rt = self.runtime.clone();
            self.runtime.spawn(Box::pin(async move {
                connection.serve(handler, rt, None).await;
                drop(rx);
            }));
        }

        drop(drain_rx);
        drain_tx.closed().await;
    }

    /// Serves on the given listener until `signal` resolves, then drains
    /// all in-flight connections before returning.
    ///
    /// When `signal` completes:
    /// 1. The server stops accepting new connections.
    /// 2. All open HTTP/2 connections receive a GOAWAY frame.
    /// 3. In-flight RPCs continue to completion.
    /// 4. This method returns once all connections are closed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// server.serve_with_shutdown(&listener, async {
    ///     tokio::signal::ctrl_c().await.ok();
    /// }).await;
    /// ```
    pub async fn serve_with_shutdown(
        &self,
        listener: &impl Listener,
        signal: impl Future<Output = ()>,
    ) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

        tokio::pin!(signal);

        loop {
            tokio::select! {
                conn = listener.accept() => {
                    let Some(connection) = conn else { break };
                    let handler = match self.handler.as_ref() {
                        Some(h) => h.clone(),
                        None => continue,
                    };
                    let rx = shutdown_rx.clone();
                    let rt = self.runtime.clone();
                    self.runtime.spawn(Box::pin(async move {
                        connection.serve(handler, rt, Some(rx)).await;
                    }));
                }
                _ = &mut signal => {
                    // Broadcast shutdown to all connections.
                    let _ = shutdown_tx.send(());
                    break;
                }
            }
        }

        // Wait for all connections to drain.
        drop(shutdown_rx);
        shutdown_tx.closed().await;
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
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut impl SendStream,
        rx: impl RecvStream + 'static,
    ) -> Trailers;
}

#[async_trait]
pub trait DynHandle: Send + Sync {
    async fn dyn_handle(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut dyn DynSendStream,
        rx: BoxedRecvStream,
    ) -> Trailers;
}

#[async_trait]
impl<T: Handle> DynHandle for T {
    async fn dyn_handle(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        mut tx: &mut dyn DynSendStream,
        rx: BoxedRecvStream,
    ) -> Trailers {
        self.handle(headers, options, &mut tx, rx).await
    }
}

// TODO: delete this type which is only needed pre-rust v1.92 due to a bug
// handling lifetimes:
//
// error: implementation of `server::RecvStream` is not general enough
//    --> grpc/src/server/mod.rs:108:5
//     |
// 108 |     async fn dyn_handle(
//     |     ^^^^^ implementation of `server::RecvStream` is not general enough
//     |
//     = note: `Box<(dyn server::DynRecvStream + '0)>` must implement `server::RecvStream`, for any lifetime `'0`...
//     = note: ...but `server::RecvStream` is actually implemented for the type `Box<(dyn server::DynRecvStream + 'static)>`
pub struct BoxedRecvStream(pub Box<dyn DynRecvStream + 'static>);

// Implement RecvStream for the wrapper instead of the Box directly
impl RecvStream for BoxedRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
        self.0.dyn_next(msg).await
    }
}

/// Bridges [`DynHandle`] (object-safe) back to [`Handle`] (generic),
/// enabling interceptor composition on top of a dynamic handler.
pub(crate) struct DynHandleWrapper(pub Arc<dyn DynHandle>);

impl Handle for DynHandleWrapper {
    async fn handle(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut impl SendStream,
        rx: impl RecvStream + 'static,
    ) -> Trailers {
        self.0
            .dyn_handle(headers, options, tx, BoxedRecvStream(Box::new(rx)))
            .await
    }
}

/// Represents the sending side of a server stream.  See `ResponseStream`
/// documentation for information about the different types of items and the
/// order in which they must be sent.
#[trait_variant::make(Send)]
pub trait SendStream {
    /// Sends the next item on the stream. Returns `Ok(())` on success, or
    /// `Err(())` on failure. `Err(())` is a terminal state.
    /// Calling this method after an error should be avoided and is unspecified.
    ///
    /// # Cancel safety
    ///
    /// This method is not intended to be cancellation safe.  If the returned
    /// future is not polled to completion, the behavior of any subsequent calls
    /// to the SendStream are undefined and data may be lost.
    async fn send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        options: SendOptions,
    ) -> Result<(), ()>;
}

#[async_trait]
pub trait DynSendStream: Send {
    async fn dyn_send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        options: SendOptions,
    ) -> Result<(), ()>;
}

#[async_trait]
impl<T: SendStream> DynSendStream for T {
    async fn dyn_send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        options: SendOptions,
    ) -> Result<(), ()> {
        self.send(item, options).await
    }
}

impl<'b> SendStream for &mut (dyn DynSendStream + 'b) {
    async fn send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        options: SendOptions,
    ) -> Result<(), ()> {
        (**self).dyn_send(item, options).await
    }
}

impl<'b> SendStream for Box<dyn DynSendStream + 'b> {
    async fn send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        options: SendOptions,
    ) -> Result<(), ()> {
        (**self).dyn_send(item, options).await
    }
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
    /// Returns the next message on the stream. Returns `Some(Ok(()))` on
    /// success, `None` on normal stream end, or `Some(Err(()))` if the stream
    /// encountered an error before the client's final request message. Both
    /// `None` and `Some(Err(()))` are terminal states.
    /// Calling this method again after reaching a terminal state is unspecified
    /// and should be avoided.
    ///
    /// # Cancel safety
    ///
    /// This method is not intended to be cancellation safe.  If the returned
    /// future is not polled to completion, the behavior of any subsequent calls
    /// to the RecvStream are undefined and data may be lost.
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>>;
}

#[async_trait]
pub trait DynRecvStream: Send {
    async fn dyn_next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>>;
}

#[async_trait]
impl<T: RecvStream> DynRecvStream for T {
    async fn dyn_next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
        self.next(msg).await
    }
}

impl<'a> RecvStream for Box<dyn DynRecvStream + 'a> {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
        (**self).dyn_next(msg).await
    }
}
