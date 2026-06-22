/*
 *
 * Copyright 2026 gRPC authors.
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

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Buf, Bytes};
use tokio::sync::{mpsc, oneshot};

use crate::client::CallOptions;
use crate::core::{
    RecvMessage, RequestHeaders, ResponseHeaders, ServerResponseStreamItem, Trailers,
};
use crate::metadata::MetadataMap;
use crate::credentials::dyn_wrapper::DynServerCredentials;
use crate::credentials::ServerCredentials;
use crate::rt::hyper_wrapper::{HyperCompatExec, HyperStream};
use crate::rt::{EndpointListener, GrpcEndpoint, GrpcRuntime};
use crate::server::{
    BoxedRecvStream, DynHandle, DynHandleWrapper, Handle,
    Listener, RecvStream, SendOptions, SendStream, Transport,
};
use crate::server::interceptor::HandleExt;
use crate::server::interceptor::http2_framing::{DeframeConfig, GrpcFramingInterceptor};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use crate::status::{StatusCodeError, StatusError};

// ---------------------------------------------------------------------------
// ByteSourceRecvStream
// ---------------------------------------------------------------------------

/// A [`RecvStream`] that yields raw `Bytes` chunks from an HTTP body.
/// Each `next()` call reads one HTTP/2 DATA frame and decodes the bytes
/// into the provided [`RecvMessage`].
///
/// This is the lowest-level recv stream in the transport — it does not
/// understand gRPC framing. It is meant to be consumed by the
/// `GrpcFramingInterceptor` which wraps it in a `DeframingRecvStream`.
///
/// Generic over `B: Body` to support both `hyper::body::Incoming` in
/// production and mock bodies in tests.
pub(crate) struct ByteSourceRecvStream<B> {
    body: B,
}

impl<B> ByteSourceRecvStream<B> {
    pub(crate) fn new(body: B) -> Self {
        Self { body }
    }
}

impl<B> RecvStream for ByteSourceRecvStream<B>
where
    B: http_body::Body<Data = Bytes> + Unpin + Send + 'static,
    B::Error: std::fmt::Debug,
{
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
        use http_body_util::BodyExt;
        loop {
            match Pin::new(&mut self.body).frame().await {
                Some(Ok(frame)) => {
                    if let Ok(mut data) = frame.into_data() {
                        if data.is_empty() {
                            // Empty DATA frames are valid per gRPC/HTTP2 spec.
                            // All major implementations (Go, Java, C-core) accept
                            // them. They are commonly used for END_STREAM signaling.
                            // TODO: Consider abuse protection — C-core counts
                            // excessive empty DATA frames without END_STREAM and
                            // rejects after a threshold.
                            continue;
                        }
                        // Pass the Bytes directly so that copy_to_bytes()
                        // inside RecvMessage::decode() is O(1).
                        return Some(msg.decode(&mut data).map_err(|_| ()));
                    }
                    // Hyper only yields DATA and trailer HEADERS via
                    // Body::frame(). All other HTTP/2 frame types
                    // (WINDOW_UPDATE, RST_STREAM, etc.) are handled
                    // internally by h2. A trailer HEADERS frame on the
                    // request stream is invalid gRPC semantics — treat
                    // as a protocol error.
                    return Some(Err(()));
                }
                Some(Err(_)) => return Some(Err(())),
                None => return None, // Body exhausted.
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HttpSendStream
// ---------------------------------------------------------------------------

/// The transport-level [`SendStream`] for the Hyper transport.
///
/// - `Headers`: Sends `ResponseHeaders` via a oneshot channel to `serve()`,
///   which uses them to build the HTTP/2 response headers.
/// - `Message`: Encodes and pushes bytes via an mpsc channel to
///   `GrpcResponseBody`, which yields them as HTTP/2 DATA frames.
pub(crate) struct HttpSendStream {
    headers_tx: Option<oneshot::Sender<ResponseHeaders>>,
    body_tx: mpsc::Sender<Bytes>,
}

impl HttpSendStream {
    fn new(
        headers_tx: oneshot::Sender<ResponseHeaders>,
        body_tx: mpsc::Sender<Bytes>,
    ) -> Self {
        Self {
            headers_tx: Some(headers_tx),
            body_tx,
        }
    }
}

impl SendStream for HttpSendStream {
    async fn send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        _options: SendOptions,
    ) -> Result<(), ()> {
        match item {
            ServerResponseStreamItem::Headers(h) => {
                match self.headers_tx.take() {
                    Some(tx) => tx.send(h).map_err(|_| ()),
                    None => Err(()), // Headers already sent.
                }
            }
            ServerResponseStreamItem::Message(msg) => {
                let mut buf = msg.encode().map_err(|_| ())?;
                let bytes = buf.copy_to_bytes(buf.remaining());
                self.body_tx.send(bytes).await.map_err(|_| ())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GrpcResponseBody
// ---------------------------------------------------------------------------

/// A custom [`http_body::Body`] that yields gRPC response data frames
/// followed by trailing headers containing the gRPC status.
///
/// - Phase 1: Polls `body_rx` for data → yields `Frame::data(bytes)`.
/// - Phase 2: When `body_rx` closes, polls `trailers_rx` → yields
///   `Frame::trailers(header_map)` with `grpc-status`, etc.
pub(crate) struct GrpcResponseBody {
    body_rx: mpsc::Receiver<Bytes>,
    trailers_rx: Option<oneshot::Receiver<Trailers>>,
    data_done: bool,
}

impl GrpcResponseBody {
    fn new(
        body_rx: mpsc::Receiver<Bytes>,
        trailers_rx: oneshot::Receiver<Trailers>,
    ) -> Self {
        Self {
            body_rx,
            trailers_rx: Some(trailers_rx),
            data_done: false,
        }
    }
}

impl http_body::Body for GrpcResponseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();

        // Phase 1: Yield data frames.
        if !this.data_done {
            match this.body_rx.poll_recv(cx) {
                Poll::Ready(Some(bytes)) => {
                    return Poll::Ready(Some(Ok(http_body::Frame::data(bytes))));
                }
                Poll::Ready(None) => {
                    this.data_done = true;
                    // Fall through to trailers.
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Phase 2: Yield trailers.
        if let Some(mut rx) = this.trailers_rx.take() {
            match Pin::new(&mut rx).poll(cx) {
                Poll::Ready(Ok(trailers)) => {
                    let header_map = trailers_to_header_map(&trailers);
                    Poll::Ready(Some(Ok(http_body::Frame::trailers(header_map))))
                }
                Poll::Ready(Err(_)) => {
                    // Handler dropped without sending trailers.
                    let mut map = http::HeaderMap::new();
                    map.insert(
                        "grpc-status",
                        (StatusCodeError::Internal as i32).to_string().parse().expect("valid header value"),
                    );
                    map.insert(
                        "grpc-message",
                        "handler did not return trailers".parse().expect("valid header value"),
                    );
                    Poll::Ready(Some(Ok(http_body::Frame::trailers(map))))
                }
                Poll::Pending => {
                    this.trailers_rx = Some(rx);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(None) // All done.
        }
    }
}

/// Converts gRPC [`Trailers`] to an HTTP `HeaderMap` for trailing headers.
fn trailers_to_header_map(trailers: &Trailers) -> http::HeaderMap {
    let mut map = http::HeaderMap::new();

    match trailers.status() {
        Ok(()) => {
            // grpc-status: 0 (OK)
            map.insert("grpc-status", "0".parse().expect("valid header value"));
        }
        Err(err) => {
            // grpc-status: <code>
            let code = err.code() as i32;
            map.insert(
                "grpc-status",
                code.to_string().parse().expect("valid header value"),
            );
            // grpc-message: <message> (percent-encoded per gRPC spec)
            let msg = err.message();
            if !msg.is_empty() {
                let encoded = percent_encode(msg.as_bytes(), NON_ALPHANUMERIC).to_string();
                if let Ok(val) = encoded.parse() {
                    map.insert("grpc-message", val);
                }
            }
        }
    }

    // Custom trailer metadata.
    let trailer_headers = trailers.metadata().clone().into_headers();
    for (key, value) in trailer_headers.iter() {
        map.insert(key.clone(), value.clone());
    }

    map
}

// ---------------------------------------------------------------------------
// Content-type validation
// ---------------------------------------------------------------------------

/// Validates that the request has a gRPC-compatible content-type.
fn validate_content_type(
    req: &http::Request<hyper::body::Incoming>,
) -> Result<(), StatusError> {
    match req
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
    {
        Some(ct) if ct.starts_with("application/grpc") => Ok(()),
        Some(ct) => Err(StatusError::new(
            StatusCodeError::Unimplemented,
            format!("unsupported content-type: {ct}"),
        )),
        None => Err(StatusError::new(
            StatusCodeError::Unimplemented,
            "missing content-type header",
        )),
    }
}

/// Builds a trailers-only error response (HTTP 200 with grpc-status in
/// trailing headers, no body).
fn trailers_only_error(err: StatusError) -> http::Response<GrpcResponseBody> {
    let (body_tx, body_rx) = mpsc::channel(1);
    let (trailers_tx, trailers_rx) = oneshot::channel();
    drop(body_tx); // Close body immediately — no data frames.

    let trailers = Trailers::new(Err(err));
    let _ = trailers_tx.send(trailers);

    http::Response::builder()
        .status(200)
        .header("content-type", "application/grpc")
        .body(GrpcResponseBody::new(body_rx, trailers_rx))
        .expect("valid response")
}

// ---------------------------------------------------------------------------
// Request header extraction
// ---------------------------------------------------------------------------

fn extract_request_headers(
    parts: &http::request::Parts,
) -> RequestHeaders {
    let method_name = parts.uri.path().to_string();
    let metadata = MetadataMap::from_headers(parts.headers.clone()).unwrap_or_default();
    RequestHeaders::new()
        .with_method_name(method_name)
        .with_metadata(metadata)
}

// ---------------------------------------------------------------------------
// Http2Config
// ---------------------------------------------------------------------------

/// Configuration for the HTTP/2 transport.
///
/// All fields default to `None`, which means the transport default is used.
/// Use the builder-style methods to override specific settings.
///
/// # Example
///
/// ```ignore
/// let config = Http2Config::builder()
///     .max_message_size(8 * 1024 * 1024) // 8MB
///     .max_concurrent_streams(100)
///     .keep_alive(Duration::from_secs(30), Duration::from_secs(10))
///     .build();
///
/// let listener = HyperTransport::new_tcp_stream(addr, creds, &rt).await?
///     .with_config(config);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Http2Config {
    max_message_size: Option<usize>,
    max_concurrent_streams: Option<u32>,
    initial_connection_window_size: Option<u32>,
    initial_stream_window_size: Option<u32>,
    keep_alive_interval: Option<Duration>,
    keep_alive_timeout: Option<Duration>,
    max_connection_age: Option<Duration>,
    max_connection_age_grace: Option<Duration>,
}

impl Http2Config {
    /// Creates a new `Http2Config` with default settings (all `None`).
    pub fn builder() -> Self {
        Self::default()
    }

    /// Sets the maximum allowed message payload size in bytes.
    ///
    /// If not set, the default (4MB) is used.
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Sets the maximum number of concurrent HTTP/2 streams per connection.
    pub fn max_concurrent_streams(mut self, max: u32) -> Self {
        self.max_concurrent_streams = Some(max);
        self
    }

    /// Sets the initial HTTP/2 connection-level flow control window size.
    pub fn initial_connection_window_size(mut self, size: u32) -> Self {
        self.initial_connection_window_size = Some(size);
        self
    }

    /// Sets the initial HTTP/2 stream-level flow control window size.
    pub fn initial_stream_window_size(mut self, size: u32) -> Self {
        self.initial_stream_window_size = Some(size);
        self
    }

    /// Sets the HTTP/2 keep-alive interval and timeout.
    ///
    /// The server sends a PING frame after `interval` of inactivity. If
    /// no response is received within `timeout`, the connection is closed.
    pub fn keep_alive(mut self, interval: Duration, timeout: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self.keep_alive_timeout = Some(timeout);
        self
    }

    /// Sets the maximum duration a connection is allowed to exist.
    ///
    /// After this duration, the server sends a GOAWAY frame to the client.
    /// If `max_connection_age_grace` is also set, in-flight RPCs are given
    /// a grace period to complete before the connection is force-closed.
    pub fn max_connection_age(mut self, age: Duration) -> Self {
        self.max_connection_age = Some(age);
        self
    }

    /// Sets the grace period after `max_connection_age` expires.
    ///
    /// After GOAWAY is sent, existing RPCs are given this much additional
    /// time to complete. After the grace period, the connection is forcibly
    /// terminated. If not set, the connection drains indefinitely.
    ///
    /// Has no effect without `max_connection_age`.
    pub fn max_connection_age_grace(mut self, grace: Duration) -> Self {
        self.max_connection_age_grace = Some(grace);
        self
    }

    /// Finalizes the configuration.
    pub fn build(self) -> Self {
        self
    }
}

// ---------------------------------------------------------------------------
// HyperConnection
// ---------------------------------------------------------------------------

pub struct HyperConnection {
    io: Box<dyn crate::rt::GrpcEndpoint>,
    config: Http2Config,
}

impl Transport for HyperConnection {
    async fn serve(
        self,
        handler: Arc<dyn DynHandle>,
        runtime: GrpcRuntime,
        shutdown: Option<tokio::sync::watch::Receiver<()>>,
    ) {
        let exec_runtime = runtime.clone();
        let timer_runtime = runtime.clone();
        let config = self.config.clone();
        let service =
            hyper::service::service_fn(move |req: http::Request<hyper::body::Incoming>| {
                let handler = handler.clone();
                let runtime = runtime.clone();
                let config = config.clone();
                async move {
                    // 1. Content-type validation.
                    if let Err(err) = validate_content_type(&req) {
                        return Ok::<_, Infallible>(trailers_only_error(err));
                    }

                    // 2. Extract request headers.
                    let (parts, body) = req.into_parts();
                    let request_headers = extract_request_headers(&parts);

                    // 3. Create channels.
                    let (headers_tx, headers_rx) = oneshot::channel::<ResponseHeaders>();
                    let (body_tx, body_rx) = mpsc::channel::<Bytes>(32);
                    let (trailers_tx, trailers_rx) = oneshot::channel::<Trailers>();

                    // 4. Create streams.
                    let mut tx = HttpSendStream::new(headers_tx, body_tx);
                    let rx = BoxedRecvStream(Box::new(ByteSourceRecvStream::new(body)));

                    // 5. Spawn handler on runtime.
                    runtime.spawn(Box::pin(async move {
                        let options = CallOptions::default();
                        // Compose the framing interceptor on top of the handler,
                        // using the configured max message size.
                        let max_msg_size = config.max_message_size
                            .unwrap_or(4 * 1024 * 1024); // 4MB default
                        let deframe_config = DeframeConfig {
                            max_message_size: max_msg_size,
                        };
                        let base = DynHandleWrapper(handler);
                        let framed = base.with_interceptor(
                            GrpcFramingInterceptor::new(deframe_config),
                        );
                        let trailers = framed
                            .handle(request_headers, options, &mut tx, rx)
                            .await;
                        drop(tx); // Close body channel.
                        let _ = trailers_tx.send(trailers);
                    }));

                    // 6. Await initial response headers from the handler.
                    let response_headers = match headers_rx.await {
                        Ok(h) => h,
                        Err(_) => {
                            // Handler did not send headers. This is normal for
                            // error-only responses (trailers-only per gRPC spec).
                            // Try to get the handler's actual trailers.
                            match trailers_rx.await {
                                Ok(trailers) => {
                                    let status = trailers.into_status();
                                    let err = match status {
                                        Err(e) => e,
                                        Ok(()) => StatusError::new(
                                            StatusCodeError::Internal,
                                            "handler completed without sending headers or error",
                                        ),
                                    };
                                    return Ok(trailers_only_error(err));
                                }
                                Err(_) => {
                                    // Handler task panicked or was cancelled.
                                    let err = StatusError::new(
                                        StatusCodeError::Internal,
                                        "handler did not send response headers",
                                    );
                                    return Ok(trailers_only_error(err));
                                }
                            }
                        }
                    };

                    // 7. Build HTTP/2 response with handler's headers.
                    let mut builder = http::Response::builder()
                        .status(200)
                        .header("content-type", "application/grpc");

                    // Add metadata from ResponseHeaders.
                    let header_map = response_headers.metadata().clone().into_headers();
                    for (key, value) in header_map.iter() {
                        builder = builder.header(key.clone(), value.clone());
                    }

                    // 8. Create response body.
                    let response_body = GrpcResponseBody::new(body_rx, trailers_rx);

                    Ok::<_, Infallible>(
                        builder.body(response_body).expect("valid response"),
                    )
                }
            });

        // Apply HTTP/2 settings from config to the Hyper connection builder.
        let mut h2_builder = hyper::server::conn::http2::Builder::new(
            HyperCompatExec { inner: exec_runtime },
        );
        if let Some(max) = self.config.max_concurrent_streams {
            h2_builder.max_concurrent_streams(max);
        }
        if let Some(sz) = self.config.initial_connection_window_size {
            h2_builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.config.initial_stream_window_size {
            h2_builder.initial_stream_window_size(sz);
        }
        if let Some(interval) = self.config.keep_alive_interval {
            h2_builder.keep_alive_interval(interval);
        }
        if let Some(timeout) = self.config.keep_alive_timeout {
            h2_builder.keep_alive_timeout(timeout);
        }

        let mut conn = std::pin::pin!(
            h2_builder.serve_connection(HyperStream::new(self.io), service)
        );

        // --- Age timer: starts ticking if max_connection_age is configured.
        //     Otherwise uses a future that never resolves.
        let age_timer: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            match self.config.max_connection_age {
                // TODO: Add ±10% jitter per gRFC A9 to prevent thundering herd.
                Some(age) => Box::pin(timer_runtime.sleep(age)),
                None => Box::pin(std::future::pending()),
            };
        tokio::pin!(age_timer);

        // --- Grace timer: starts as a future that never resolves.
        //     Hot-swapped to a real sleep when age_timer fires.
        let grace_timer: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            Box::pin(std::future::pending());
        tokio::pin!(grace_timer);

        // --- Shutdown signal: optional.
        let mut shutdown_rx = shutdown;

        loop {
            tokio::select! {
                // Branch 1: Connection completes naturally.
                result = &mut conn => {
                    let _ = result;
                    return;
                }

                // Branch 2: Server shutdown signal.
                _ = async {
                    shutdown_rx.as_mut().unwrap().changed().await
                }, if shutdown_rx.is_some() => {
                    conn.as_mut().graceful_shutdown();
                    shutdown_rx = None; // Don't trigger again.
                }

                // Branch 3: Max connection age reached → send GOAWAY.
                _ = &mut age_timer => {
                    conn.as_mut().graceful_shutdown(); // Sends GOAWAY!

                    // Hot-swap grace timer: replace pending() with real sleep.
                    if let Some(grace) = self.config.max_connection_age_grace {
                        grace_timer.set(Box::pin(timer_runtime.sleep(grace)));
                    }
                    // else: no grace → connection drains indefinitely
                    //        (conn will finish naturally via Branch 1)

                    // Disable age timer so it doesn't fire again.
                    age_timer.set(Box::pin(std::future::pending()));
                }

                // Branch 4: Grace period expired → force close.
                _ = &mut grace_timer => {
                    // Dropping conn force-closes the connection.
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// HyperTransport
// ---------------------------------------------------------------------------

pub struct HyperTransport {
    listener: Box<dyn EndpointListener>,
    creds: Arc<dyn DynServerCredentials>,
    runtime: GrpcRuntime,
    config: Http2Config,
}

impl HyperTransport {
    /// Internal constructor — used by factories and tests.
    pub(crate) fn new(
        listener: Box<dyn EndpointListener>,
        creds: Arc<dyn DynServerCredentials>,
        runtime: GrpcRuntime,
    ) -> Self {
        Self { listener, creds, runtime, config: Http2Config::default() }
    }

    /// Creates an HTTP/2 server listener bound to a TCP address.
    ///
    /// The `creds` parameter specifies the server-side credentials (e.g.,
    /// TLS certificates). The generic `C` is immediately type-erased — the
    /// concrete credential type is not retained.
    pub async fn new_tcp_stream<C>(
        addr: std::net::SocketAddr,
        creds: Arc<C>,
        runtime: &GrpcRuntime,
    ) -> Result<Self, String>
    where
        C: ServerCredentials,
        C::Output<Box<dyn GrpcEndpoint>>: GrpcEndpoint + 'static,
    {
        let listener = runtime.tcp_listener(addr).await?;
        Ok(Self {
            listener,
            creds: creds as Arc<dyn DynServerCredentials>,
            runtime: runtime.clone(),
            config: Http2Config::default(),
        })
    }

    /// Applies the given HTTP/2 configuration to this transport.
    ///
    /// All connections accepted from this transport will use the specified
    /// settings.
    pub fn with_config(mut self, config: Http2Config) -> Self {
        self.config = config;
        self
    }

    /// Creates an HTTP/2 server listener bound to a Unix socket path.
    ///
    /// The `creds` parameter specifies the server-side credentials.
    /// On Linux, paths starting with `\0` are treated as abstract
    /// namespace sockets (handled transparently by the runtime).
    pub async fn new_unix_listener<C>(
        path: std::path::PathBuf,
        creds: Arc<C>,
        opts: crate::rt::UnixSocketOptions,
        runtime: &GrpcRuntime,
    ) -> Result<Self, String>
    where
        C: ServerCredentials,
        C::Output<Box<dyn GrpcEndpoint>>: GrpcEndpoint + 'static,
    {
        let listener = runtime.unix_listener(path, opts).await?;
        Ok(Self {
            listener,
            creds: creds as Arc<dyn DynServerCredentials>,
            runtime: runtime.clone(),
            config: Http2Config::default(),
        })
    }
}

impl Listener for HyperTransport {
    type Transport = HyperConnection;

    async fn accept(&self) -> Option<Self::Transport> {
        // 1. Accept raw byte stream from the listener.
        let endpoint = self.listener.accept().await.ok()?;
        // 2. Perform server-side credential handshake (e.g., TLS).
        let handshake = self.creds.dyn_accept(endpoint, self.runtime.clone()).await.ok()?;
        // 3. Return the wrapped connection.
        Some(HyperConnection {
            io: handshake.endpoint,
            config: self.config.clone(),
        })
    }

    fn local_addr(&self) -> Result<std::net::SocketAddr, String> {
        self.listener.local_addr()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http_body::Frame;
    use http_body_util::StreamBody;
    use futures::stream;

    /// A simple RecvMessage that captures raw bytes.
    struct BytesCapture {
        data: Option<Bytes>,
    }

    impl BytesCapture {
        fn new() -> Self {
            Self { data: None }
        }
    }

    impl RecvMessage for BytesCapture {
        fn decode(&mut self, data: &mut dyn Buf) -> Result<(), String> {
            self.data = Some(data.copy_to_bytes(data.remaining()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn byte_source_yields_data_frame() {
        let body = StreamBody::new(stream::iter(vec![
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"hello"))),
        ]));
        let mut recv = ByteSourceRecvStream::new(body);
        let mut msg = BytesCapture::new();

        let result = recv.next(&mut msg).await;
        assert!(matches!(result, Some(Ok(()))));
        assert_eq!(msg.data.as_deref(), Some(b"hello".as_slice()));
    }

    #[tokio::test]
    async fn byte_source_skips_empty_data_frame() {
        let body = StreamBody::new(stream::iter(vec![
            Ok::<_, Infallible>(Frame::data(Bytes::new())),           // empty — skip
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"real"))), // real data
        ]));
        let mut recv = ByteSourceRecvStream::new(body);
        let mut msg = BytesCapture::new();

        let result = recv.next(&mut msg).await;
        assert!(matches!(result, Some(Ok(()))));
        assert_eq!(msg.data.as_deref(), Some(b"real".as_slice()));
    }

    #[tokio::test]
    async fn byte_source_trailer_frame_returns_error() {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().expect("valid"));
        let body = StreamBody::new(stream::iter(vec![
            Ok::<_, Infallible>(Frame::trailers(trailers)),
        ]));
        let mut recv = ByteSourceRecvStream::new(body);
        let mut msg = BytesCapture::new();

        let result = recv.next(&mut msg).await;
        assert!(matches!(result, Some(Err(()))));
    }

    #[tokio::test]
    async fn byte_source_exhausted_returns_none() {
        let body = StreamBody::new(stream::empty::<Result<Frame<Bytes>, Infallible>>());
        let mut recv = ByteSourceRecvStream::new(body);
        let mut msg = BytesCapture::new();

        let result = recv.next(&mut msg).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn byte_source_multiple_data_frames() {
        let body = StreamBody::new(stream::iter(vec![
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"one"))),
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"two"))),
        ]));
        let mut recv = ByteSourceRecvStream::new(body);

        let mut msg1 = BytesCapture::new();
        let result1 = recv.next(&mut msg1).await;
        assert!(matches!(result1, Some(Ok(()))));
        assert_eq!(msg1.data.as_deref(), Some(b"one".as_slice()));

        let mut msg2 = BytesCapture::new();
        let result2 = recv.next(&mut msg2).await;
        assert!(matches!(result2, Some(Ok(()))));
        assert_eq!(msg2.data.as_deref(), Some(b"two".as_slice()));

        let mut msg3 = BytesCapture::new();
        let result3 = recv.next(&mut msg3).await;
        assert!(result3.is_none());
    }

    // --- Http2Config tests ---

    #[test]
    fn http2_config_default_all_none() {
        let config = Http2Config::default();
        assert!(config.max_message_size.is_none());
        assert!(config.max_concurrent_streams.is_none());
        assert!(config.initial_connection_window_size.is_none());
        assert!(config.initial_stream_window_size.is_none());
        assert!(config.keep_alive_interval.is_none());
        assert!(config.keep_alive_timeout.is_none());
        assert!(config.max_connection_age.is_none());
        assert!(config.max_connection_age_grace.is_none());
    }

    #[test]
    fn http2_config_builder_returns_default() {
        let config = Http2Config::builder();
        assert!(config.max_message_size.is_none());
    }

    #[test]
    fn http2_config_max_message_size() {
        let config = Http2Config::builder()
            .max_message_size(8 * 1024 * 1024)
            .build();
        assert_eq!(config.max_message_size, Some(8 * 1024 * 1024));
    }

    #[test]
    fn http2_config_max_concurrent_streams() {
        let config = Http2Config::builder()
            .max_concurrent_streams(100)
            .build();
        assert_eq!(config.max_concurrent_streams, Some(100));
    }

    #[test]
    fn http2_config_window_sizes() {
        let config = Http2Config::builder()
            .initial_connection_window_size(1024 * 1024)
            .initial_stream_window_size(512 * 1024)
            .build();
        assert_eq!(config.initial_connection_window_size, Some(1024 * 1024));
        assert_eq!(config.initial_stream_window_size, Some(512 * 1024));
    }

    #[test]
    fn http2_config_keep_alive() {
        use std::time::Duration;
        let config = Http2Config::builder()
            .keep_alive(Duration::from_secs(30), Duration::from_secs(10))
            .build();
        assert_eq!(config.keep_alive_interval, Some(Duration::from_secs(30)));
        assert_eq!(config.keep_alive_timeout, Some(Duration::from_secs(10)));
    }

    #[test]
    fn http2_config_chained_builder() {
        use std::time::Duration;
        let config = Http2Config::builder()
            .max_message_size(16 * 1024 * 1024)
            .max_concurrent_streams(200)
            .keep_alive(Duration::from_secs(60), Duration::from_secs(20))
            .build();
        assert_eq!(config.max_message_size, Some(16 * 1024 * 1024));
        assert_eq!(config.max_concurrent_streams, Some(200));
        assert_eq!(config.keep_alive_interval, Some(Duration::from_secs(60)));
        assert_eq!(config.keep_alive_timeout, Some(Duration::from_secs(20)));
        // Unset fields remain None.
        assert!(config.initial_connection_window_size.is_none());
        assert!(config.initial_stream_window_size.is_none());
    }

    #[test]
    fn http2_config_clone() {
        let config = Http2Config::builder()
            .max_message_size(4096)
            .build();
        let cloned = config.clone();
        assert_eq!(cloned.max_message_size, config.max_message_size);
    }

    // --- Max connection age config tests ---

    #[test]
    fn http2_config_max_connection_age() {
        use std::time::Duration;
        let config = Http2Config::builder()
            .max_connection_age(Duration::from_secs(600))
            .build();
        assert_eq!(config.max_connection_age, Some(Duration::from_secs(600)));
        assert!(config.max_connection_age_grace.is_none());
    }

    #[test]
    fn http2_config_max_connection_age_grace() {
        use std::time::Duration;
        let config = Http2Config::builder()
            .max_connection_age_grace(Duration::from_secs(30))
            .build();
        assert!(config.max_connection_age.is_none());
        assert_eq!(config.max_connection_age_grace, Some(Duration::from_secs(30)));
    }

    #[test]
    fn http2_config_max_connection_age_with_grace() {
        use std::time::Duration;
        let config = Http2Config::builder()
            .max_connection_age(Duration::from_secs(600))
            .max_connection_age_grace(Duration::from_secs(30))
            .build();
        assert_eq!(config.max_connection_age, Some(Duration::from_secs(600)));
        assert_eq!(config.max_connection_age_grace, Some(Duration::from_secs(30)));
    }

    #[test]
    fn http2_config_full_chained_builder() {
        use std::time::Duration;
        let config = Http2Config::builder()
            .max_message_size(8 * 1024 * 1024)
            .max_concurrent_streams(100)
            .keep_alive(Duration::from_secs(30), Duration::from_secs(10))
            .max_connection_age(Duration::from_secs(300))
            .max_connection_age_grace(Duration::from_secs(15))
            .build();
        assert_eq!(config.max_message_size, Some(8 * 1024 * 1024));
        assert_eq!(config.max_concurrent_streams, Some(100));
        assert_eq!(config.keep_alive_interval, Some(Duration::from_secs(30)));
        assert_eq!(config.keep_alive_timeout, Some(Duration::from_secs(10)));
        assert_eq!(config.max_connection_age, Some(Duration::from_secs(300)));
        assert_eq!(config.max_connection_age_grace, Some(Duration::from_secs(15)));
    }
}
