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

//! End-to-end tests for the streaming building blocks, plus a protoc-free
//! stand-in for generated code (mirrors the RouteGuide codegen shape using
//! `protobuf_well_known_types::Any` as the message type).

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use bytes::Buf;
use bytes::Bytes;
use grpc::client::CallOptions;
use grpc::core::RecvMessage;
use grpc::server_internal::BoxedRecvStream;
use grpc::server_internal::DynHandle;
use grpc::server_internal::RecvStream;
use grpc::server_internal::RequestHeaders;
use grpc::server_internal::ResponseStreamItem;
use grpc::server_internal::SendOptions;
use grpc::server_internal::SendStream;
use grpc::server_internal::Service;
use grpc::server_internal::descriptor::MethodDescriptor;
use grpc::server_internal::descriptor::MethodType;
use grpc::server_internal::descriptor::ServiceDescriptor;
use protobuf::ClearAndParse;
use protobuf::Proxied;
use protobuf::Serialize;
use protobuf_well_known_types::Any;

use crate::Status;
use crate::StatusError;
use crate::server::BidiStreamingAdapter;
use crate::server::BidiStreamingMethod;
use crate::server::ClientStreamingAdapter;
use crate::server::ClientStreamingMethod;
use crate::server::GrpcStreamingRequest;
use crate::server::GrpcStreamingResponse;
use crate::server::ServerStreamingAdapter;
use crate::server::ServerStreamingMethod;
use crate::server::UnaryAdapter;
use crate::server::UnaryMethod;
use crate::status::StatusCodeError;

// ---------------------------------------------------------------------------
// Test harness: a trivial executor + mock send/recv streams.
// ---------------------------------------------------------------------------

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

fn encode(msg: &Any) -> Bytes {
    Bytes::from(msg.serialize().expect("serialize should succeed"))
}

fn any(type_url: &str) -> Any {
    let mut a = Any::new();
    a.set_type_url(type_url);
    a
}

/// A receive stream that yields a fixed sequence of encoded messages.
struct Messages(std::collections::VecDeque<Bytes>);

impl Messages {
    fn new(msgs: impl IntoIterator<Item = Bytes>) -> Self {
        Self(msgs.into_iter().collect())
    }
}

impl RecvStream for Messages {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
        let mut bytes = self.0.pop_front()?;
        Some(msg.decode(&mut bytes).map_err(|_| ()))
    }
}

/// A send stream that records the encoded bytes of every message sent.
#[derive(Default)]
struct Captured {
    messages: Vec<Vec<u8>>,
}

impl SendStream for Captured {
    async fn send<'a>(
        &mut self,
        item: ResponseStreamItem<'a>,
        _options: SendOptions,
    ) -> Result<(), ()> {
        if let ResponseStreamItem::Message(m) = item {
            let mut buf = m.encode().expect("encode should succeed");
            let mut bytes = vec![0u8; buf.remaining()];
            buf.copy_to_slice(&mut bytes);
            self.messages.push(bytes);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-cardinality dispatch tests.
// ---------------------------------------------------------------------------

/// Server-streaming: emits `n` responses echoing the request's type URL.
struct Repeat {
    n: usize,
}

impl ServerStreamingMethod for Repeat {
    type Request = Any;
    type Response = Any;

    async fn call(
        &self,
        _request: <Any as Proxied>::View<'_>,
        mut responses: GrpcStreamingResponse<'_, Any>,
    ) -> Status {
        for _ in 0..self.n {
            let resp = any("echo");
            responses
                .send(&resp)
                .await
                .map_err(|_| StatusError::new(StatusCodeError::Cancelled, "send failed"))?;
        }
        Ok(())
    }
}

#[test]
fn server_streaming_emits_n_responses_and_ok() {
    let adapter = ServerStreamingAdapter::new(Repeat { n: 3 });
    let mut tx = Captured::default();
    let rx = Messages::new([encode(&any("type.googleapis.com/echo"))]);

    let trailers = block_on(adapter.dyn_handle(
        RequestHeaders::new(),
        CallOptions::default(),
        &mut tx,
        BoxedRecvStream(Box::new(rx)),
    ));

    assert!(trailers.status().is_ok());
    assert_eq!(tx.messages.len(), 3);
}

/// Client-streaming: counts request messages and reports the count.
struct Count;

impl ClientStreamingMethod for Count {
    type Request = Any;
    type Response = Any;

    async fn call(
        &self,
        mut requests: GrpcStreamingRequest<Any>,
        mut response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> Status {
        let mut count = 0usize;
        while requests.recv().await.is_some() {
            count += 1;
        }
        response.set_type_url(format!("count/{count}"));
        Ok(())
    }
}

#[test]
fn client_streaming_counts_requests_and_flushes_one_response() {
    let adapter = ClientStreamingAdapter::new(Count);
    let mut tx = Captured::default();
    let rx = Messages::new([encode(&any("a")), encode(&any("b")), encode(&any("c"))]);

    let trailers = block_on(adapter.dyn_handle(
        RequestHeaders::new(),
        CallOptions::default(),
        &mut tx,
        BoxedRecvStream(Box::new(rx)),
    ));

    assert!(trailers.status().is_ok());
    assert_eq!(tx.messages.len(), 1);
    let mut got = Any::new();
    got.clear_and_parse(&tx.messages[0])
        .expect("parse response");
    assert_eq!(got.type_url(), "count/3");
}

/// Bidi: echoes every request message back as a response.
struct Echo;

impl BidiStreamingMethod for Echo {
    type Request = Any;
    type Response = Any;

    async fn call(
        &self,
        mut requests: GrpcStreamingRequest<Any>,
        mut responses: GrpcStreamingResponse<'_, Any>,
    ) -> Status {
        while let Some(req) = requests.recv().await {
            responses
                .send(&req)
                .await
                .map_err(|_| StatusError::new(StatusCodeError::Cancelled, "send failed"))?;
        }
        Ok(())
    }
}

#[test]
fn bidi_echoes_each_request() {
    let adapter = BidiStreamingAdapter::new(Echo);
    let mut tx = Captured::default();
    let rx = Messages::new([encode(&any("x")), encode(&any("y"))]);

    let trailers = block_on(adapter.dyn_handle(
        RequestHeaders::new(),
        CallOptions::default(),
        &mut tx,
        BoxedRecvStream(Box::new(rx)),
    ));

    assert!(trailers.status().is_ok());
    assert_eq!(tx.messages.len(), 2);
}

// ---------------------------------------------------------------------------
// Codegen-shape validation: a protoc-free stand-in for generated code that
// registers one method of every cardinality as `Arc<dyn DynHandle>`, exactly
// like the RouteGuide `*Server<T>` would.
// ---------------------------------------------------------------------------

/// Application service trait (what generated code would emit + the user impls).
trait DemoService: Send + Sync + 'static {
    fn unary(
        &self,
        request: <Any as Proxied>::View<'_>,
        response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> impl Future<Output = Status> + Send;
    fn server_stream(
        &self,
        request: <Any as Proxied>::View<'_>,
        responses: GrpcStreamingResponse<'_, Any>,
    ) -> impl Future<Output = Status> + Send;
    fn client_stream(
        &self,
        requests: GrpcStreamingRequest<Any>,
        response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> impl Future<Output = Status> + Send;
    fn bidi(
        &self,
        requests: GrpcStreamingRequest<Any>,
        responses: GrpcStreamingResponse<'_, Any>,
    ) -> impl Future<Output = Status> + Send;
}

struct DemoUnary<T> {
    service: Arc<T>,
}
impl<T: DemoService> UnaryMethod for DemoUnary<T> {
    type Request = Any;
    type Response = Any;
    async fn call(
        &self,
        request: <Any as Proxied>::View<'_>,
        response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> Status {
        self.service.unary(request, response).await
    }
}

struct DemoServerStream<T> {
    service: Arc<T>,
}
impl<T: DemoService> ServerStreamingMethod for DemoServerStream<T> {
    type Request = Any;
    type Response = Any;
    async fn call(
        &self,
        request: <Any as Proxied>::View<'_>,
        responses: GrpcStreamingResponse<'_, Any>,
    ) -> Status {
        self.service.server_stream(request, responses).await
    }
}

struct DemoClientStream<T> {
    service: Arc<T>,
}
impl<T: DemoService> ClientStreamingMethod for DemoClientStream<T> {
    type Request = Any;
    type Response = Any;
    async fn call(
        &self,
        requests: GrpcStreamingRequest<Any>,
        response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> Status {
        self.service.client_stream(requests, response).await
    }
}

struct DemoBidi<T> {
    service: Arc<T>,
}
impl<T: DemoService> BidiStreamingMethod for DemoBidi<T> {
    type Request = Any;
    type Response = Any;
    async fn call(
        &self,
        requests: GrpcStreamingRequest<Any>,
        responses: GrpcStreamingResponse<'_, Any>,
    ) -> Status {
        self.service.bidi(requests, responses).await
    }
}

struct DemoServer<T> {
    service: Arc<T>,
}
impl<T: DemoService> DemoServer<T> {
    fn new(service: T) -> Self {
        Self {
            service: Arc::new(service),
        }
    }
}
impl<T: DemoService> Service for DemoServer<T> {
    fn descriptor(&self) -> ServiceDescriptor {
        ServiceDescriptor::new(
            "demo.Demo",
            vec![
                MethodDescriptor::new("/demo.Demo/Unary", MethodType::Unary),
                MethodDescriptor::new("/demo.Demo/ServerStream", MethodType::ServerStreaming),
                MethodDescriptor::new("/demo.Demo/ClientStream", MethodType::ClientStreaming),
                MethodDescriptor::new("/demo.Demo/Bidi", MethodType::BidiStreaming),
            ],
        )
    }

    fn register_methods(self) -> Vec<(String, Arc<dyn DynHandle>)> {
        vec![
            (
                "/demo.Demo/Unary".to_string(),
                Arc::new(UnaryAdapter::new(DemoUnary {
                    service: self.service.clone(),
                })),
            ),
            (
                "/demo.Demo/ServerStream".to_string(),
                Arc::new(ServerStreamingAdapter::new(DemoServerStream {
                    service: self.service.clone(),
                })),
            ),
            (
                "/demo.Demo/ClientStream".to_string(),
                Arc::new(ClientStreamingAdapter::new(DemoClientStream {
                    service: self.service.clone(),
                })),
            ),
            (
                "/demo.Demo/Bidi".to_string(),
                Arc::new(BidiStreamingAdapter::new(DemoBidi {
                    service: self.service.clone(),
                })),
            ),
        ]
    }
}

struct DemoImpl;
impl DemoService for DemoImpl {
    async fn unary(
        &self,
        _request: <Any as Proxied>::View<'_>,
        _response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> Status {
        Ok(())
    }
    async fn server_stream(
        &self,
        _request: <Any as Proxied>::View<'_>,
        _responses: GrpcStreamingResponse<'_, Any>,
    ) -> Status {
        Ok(())
    }
    async fn client_stream(
        &self,
        _requests: GrpcStreamingRequest<Any>,
        _response: <Any as protobuf::MutProxied>::Mut<'_>,
    ) -> Status {
        Ok(())
    }
    async fn bidi(
        &self,
        _requests: GrpcStreamingRequest<Any>,
        _responses: GrpcStreamingResponse<'_, Any>,
    ) -> Status {
        Ok(())
    }
}

#[test]
fn generated_service_shape_registers_all_cardinalities() {
    let server = DemoServer::new(DemoImpl);

    let descriptor = server.descriptor();
    assert_eq!(descriptor.name(), "demo.Demo");
    assert_eq!(descriptor.methods().len(), 4);

    let methods = server.register_methods();
    assert_eq!(methods.len(), 4);
    let paths: Vec<&str> = methods.iter().map(|(p, _)| p.as_str()).collect();
    assert!(paths.contains(&"/demo.Demo/Unary"));
    assert!(paths.contains(&"/demo.Demo/ServerStream"));
    assert!(paths.contains(&"/demo.Demo/ClientStream"));
    assert!(paths.contains(&"/demo.Demo/Bidi"));
}
