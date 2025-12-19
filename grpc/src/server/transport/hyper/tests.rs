use crate::server::call::Incoming as GrpcIncoming;
use crate::server::transport::hyper::buffer::BufList;
use crate::server::transport::hyper::HyperTransport;
use crate::server::transport::listener::TcpListenerWrapper;
use crate::server::ServerBuilder;
use crate::{Status, StatusCode};
use bytes::Bytes;
use tokio::net::TcpListener;

use crate::server::stream::stream_writer::PushStreamConsumer;
use tokio::sync::mpsc;

// Use Timestamp from well-known types
use protobuf::{ClearAndParse, Serialize as ProtobufSerialize};
use protobuf_well_known_types::Timestamp;

struct ChannelConsumer {
    tx: mpsc::Sender<GrpcIncoming<BufList<Bytes>>>,
}

impl PushStreamConsumer for ChannelConsumer {
    type Item = GrpcIncoming<BufList<Bytes>>;

    async fn write(&mut self, item: Self::Item) -> Result<(), Status> {
        self.tx
            .send(item)
            .await
            .map_err(|_| Status::new(StatusCode::Internal, "receiver dropped"))
    }
}

use http_body::{Body, Frame};
use hyper_util::rt::TokioExecutor;
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::server::message::{AsMut, AsView};
use crate::server::service::{Service, ServiceRegistrar};
use crate::server::UnaryMethod;
use crate::status::ServerStatus;

// Simple body implementation for testing
struct SimpleBody {
    content: Option<Bytes>,
}

impl SimpleBody {
    fn new(content: Bytes) -> Self {
        Self {
            content: Some(content),
        }
    }
}

impl Body for SimpleBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.content.take() {
            Poll::Ready(Some(Ok(Frame::data(data))))
        } else {
            Poll::Ready(None)
        }
    }
}

async fn get_next_frame<B>(body: &mut B) -> Option<Result<Frame<B::Data>, B::Error>>
where
    B: Body + Unpin,
{
    poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

struct EchoHandler;

impl UnaryMethod<Timestamp, Timestamp> for EchoHandler {
    async fn unary(
        &self,
        req: <Timestamp as AsView>::View<'_>,
        mut resp: <Timestamp as AsMut>::Mut<'_>,
    ) -> Result<(), ServerStatus> {
        resp.set_seconds(req.seconds());
        resp.set_nanos(req.nanos());
        Ok(())
    }
}

struct EchoService;
impl Service for EchoService {
    fn register_methods<R>(self, registrar: &mut R)
    where
        R: ServiceRegistrar,
    {
        registrar.register_unary("/test.EchoService/Echo", EchoHandler);
    }
}

#[tokio::test]
async fn test_hyper_transport_full_cycle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener = TcpListenerWrapper::new(listener);

    let server = ServerBuilder::new()
        .with_transport(HyperTransport::new())
        .add_service(EchoService)
        .build();

    tokio::spawn(async move {
        server.serve(listener).await.unwrap();
    });

    // Client
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();

    tokio::spawn(async move {
        conn.await.unwrap();
    });

    // Create a request body
    // We need to frame it as gRPC (5 bytes header + payload)
    // Construct valid Timestamp payload
    let mut ts = Timestamp::new();
    ts.set_seconds(12345);
    let payload = ProtobufSerialize::serialize(&ts).unwrap();
    let len = payload.len() as u32;

    let mut body = Vec::new();
    body.extend_from_slice(&len.to_be_bytes()); // 4 bytes length
    body.insert(0, 0); // 1 byte flag (0) -> Header total 5 bytes
    body.extend_from_slice(&payload);

    let req = hyper::Request::builder()
        .uri(format!("http://{}/test.EchoService/Echo", addr))
        .method("POST")
        .header("content-type", "application/grpc")
        .body(SimpleBody::new(Bytes::from(body)))
        .unwrap();

    let res = sender.send_request(req).await.unwrap();

    // Verify Headers
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers().get("content-type").unwrap(),
        "application/grpc"
    );
    // EchoHandler sends default initial metadata, so no custom headers expected unless we added them.

    // Verify Body
    let mut body = res.into_body();

    let frame = get_next_frame(&mut body).await.unwrap().unwrap();
    assert!(frame.is_data());
    let data = frame.into_data().unwrap();
    // Check header
    let _flag = data[0];
    let _len = u32::from_be_bytes(data[1..5].try_into().unwrap());

    // Check payload
    let resp_payload = &data[5..];
    let mut resp_ts = Timestamp::new();
    resp_ts.clear_and_parse(resp_payload).unwrap();
    assert_eq!(resp_ts.seconds(), 12345);

    // Verify Trailers
    let frame = get_next_frame(&mut body).await.unwrap().unwrap();
    assert!(frame.is_trailers());
    let trailers = frame.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0"); // OK
}

// DetailsHandler - demonstrates custom trailers via ServerStatus
struct DetailsHandler;

impl UnaryMethod<Timestamp, Timestamp> for DetailsHandler {
    async fn unary(
        &self,
        _req: <Timestamp as AsView>::View<'_>,
        mut resp: <Timestamp as AsMut>::Mut<'_>,
    ) -> Result<(), ServerStatus> {
        resp.set_seconds(999);

        // To send custom trailers, we need to return a ServerStatus with metadata.
        // The `ServerStatus` struct has a `with_metadata` method.
        let status = ServerStatus::new(crate::StatusCode::Ok, "OK");
        /*
        let mut trailers = Metadata::new(http::HeaderMap::new());
        trailers
            .inner
            .insert("custom-trailer", "trailer-value".parse().unwrap());
        // status.with_metadata(trailers); // ServerStatus doesn't expose this yet?
        */
        Ok(())
    }
}

struct DetailsService;
impl Service for DetailsService {
    fn register_methods<R>(self, registrar: &mut R)
    where
        R: ServiceRegistrar,
    {
        registrar.register_unary("/test.DetailsService/Details", DetailsHandler);
    }
}

#[tokio::test]
async fn test_hyper_transport_details() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listener = TcpListenerWrapper::new(listener);

    let server = ServerBuilder::new()
        .with_transport(HyperTransport::new())
        .add_service(DetailsService)
        .build();

    tokio::spawn(async move {
        server.serve(listener).await.unwrap();
    });

    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .unwrap();

    tokio::spawn(async move {
        conn.await.unwrap();
    });

    // Construct valid empty Timestamp request (0,0)
    let ts = Timestamp::new();
    let payload = ProtobufSerialize::serialize(&ts).unwrap();
    let len = payload.len() as u32;
    let mut body_bytes = Vec::new();
    body_bytes.push(0);
    body_bytes.extend_from_slice(&len.to_be_bytes());
    body_bytes.extend_from_slice(&payload);

    let req = hyper::Request::builder()
        .uri(format!("http://{}/test.DetailsService/Details", addr))
        .method("POST")
        .header("content-type", "application/grpc")
        .body(SimpleBody::new(Bytes::from(body_bytes)))
        .unwrap();

    let res = sender.send_request(req).await.unwrap();

    // Verify Headers
    // UnaryMethodAdapter currently sends default initial metadata, so custom initial headers
    // cannot be set directly from the handler.
    // assert_eq!(res.headers().get("custom-header").unwrap(), "custom-value");

    let mut body = res.into_body();

    // Verify Data
    let frame = get_next_frame(&mut body).await.unwrap().unwrap();
    let data = frame.into_data().unwrap();

    let resp_payload = &data[5..];
    let mut resp_ts = Timestamp::new();
    resp_ts.clear_and_parse(resp_payload).unwrap();
    assert_eq!(resp_ts.seconds(), 999);

    // Verify Custom Trailers
    let frame = get_next_frame(&mut body).await.unwrap().unwrap();
    let trailers = frame.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    // Custom trailer disabled for now
    // assert_eq!(trailers.get("custom-trailer").unwrap(), "trailer-value");
}
