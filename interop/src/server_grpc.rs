//! gRPC-native interop server implementation.
//!
//! This module implements the interop test service handlers directly against
//! the `grpc::server` API (not tonic's server), demonstrating the native
//! gRPC Rust API.

use std::sync::Arc;

use grpc::StatusCodeError;
use grpc::StatusError;
use grpc::client::CallOptions;
use grpc::core::{RequestHeaders, ResponseHeaders, ServerResponseStreamItem, Trailers};
use grpc::credentials::InsecureServerCredentials;
use grpc::metadata::{Ascii, Binary, MetadataValue};
use grpc::server::descriptor::{MethodDescriptor, MethodType, ServiceDescriptor};
use grpc::server::interceptor::Intercept;
use grpc::server::service::{Service, ServiceExt, ServiceRegistrar};
use grpc::server::transport::hyper::HyperTransport;
use grpc::server::{Handle, RecvStream, SendOptions, SendStream, Server};
use grpc_protobuf::{ProtoRecvMessage, ProtoSendMessage};
use protobuf::proto;

use crate::grpc_pb::{
    Empty, Payload, SimpleRequest, SimpleResponse, StreamingInputCallRequest,
    StreamingInputCallResponse, StreamingOutputCallRequest, StreamingOutputCallResponse,
};

// ---------------------------------------------------------------------------
// EmptyCallHandler
// ---------------------------------------------------------------------------

struct EmptyCallHandler;

impl Handle for EmptyCallHandler {
    async fn handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut impl SendStream,
        mut rx: impl RecvStream + 'static,
    ) -> Trailers {
        // Receive the Empty request.
        let mut request = Empty::new();
        let mut recv_msg = ProtoRecvMessage::from_mut(&mut request);
        match rx.next(&mut recv_msg).await {
            Some(Ok(())) => {}
            Some(Err(())) => {
                return Trailers::new(Err(StatusError::new(
                    StatusCodeError::Internal,
                    "failed to receive request",
                )));
            }
            None => {
                return Trailers::new(Err(StatusError::new(
                    StatusCodeError::Internal,
                    "no request message received",
                )));
            }
        }

        // Send response headers.
        let _ = tx
            .send(
                ServerResponseStreamItem::Headers(ResponseHeaders::new()),
                SendOptions::default(),
            )
            .await;

        // Send the Empty response.
        let response = proto!(Empty {});
        let send_msg = ProtoSendMessage::from_view(&response);
        let _ = tx
            .send(
                ServerResponseStreamItem::Message(&send_msg),
                SendOptions::default(),
            )
            .await;

        Trailers::new(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// UnaryCallHandler
// ---------------------------------------------------------------------------

struct UnaryCallHandler;

impl Handle for UnaryCallHandler {
    async fn handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut impl SendStream,
        mut rx: impl RecvStream + 'static,
    ) -> Trailers {
        // Receive the SimpleRequest.
        let mut request = SimpleRequest::new();
        let mut recv_msg = ProtoRecvMessage::from_mut(&mut request);
        match rx.next(&mut recv_msg).await {
            Some(Ok(())) => {}
            Some(Err(())) => {
                return Trailers::new(Err(StatusError::new(
                    StatusCodeError::Internal,
                    "failed to receive request",
                )));
            }
            None => {
                return Trailers::new(Err(StatusError::new(
                    StatusCodeError::Internal,
                    "no request message received",
                )));
            }
        }

        // If response_status is set with a non-zero code, return that as an error.
        if request.response_status().code() != 0 {
            let echo_status = request.response_status();
            let code = StatusCodeError::from(echo_status.code());
            let message = echo_status.message().to_string();
            return Trailers::new(Err(StatusError::new(code, message)));
        }

        // Build the response with a payload of the requested size.
        let res_size = if request.response_size() >= 0 {
            request.response_size() as usize
        } else {
            return Trailers::new(Err(StatusError::new(
                StatusCodeError::InvalidArgument,
                "response_size cannot be negative",
            )));
        };

        // Send response headers.
        let _ = tx
            .send(
                ServerResponseStreamItem::Headers(ResponseHeaders::new()),
                SendOptions::default(),
            )
            .await;

        // Send the SimpleResponse.
        let response = proto!(SimpleResponse {
            payload: Payload {
                body: vec![0u8; res_size],
            },
        });
        let send_msg = ProtoSendMessage::from_view(&response);
        let _ = tx
            .send(
                ServerResponseStreamItem::Message(&send_msg),
                SendOptions::default(),
            )
            .await;

        Trailers::new(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// UnimplementedCallHandler
// ---------------------------------------------------------------------------

struct UnimplementedCallHandler;

impl Handle for UnimplementedCallHandler {
    async fn handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        _tx: &mut impl SendStream,
        _rx: impl RecvStream + 'static,
    ) -> Trailers {
        Trailers::new(Err(StatusError::new(
            StatusCodeError::Unimplemented,
            "",
        )))
    }
}

// ---------------------------------------------------------------------------
// StreamingOutputCallHandler (server streaming)
// ---------------------------------------------------------------------------

struct StreamingOutputCallHandler;

impl Handle for StreamingOutputCallHandler {
    async fn handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut impl SendStream,
        mut rx: impl RecvStream + 'static,
    ) -> Trailers {
        // Receive the StreamingOutputCallRequest.
        let mut request = StreamingOutputCallRequest::new();
        let mut recv_msg = ProtoRecvMessage::from_mut(&mut request);
        match rx.next(&mut recv_msg).await {
            Some(Ok(())) => {}
            _ => {
                return Trailers::new(Err(StatusError::new(
                    StatusCodeError::Internal,
                    "failed to receive request",
                )));
            }
        }

        // Send response headers.
        let _ = tx
            .send(
                ServerResponseStreamItem::Headers(ResponseHeaders::new()),
                SendOptions::default(),
            )
            .await;

        // Send one response per response_parameters entry.
        for param in request.response_parameters() {
            let payload = crate::grpc_utils::server_payload(param.size() as usize);
            let response = proto!(StreamingOutputCallResponse { payload: payload });
            let send_msg = ProtoSendMessage::from_view(&response);
            let _ = tx
                .send(
                    ServerResponseStreamItem::Message(&send_msg),
                    SendOptions::default(),
                )
                .await;
        }

        Trailers::new(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// StreamingInputCallHandler (client streaming)
// ---------------------------------------------------------------------------

struct StreamingInputCallHandler;

impl Handle for StreamingInputCallHandler {
    async fn handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut impl SendStream,
        mut rx: impl RecvStream + 'static,
    ) -> Trailers {
        // Receive all client messages and aggregate payload sizes.
        let mut aggregated_payload_size: i32 = 0;
        loop {
            let mut request = StreamingInputCallRequest::new();
            let mut recv_msg = ProtoRecvMessage::from_mut(&mut request);
            match rx.next(&mut recv_msg).await {
                Some(Ok(())) => {
                    aggregated_payload_size += request.payload().body().len() as i32;
                }
                Some(Err(())) => {
                    return Trailers::new(Err(StatusError::new(
                        StatusCodeError::Internal,
                        "failed to receive streaming request",
                    )));
                }
                None => break, // Client done sending.
            }
        }

        // Send response headers.
        let _ = tx
            .send(
                ServerResponseStreamItem::Headers(ResponseHeaders::new()),
                SendOptions::default(),
            )
            .await;

        // Send the aggregated response.
        let response = proto!(StreamingInputCallResponse {
            aggregated_payload_size: aggregated_payload_size,
        });
        let send_msg = ProtoSendMessage::from_view(&response);
        let _ = tx
            .send(
                ServerResponseStreamItem::Message(&send_msg),
                SendOptions::default(),
            )
            .await;

        Trailers::new(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// FullDuplexCallHandler (bidirectional streaming)
// ---------------------------------------------------------------------------

struct FullDuplexCallHandler;

impl Handle for FullDuplexCallHandler {
    async fn handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut impl SendStream,
        mut rx: impl RecvStream + 'static,
    ) -> Trailers {
        // Send response headers first.
        let _ = tx
            .send(
                ServerResponseStreamItem::Headers(ResponseHeaders::new()),
                SendOptions::default(),
            )
            .await;

        // For each incoming message, check for error status and send responses.
        loop {
            let mut request = StreamingOutputCallRequest::new();
            let mut recv_msg = ProtoRecvMessage::from_mut(&mut request);
            match rx.next(&mut recv_msg).await {
                Some(Ok(())) => {
                    // If response_status is set with a non-zero code, return error.
                    if request.response_status().code() != 0 {
                        let echo_status = request.response_status();
                        let code = StatusCodeError::from(echo_status.code());
                        let message = echo_status.message().to_string();
                        return Trailers::new(Err(StatusError::new(code, message)));
                    }

                    // Send one response per response_parameters entry.
                    for param in request.response_parameters() {
                        let payload =
                            crate::grpc_utils::server_payload(param.size() as usize);
                        let response =
                            proto!(StreamingOutputCallResponse { payload: payload });
                        let send_msg = ProtoSendMessage::from_view(&response);
                        let _ = tx
                            .send(
                                ServerResponseStreamItem::Message(&send_msg),
                                SendOptions::default(),
                            )
                            .await;
                    }
                }
                Some(Err(())) => {
                    return Trailers::new(Err(StatusError::new(
                        StatusCodeError::Internal,
                        "failed to receive streaming request",
                    )));
                }
                None => break, // Client done sending.
            }
        }

        Trailers::new(Ok(()))
    }
}

// ---------------------------------------------------------------------------
// EchoMetadataInterceptor
// ---------------------------------------------------------------------------

/// Server interceptor that echoes specific request headers back in the
/// response, as required by the gRPC interop `custom_metadata` test.
///
/// - `x-grpc-test-echo-initial` → echoed in response headers
/// - `x-grpc-test-echo-trailing-bin` → echoed in response trailers
#[derive(Clone)]
struct EchoMetadataInterceptor;

impl Intercept for EchoMetadataInterceptor {
    async fn intercept(
        &self,
        headers: RequestHeaders,
        options: CallOptions,
        tx: &mut impl SendStream,
        rx: impl RecvStream + 'static,
        next: &impl Handle,
    ) -> Trailers {
        // Extract echo values from request metadata.
        let echo_initial: Option<MetadataValue<Ascii>> = headers
            .metadata()
            .get("x-grpc-test-echo-initial")
            .cloned();
        let echo_trailing: Option<MetadataValue<Binary>> = headers
            .metadata()
            .get_bin("x-grpc-test-echo-trailing-bin")
            .cloned();

        // Wrap tx to inject echo-initial into response headers.
        let mut echo_tx = EchoSendStream {
            inner: tx,
            echo_initial,
        };

        // Run the handler with the wrapped stream.
        let mut trailers = next.handle(headers, options, &mut echo_tx, rx).await;

        // Add echo-trailing-bin to trailers.
        if let Some(val) = echo_trailing {
            trailers
                .metadata_mut()
                .insert_bin("x-grpc-test-echo-trailing-bin", val);
        }

        trailers
    }
}

/// SendStream wrapper that injects echo-initial into response headers.
struct EchoSendStream<'a, S: SendStream> {
    inner: &'a mut S,
    echo_initial: Option<MetadataValue<Ascii>>,
}

impl<S: SendStream> SendStream for EchoSendStream<'_, S> {
    async fn send<'a>(
        &mut self,
        item: ServerResponseStreamItem<'a>,
        options: SendOptions,
    ) -> Result<(), ()> {
        match item {
            ServerResponseStreamItem::Headers(mut h) => {
                if let Some(val) = self.echo_initial.take() {
                    h.metadata_mut()
                        .insert("x-grpc-test-echo-initial", val);
                }
                self.inner
                    .send(ServerResponseStreamItem::Headers(h), options)
                    .await
            }
            other => self.inner.send(other, options).await,
        }
    }
}

// ---------------------------------------------------------------------------
// TestServiceImpl (Service registration)
// ---------------------------------------------------------------------------

struct TestServiceImpl;

impl Service for TestServiceImpl {
    fn descriptor(&self) -> ServiceDescriptor {
        ServiceDescriptor::new(
            "grpc.testing.TestService",
            vec![
                MethodDescriptor::new(
                    "/grpc.testing.TestService/EmptyCall",
                    MethodType::Unary,
                ),
                MethodDescriptor::new(
                    "/grpc.testing.TestService/UnaryCall",
                    MethodType::Unary,
                ),
                MethodDescriptor::new(
                    "/grpc.testing.TestService/UnimplementedCall",
                    MethodType::Unary,
                ),
                MethodDescriptor::new(
                    "/grpc.testing.TestService/StreamingOutputCall",
                    MethodType::ServerStreaming,
                ),
                MethodDescriptor::new(
                    "/grpc.testing.TestService/StreamingInputCall",
                    MethodType::ClientStreaming,
                ),
                MethodDescriptor::new(
                    "/grpc.testing.TestService/FullDuplexCall",
                    MethodType::BidiStreaming,
                ),
            ],
        )
    }

    fn register_methods(self, registrar: &mut impl ServiceRegistrar) {
        registrar.register_method(
            MethodDescriptor::new(
                "/grpc.testing.TestService/EmptyCall",
                MethodType::Unary,
            ),
            EmptyCallHandler,
        );
        registrar.register_method(
            MethodDescriptor::new(
                "/grpc.testing.TestService/UnaryCall",
                MethodType::Unary,
            ),
            UnaryCallHandler,
        );
        registrar.register_method(
            MethodDescriptor::new(
                "/grpc.testing.TestService/UnimplementedCall",
                MethodType::Unary,
            ),
            UnimplementedCallHandler,
        );
        registrar.register_method(
            MethodDescriptor::new(
                "/grpc.testing.TestService/StreamingOutputCall",
                MethodType::ServerStreaming,
            ),
            StreamingOutputCallHandler,
        );
        registrar.register_method(
            MethodDescriptor::new(
                "/grpc.testing.TestService/StreamingInputCall",
                MethodType::ClientStreaming,
            ),
            StreamingInputCallHandler,
        );
        registrar.register_method(
            MethodDescriptor::new(
                "/grpc.testing.TestService/FullDuplexCall",
                MethodType::BidiStreaming,
            ),
            FullDuplexCallHandler,
        );
    }
}

// ---------------------------------------------------------------------------
// Server entry point
// ---------------------------------------------------------------------------

pub async fn run_server(
    addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let creds = Arc::new(InsecureServerCredentials::new());
    let rt = grpc::rt::default_runtime();

    let listener = HyperTransport::new_tcp_stream(addr, creds, &rt)
        .await
        .map_err(|e| format!("failed to bind listener: {e}"))?;

    println!("gRPC-native interop server listening on {}", addr);

    let server = Server::builder()
        .add_service(TestServiceImpl.with_interceptor(EchoMetadataInterceptor))
        .build();

    server
        .serve_with_shutdown(&listener, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await;

    Ok(())
}
