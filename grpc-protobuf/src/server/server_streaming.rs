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

use std::future::Future;

use async_trait::async_trait;
use grpc::client::CallOptions;
use grpc::server_internal::BoxedRecvStream;
use grpc::server_internal::DynHandle;
use grpc::server_internal::DynSendStream;
use grpc::server_internal::RecvStream;
use grpc::server_internal::RequestHeaders;
use grpc::server_internal::Trailers;
use protobuf::AsView;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MutProxied;
use protobuf::Proxied;
use protobuf::Serialize;

use crate::ProtoRecvMessage;
use crate::Status;
use crate::StatusError;
use crate::server::stream::GrpcStreamingResponse;
use crate::status::StatusCodeError;
use crate::trailers_conv::trailers_from_status;

/// A single server-streaming method (one request in, a stream of responses out).
///
/// Implemented by generated marker structs; the service is held privately by the
/// concrete type and `call` takes `&self`.
pub trait ServerStreamingMethod: Send + Sync + 'static {
    type Request: Message + Default;
    type Response: Message + Default;

    fn call(
        &self,
        request: <Self::Request as Proxied>::View<'_>,
        responses: GrpcStreamingResponse<'_, Self::Response>,
    ) -> impl Future<Output = Status> + Send;
}

/// Generic [`DynHandle`] adapter that dispatches a [`ServerStreamingMethod`].
///
/// Implements the object-safe [`DynHandle`] directly (rather than [`Handle`])
/// so it receives the runtime's already-erased streams by value/reference and
/// avoids re-boxing.
///
/// [`Handle`]: grpc::server_internal::Handle
pub struct ServerStreamingAdapter<M: ServerStreamingMethod> {
    method: M,
}

impl<M: ServerStreamingMethod> ServerStreamingAdapter<M> {
    pub fn new(method: M) -> Self {
        Self { method }
    }
}

#[async_trait]
impl<M> DynHandle for ServerStreamingAdapter<M>
where
    M: ServerStreamingMethod,
    for<'a> <M::Request as MutProxied>::Mut<'a>: ClearAndParse + Send + Sync,
    for<'a> <M::Response as Proxied>::View<'a>: Serialize + Send + Sync,
{
    async fn dyn_handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut dyn DynSendStream,
        mut rx: BoxedRecvStream,
    ) -> Trailers {
        let mut req = <M::Request as Default>::default();
        {
            let mut recv = ProtoRecvMessage::from_mut(&mut req);
            match rx.next(&mut recv).await {
                Some(Ok(())) => {}
                _ => {
                    return trailers_from_status(Err(StatusError::new(
                        StatusCodeError::Internal,
                        "client did not send a request message",
                    )));
                }
            }
        }

        let responses = GrpcStreamingResponse::new(tx);
        let status = self.method.call(req.as_view(), responses).await;
        trailers_from_status(status)
    }
}
