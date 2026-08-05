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
use grpc::server_internal::RequestHeaders;
use grpc::server_internal::ResponseStreamItem;
use grpc::server_internal::SendOptions;
use grpc::server_internal::Trailers;
use protobuf::AsMut;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MutProxied;
use protobuf::Proxied;
use protobuf::Serialize;

use crate::ProtoSendMessage;
use crate::Status;
use crate::server::stream::GrpcStreamingRequest;
use crate::trailers_conv::trailers_from_status;

/// A single client-streaming method (a stream of requests in, one response out).
pub trait ClientStreamingMethod: Send + Sync + 'static {
    type Request: Message + Default;
    type Response: Message + Default;

    fn call(
        &self,
        requests: GrpcStreamingRequest<Self::Request>,
        response: <Self::Response as MutProxied>::Mut<'_>,
    ) -> impl Future<Output = Status> + Send;
}

/// Generic [`DynHandle`] adapter that dispatches a [`ClientStreamingMethod`].
pub struct ClientStreamingAdapter<M: ClientStreamingMethod> {
    method: M,
}

impl<M: ClientStreamingMethod> ClientStreamingAdapter<M> {
    pub fn new(method: M) -> Self {
        Self { method }
    }
}

#[async_trait]
impl<M> DynHandle for ClientStreamingAdapter<M>
where
    M: ClientStreamingMethod,
    for<'a> <M::Request as MutProxied>::Mut<'a>: ClearAndParse + Send + Sync,
    for<'a> <M::Response as Proxied>::View<'a>: Serialize + Send + Sync,
{
    async fn dyn_handle(
        &self,
        _headers: RequestHeaders,
        _options: CallOptions,
        tx: &mut dyn DynSendStream,
        rx: BoxedRecvStream,
    ) -> Trailers {
        let requests = GrpcStreamingRequest::new(rx);
        let mut resp = <M::Response as Default>::default();
        let status = self.method.call(requests, resp.as_mut()).await;

        if status.is_ok() {
            let send = ProtoSendMessage::from_view(&resp);
            let mut options = SendOptions::default();
            options.final_msg = true;
            let _ = tx
                .dyn_send(ResponseStreamItem::Message(&send), options)
                .await;
        }

        trailers_from_status(status)
    }
}
