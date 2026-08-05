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
use grpc::server_internal::Trailers;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MutProxied;
use protobuf::Proxied;
use protobuf::Serialize;

use crate::Status;
use crate::server::stream::GrpcStreamingRequest;
use crate::server::stream::GrpcStreamingResponse;
use crate::trailers_conv::trailers_from_status;

/// A single bidirectional-streaming method (a stream of requests in, a stream of
/// responses out, interleaved).
pub trait BidiStreamingMethod: Send + Sync + 'static {
    type Request: Message + Default;
    type Response: Message + Default;

    fn call(
        &self,
        requests: GrpcStreamingRequest<Self::Request>,
        responses: GrpcStreamingResponse<'_, Self::Response>,
    ) -> impl Future<Output = Status> + Send;
}

/// Generic [`DynHandle`] adapter that dispatches a [`BidiStreamingMethod`].
pub struct BidiStreamingAdapter<M: BidiStreamingMethod> {
    method: M,
}

impl<M: BidiStreamingMethod> BidiStreamingAdapter<M> {
    pub fn new(method: M) -> Self {
        Self { method }
    }
}

#[async_trait]
impl<M> DynHandle for BidiStreamingAdapter<M>
where
    M: BidiStreamingMethod,
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
        // The request stream owns `rx`; the response sink borrows `tx`.  They are
        // independent, so a handler can freely interleave receives and sends.
        let requests = GrpcStreamingRequest::new(rx);
        let responses = GrpcStreamingResponse::new(tx);
        trailers_from_status(self.method.call(requests, responses).await)
    }
}
