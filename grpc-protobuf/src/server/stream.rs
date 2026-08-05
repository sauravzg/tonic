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

//! Handler-facing streaming message types, used by generated server code.
//!
//! These are the server-side analogues of the client's
//! [`GrpcStreamingRequest`](crate::GrpcStreamingRequest) /
//! [`GrpcStreamingResponse`](crate::GrpcStreamingResponse), with mirror-image
//! roles: on the server, [`GrpcStreamingRequest`] is the *incoming request
//! source* and [`GrpcStreamingResponse`] is the *outgoing response sink*.
//!
//! Both borrow the transport streams the runtime already erased (`&mut dyn
//! DynRecvStream` / `&mut dyn DynSendStream`), so they add no per-RPC
//! allocation on top of the runtime's dispatch.

use std::marker::PhantomData;

use grpc::server_internal::BoxedRecvStream;
use grpc::server_internal::DynSendStream;
use grpc::server_internal::RecvStream;
use grpc::server_internal::ResponseStreamItem;
use grpc::server_internal::SendOptions;
use protobuf::AsView;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MutProxied;
use protobuf::Proxied;
use protobuf::Serialize;

use crate::ProtoRecvMessage;
use crate::ProtoSendMessage;

/// The server's incoming request message stream (server-side analogue of the
/// client's `GrpcStreamingResponse`).
///
/// Used by generated client-streaming and bidi handlers to receive request
/// messages.  It **owns** the runtime's already-erased receive stream (moved in
/// from `DynHandle::dyn_handle`), so it carries no lifetime and can be moved
/// into a spawned task.  Broken-stream behavior is *drop semantics*:
/// [`recv`](Self::recv) returns `None` for any stream end (client half-close,
/// cancellation, or transport fault); the real teardown of a handler whose peer
/// has gone is the runtime dropping the handler future.
pub struct GrpcStreamingRequest<Req> {
    rx: BoxedRecvStream,
    _pd: PhantomData<Req>,
}

impl<Req> GrpcStreamingRequest<Req>
where
    Req: Message + Default,
    for<'b> <Req as MutProxied>::Mut<'b>: ClearAndParse + Send + Sync,
{
    /// Takes ownership of an already-erased receive stream.  Not part of the
    /// public API; used by the generated adapters in this crate.
    pub(crate) fn new(rx: BoxedRecvStream) -> Self {
        Self {
            rx,
            _pd: PhantomData,
        }
    }

    /// Receives the next request message, or `None` once there are no more
    /// (client half-close, cancellation, or transport fault all collapse to
    /// `None`).
    pub async fn recv(&mut self) -> Option<Req> {
        let mut req = <Req as Default>::default();
        {
            let mut recv = ProtoRecvMessage::from_mut(&mut req);
            match self.rx.next(&mut recv).await {
                Some(Ok(())) => {}
                _ => return None,
            }
        }
        Some(req)
    }
}

/// The server's outgoing response message sink (server-side analogue of the
/// client's `GrpcStreamingRequest`).
///
/// Used by generated server-streaming and bidi handlers to send response
/// messages.  [`send`](Self::send) is a thin pass-through over the core send
/// stream: `Err(())` means the stream is gone (drop semantics — the handler may
/// stop early or ignore it).
pub struct GrpcStreamingResponse<'a, Resp> {
    tx: &'a mut dyn DynSendStream,
    _pd: PhantomData<Resp>,
}

impl<'a, Resp> GrpcStreamingResponse<'a, Resp>
where
    Resp: Message,
    for<'b> <Resp as Proxied>::View<'b>: Serialize + Send + Sync,
{
    /// Wraps an already-erased send stream.  Not part of the public API; used by
    /// the generated adapters in this crate.
    pub(crate) fn new(tx: &'a mut dyn DynSendStream) -> Self {
        Self {
            tx,
            _pd: PhantomData,
        }
    }

    /// Sends `resp` on the response stream.  Returns `Err(())` if the stream has
    /// already ended.
    pub async fn send(&mut self, resp: &impl AsView<Proxied = Resp>) -> Result<(), ()> {
        let msg = ProtoSendMessage::from_view(resp);
        self.tx
            .dyn_send(ResponseStreamItem::Message(&msg), SendOptions::default())
            .await
    }
}
