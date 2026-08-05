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
 */

use std::future::Future;

use async_trait::async_trait;
use grpc::client::CallOptions;
use grpc::server_internal::BoxedRecvStream;
use grpc::server_internal::DynHandle;
use grpc::server_internal::DynSendStream;
use grpc::server_internal::RecvStream;
use grpc::server_internal::RequestHeaders;
use grpc::server_internal::ResponseStreamItem;
use grpc::server_internal::SendOptions;
use grpc::server_internal::Trailers;
use protobuf::AsMut;
use protobuf::AsView;
use protobuf::ClearAndParse;
use protobuf::Message;
use protobuf::MutProxied;
use protobuf::Proxied;
use protobuf::Serialize;

use crate::ProtoRecvMessage;
use crate::ProtoSendMessage;
use crate::Status;
use crate::StatusError;
use crate::status::StatusCodeError;
use crate::trailers_conv::trailers_from_status;

pub trait UnaryMethod: Send + Sync + 'static {
    type Request: Message + Default;
    type Response: Message + Default;

    fn call(
        &self,
        request: <Self::Request as Proxied>::View<'_>,
        response: <Self::Response as MutProxied>::Mut<'_>,
    ) -> impl Future<Output = Status> + Send;
}

pub struct UnaryAdapter<M: UnaryMethod> {
    method: M,
}

impl<M: UnaryMethod> UnaryAdapter<M> {
    pub fn new(method: M) -> Self {
        Self { method }
    }
}

#[async_trait]
impl<M> DynHandle for UnaryAdapter<M>
where
    M: UnaryMethod,
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

        let mut resp = <M::Response as Default>::default();
        let status = self.method.call(req.as_view(), resp.as_mut()).await;

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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use bytes::Buf;
    use bytes::Bytes;
    use grpc::core::RecvMessage;
    use grpc::server_internal::RequestHeaders;
    use protobuf::ClearAndParse;
    use protobuf::Serialize;
    use protobuf_well_known_types::Any;

    use grpc::server_internal::SendStream;

    use super::*;

    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut fut = std::pin::pin!(fut);
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    struct OneMsg(Option<Bytes>);

    impl RecvStream for OneMsg {
        async fn next(&mut self, msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
            let mut bytes = self.0.take()?;
            Some(msg.decode(&mut bytes).map_err(|_| ()))
        }
    }

    struct NoMsg;

    impl RecvStream for NoMsg {
        async fn next(&mut self, _msg: &mut dyn RecvMessage) -> Option<Result<(), ()>> {
            None
        }
    }

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

    fn encode(msg: &Any) -> Bytes {
        Bytes::from(msg.serialize().expect("serialize should succeed"))
    }

    struct EchoMethod;

    impl UnaryMethod for EchoMethod {
        type Request = Any;
        type Response = Any;

        async fn call(
            &self,
            request: <Any as Proxied>::View<'_>,
            mut response: <Any as MutProxied>::Mut<'_>,
        ) -> Status {
            response.set_type_url(request.type_url());
            Ok(())
        }
    }

    struct FailMethod;

    impl UnaryMethod for FailMethod {
        type Request = Any;
        type Response = Any;

        async fn call(
            &self,
            _request: <Any as Proxied>::View<'_>,
            _response: <Any as MutProxied>::Mut<'_>,
        ) -> Status {
            Err(StatusError::new(StatusCodeError::NotFound, "nope"))
        }
    }

    #[test]
    fn unary_happy_path_echoes_and_returns_ok() {
        let mut req = Any::new();
        req.set_type_url("type.googleapis.com/echo");

        let adapter = UnaryAdapter::<EchoMethod>::new(EchoMethod);
        let mut tx = Captured::default();
        let rx = OneMsg(Some(encode(&req)));

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
            .expect("response should parse");
        assert_eq!(got.type_url(), "type.googleapis.com/echo");
    }

    #[test]
    fn unary_error_status_produces_trailers_and_no_message() {
        let adapter = UnaryAdapter::<FailMethod>::new(FailMethod);
        let mut tx = Captured::default();
        let rx = OneMsg(Some(encode(&Any::new())));

        let trailers = block_on(adapter.dyn_handle(
            RequestHeaders::new(),
            CallOptions::default(),
            &mut tx,
            BoxedRecvStream(Box::new(rx)),
        ));

        let err = trailers.status().as_ref().unwrap_err();
        assert_eq!(err.code(), grpc::StatusCodeError::NotFound);
        assert!(tx.messages.is_empty());
    }

    #[test]
    fn unary_missing_request_is_internal_error() {
        let adapter = UnaryAdapter::<EchoMethod>::new(EchoMethod);
        let mut tx = Captured::default();

        let trailers = block_on(adapter.dyn_handle(
            RequestHeaders::new(),
            CallOptions::default(),
            &mut tx,
            BoxedRecvStream(Box::new(NoMsg)),
        ));

        let err = trailers.status().as_ref().unwrap_err();
        assert_eq!(err.code(), grpc::StatusCodeError::Internal);
        assert!(tx.messages.is_empty());
    }
}
