use crate::server::call::{Incoming, StreamingResponseWriter};
use crate::server::method_handler::ByteStreamMethodHandler;
use crate::server::stream::PushStreamProducer;
use crate::server::transport::listener::Listener;
use crate::Status;
use bytes::Buf;

/// A trait for server transports.
#[trait_variant::make(Send)]
pub trait ServerTransport {
    /// The request body type.
    type ReqB: Buf + Send + 'static;

    /// The writer type for sending responses.
    type Writer<RespB>: StreamingResponseWriter<RespB> + Send + 'static
    where
        RespB: Buf + Send + 'static;

    /// The producer type for the request stream.
    type Producer: PushStreamProducer<Item = Incoming<Self::ReqB>> + Send + Sync + 'static;

    /// Serves a connection.
    async fn serve<L, H, RespB>(self, listener: L, handler: H) -> Result<(), Status>
    where
        L: Listener + Send,
        H: ByteStreamMethodHandler<Self::ReqB, Self::Writer<RespB>, Self::Producer, RespB = RespB>
            + Send
            + Sync
            + 'static,
        RespB: Buf + Send + 'static;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::streaming_response_writer::StreamingResponseBodyWriter;
    use crate::server::call::{HandlerCallOptions, Incoming, Metadata, StreamingRequest};
    use crate::server::stream::{PushStream, PushStreamProducer, PushStreamWriter};
    use bytes::Bytes;
    use tokio::sync::mpsc;

    struct MockTransport;

    struct MockWriter {
        tx: mpsc::Sender<Bytes>,
    }

    impl<RespB> StreamingResponseWriter<RespB> for MockWriter
    where
        RespB: Buf + Send + 'static,
    {
        type BodyWriter = MockBodyWriter;
        async fn send_initial_metadata(
            self,
            _metadata: Metadata,
        ) -> Result<Self::BodyWriter, Status> {
            Ok(MockBodyWriter { tx: self.tx })
        }
    }

    struct MockBodyWriter {
        tx: mpsc::Sender<Bytes>,
    }

    impl<RespB> StreamingResponseBodyWriter<RespB> for MockBodyWriter
    where
        RespB: Buf + Send + 'static,
    {
        async fn write(&mut self, mut message: RespB) -> Result<(), Status> {
            let bytes = message.copy_to_bytes(message.remaining());
            self.tx.send(bytes).await.unwrap();
            Ok(())
        }
        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    impl ServerTransport for MockTransport {
        type ReqB = Bytes;
        type Writer<RespB>
            = MockWriter
        where
            RespB: Buf + Send + 'static;
        type Producer = MockProducer;

        async fn serve<L, H, RespB>(self, _listener: L, handler: H) -> Result<(), Status>
        where
            L: Listener + Send,
            H: ByteStreamMethodHandler<
                    Self::ReqB,
                    Self::Writer<RespB>,
                    Self::Producer,
                    RespB = RespB,
                > + Send
                + Sync
                + 'static,
            RespB: Buf + Send + 'static,
        {
            // Simulate request
            let (tx, mut rx) = mpsc::channel(1);
            let writer = MockWriter { tx };

            let producer = MockProducer;
            let stream = PushStream::new(producer);
            let req = StreamingRequest::new(stream, Metadata::default());

            handler
                .call(HandlerCallOptions::default(), req, writer)
                .await?;
            rx.recv().await;
            Ok(())
        }
    }

    struct MockProducer;

    impl PushStreamProducer for MockProducer {
        type Item = Incoming<Bytes>;

        async fn produce(
            self,
            _writer: PushStreamWriter<
                Self::Item,
                impl crate::server::stream::PushStreamConsumer<Item = Self::Item>,
            >,
        ) -> Result<(), Status> {
            Ok(())
        }
    }

    struct MockListener;

    impl Listener for MockListener {
        type IO = tokio::net::TcpStream;
        type Addr = std::net::SocketAddr;

        async fn accept(&mut self) -> Result<(Self::IO, Self::Addr), std::io::Error> {
            Err(std::io::Error::other("mock"))
        }
    }

    struct MockHandler;

    impl ByteStreamMethodHandler<Bytes, MockWriter, MockProducer> for MockHandler {
        type RespB = Bytes;
        async fn call(
            &self,
            options: HandlerCallOptions,
            req: StreamingRequest<Incoming<Bytes>, MockProducer>,
            _resp: MockWriter,
        ) -> Result<(), Status> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_server_transport_serve() {
        let transport = MockTransport;
        let handler = MockHandler;
        let listener = MockListener;
        let result = transport.serve(listener, handler).await;
        assert!(result.is_ok());
    }
}
