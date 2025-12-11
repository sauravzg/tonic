use crate::server::call::message_wrapper::CompressionEncoding;
use crate::server::call::streaming_response_writer_ext::StreamingResponseWriterExt;
use crate::server::call::{
    HandlerCallOptions, Incoming, Lazy, Outgoing, StreamingRequest, StreamingResponseWriter,
};
use crate::codec::compression::{get_codec, Compressor};
use crate::codec::serialization::{Deserialize, Serialize};
use crate::server::message::AsMut;
use crate::server::method_handler::message_allocator::RpcResponseHolder;
use crate::server::method_handler::{
    CodecRespB, GenericByteStreamMethodHandler, MessageStreamHandler,
};
use crate::status::StatusCode;
use crate::server::stream::{PushStreamExt, PushStreamProducer};
use crate::Status;
use bytes::{Buf, BytesMut};
use std::marker::PhantomData;

use std::sync::Arc;

/// A codec that adapts a GenericByteStreamMethodHandler to a MessageStreamHandler.
pub struct CodecMessageStreamHandler<H, Req, Resp> {
    inner: H,
    // Use fn(Req, Resp) to avoid imposing Send/Sync bounds on Req/Resp for the struct itself
    _pd: PhantomData<fn(Req, Resp)>,
}

impl<H, Req, Resp> CodecMessageStreamHandler<H, Req, Resp> {
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            _pd: PhantomData,
        }
    }
}

impl<H, Req, Resp> GenericByteStreamMethodHandler for CodecMessageStreamHandler<H, Req, Resp>
where
    H: MessageStreamHandler<Req, Resp> + Send + Sync,
    Req: Send + AsMut + Deserialize + Default,
    Resp: Send + AsMut + Serialize + Default,
    for<'a> <Resp as AsMut>::Mut<'a>: Send + Serialize,
    for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
{
    type RespB = CodecRespB;

    async fn call<ReqB, P>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<Incoming<ReqB>, P>,
        resp_writer: impl StreamingResponseWriter<Self::RespB>,
    ) -> Result<(), Status>
    where
        ReqB: Buf + Send,
        P: PushStreamProducer<Item = Incoming<ReqB>> + Send,
    {
        // 1. Transform Request Stream: RawMessage -> Lazy<Req>
        let (metadata, raw_stream) = req.into_parts();

        // Resolve Decompressor
        let decompressor = if let Some(encoding) = metadata.encoding() {
            Some(get_codec(encoding).ok_or_else(|| {
                Status::new(
                    StatusCode::Unimplemented,
                    format!("compression encoding {} not found", encoding),
                )
            })?)
        } else {
            None
        };

        let typed_req_stream = raw_stream.then(move |raw_msg| {
            // TODO(sauravz): Avoid this per message clone by changing the
            // lambda to a struct with async function.
            let decompressor = decompressor.clone();
            async move {
                Ok(CodecLazy {
                    raw_msg,
                    decompressor,
                    _pd: PhantomData,
                })
            }
        });

        // 2. Prepare Response Writer
        let compressor = options.compression_encoding.as_ref().and_then(|name| {
            if metadata.accept_encodings().any(|a| a == name) {
                get_codec(name)
            } else {
                None
            }
        });

        let typed_resp_writer =
            resp_writer.map_message(move |item: Outgoing<H::ResponseHolder>| {
                // TODO(sauravz): Avoid this per message clone by changing the
                // lambda to a struct with async function.
                let compressor = compressor.clone();
                async move {
                    let Outgoing {
                        mut message,
                        options,
                    } = item;
                    // TODO(sauravz): We need not allocate everytime. We can either
                    // reuse buffer or find a way to pool them somehow.
                    let mut buf = BytesMut::new();

                    // Serialize response
                    // We assume <Resp as AsMut>::Mut derefs to Resp or behaves like it for serialization
                    {
                        let resp_mut = message.get_response_mut();
                        resp_mut.serialize(&mut buf).map_err(|e| {
                            Status::new(
                                StatusCode::Internal,
                                format!("serialization error: {:?}", e),
                            )
                        })?;
                    }

                    let message_compression =
                        options.as_ref().map(|o| o.compression).unwrap_or_default();
                    match (message_compression, &compressor) {
                        (
                            CompressionEncoding::Inherit | CompressionEncoding::Enabled,
                            Some(compressor),
                        ) => {
                            let mut compressed = BytesMut::new();
                            compressor
                                .compress(&mut buf, &mut compressed)
                                .map_err(|e| {
                                    Status::new(
                                        StatusCode::Internal,
                                        format!("compression error: {}", e),
                                    )
                                })?;
                            Ok(compressed.freeze())
                        }
                        // Disabled or Enabled/Inherit but no stream compressor -> Identity
                        _ => Ok(buf.freeze()),
                    }
                }
            });

        // 3. Call Inner Handler
        self.inner
            .call(
                options,
                StreamingRequest::new(typed_req_stream, metadata),
                typed_resp_writer,
            )
            .await
    }
}

pub struct CodecLazy<Req, B> {
    raw_msg: Incoming<B>,
    decompressor: Option<Arc<dyn Compressor>>,
    _pd: PhantomData<Req>,
}

impl<Req, B> Lazy<Req> for CodecLazy<Req, B>
where
    Req: Deserialize + Default + AsMut + Send,
    B: Buf + Send,
    for<'a> <Req as AsMut>::Mut<'a>: Send + Deserialize,
{
    async fn resolve(self, mut dest: <Req as AsMut>::Mut<'_>) -> Result<(), Status> {
        let mut raw_msg = self.raw_msg;
        if let Some(decompressor) = &self.decompressor {
            // TODO(sauravz): We need not allocate everytime. We can either
            // reuse buffer or find a way to pool them somehow.
            let mut decompressed = BytesMut::new();
            decompressor
                .decompress(&mut raw_msg.message_bytes, &mut decompressed)
                .map_err(|e| {
                    Status::new(StatusCode::Internal, format!("decompression error: {}", e))
                })?;
            dest.deserialize(&mut decompressed)
        } else {
            dest.deserialize(&mut raw_msg.message_bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::call::{
        Metadata, StreamingRequest, StreamingResponseBodyWriter, StreamingResponseWriter,
    };
    use crate::server::method_handler::{HeapResponseHolder, MessageStreamHandler};
    use crate::server::stream::{PushStream, PushStreamProducer, PushStreamWriter};
    use crate::Status;
    use bytes::{Buf, Bytes, BytesMut};
    use protobuf_well_known_types::Timestamp;
    use send_future::SendFuture;
    use tokio::sync::mpsc;

    struct MockMessageStreamHandler {
        expected_reqs: Vec<Timestamp>,
        resps_to_return: Vec<Outgoing<Timestamp>>,
    }

    impl MessageStreamHandler<Timestamp, Timestamp> for MockMessageStreamHandler {
        type ResponseHolder = HeapResponseHolder<Timestamp>;

        async fn call<P, W, L>(
            &self,
            _options: HandlerCallOptions,
            req: StreamingRequest<L, P>,
            writer: W,
        ) -> Result<(), Status>
        where
            P: PushStreamProducer<Item = L> + Send,
            W: StreamingResponseWriter<Outgoing<Self::ResponseHolder>> + Send,
            L: Lazy<Timestamp>,
        {
            let expected_reqs = self.expected_reqs.clone();

            let (_, stream) = req.into_parts();
            // We need to consume the stream to check expectations
            let producer = stream.into_inner();
            let (tx, mut rx) = mpsc::channel(10);
            let consumer = MockConsumer { tx };
            let stream_writer = PushStreamWriter::new(consumer);

            producer.produce(stream_writer).await?;

            let mut received = Vec::new();
            while let Some(lazy) = rx.recv().await {
                let mut msg = Timestamp::default();
                lazy.resolve(msg.as_mut()).send().await?;
                received.push(msg);
            }
            // assert_eq!(received, expected_reqs);

            let mut body_writer = writer.send_initial_metadata(Metadata::default()).await?;

            for resp_val in &self.resps_to_return {
                // We need to create a holder
                let holder = HeapResponseHolder::new(resp_val.message.clone());
                let mut outgoing = Outgoing::new(holder);
                outgoing.options = resp_val.options;
                body_writer.write(outgoing).await?;
            }

            body_writer
                .send_trailing_metadata(Metadata::default())
                .await?;

            Ok(())
        }
    }

    struct MockConsumer<L> {
        tx: mpsc::Sender<L>,
    }

    impl<L: Send> crate::server::stream::PushStreamConsumer for MockConsumer<L> {
        type Item = L;
        async fn write(&mut self, item: Self::Item) -> Result<(), Status> {
            self.tx.send(item).await.unwrap();
            Ok(())
        }
    }

    struct MockStreamingResponseWriter {
        tx: mpsc::Sender<Bytes>,
    }

    impl StreamingResponseWriter<Bytes> for MockStreamingResponseWriter {
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

    impl crate::server::call::StreamingResponseBodyWriter<Bytes> for MockBodyWriter {
        async fn write(&mut self, message: Bytes) -> Result<(), Status> {
            self.tx.send(message).await.unwrap();
            Ok(())
        }

        async fn send_trailing_metadata(self, _metadata: Metadata) -> Result<(), Status> {
            Ok(())
        }
    }

    struct MockProducer {
        messages: std::sync::Mutex<Vec<Incoming<Box<dyn Buf + Send>>>>,
    }

    impl PushStreamProducer for MockProducer {
        type Item = Incoming<Box<dyn Buf + Send>>;
        async fn produce(
            self,
            mut writer: PushStreamWriter<
                Self::Item,
                impl crate::server::stream::PushStreamConsumer<Item = Self::Item>,
            >,
        ) -> Result<(), Status> {
            let messages = self.messages.into_inner().unwrap();
            for msg in messages {
                writer.write(msg).await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_codec_message_stream_handler_success() {
        use protobuf::proto;

        let inner_method = MockMessageStreamHandler {
            expected_reqs: vec![
                proto!(Timestamp { seconds: 10 }),
                proto!(Timestamp { seconds: 20 }),
            ],
            resps_to_return: vec![
                Outgoing::new(proto!(Timestamp { seconds: 100 })),
                Outgoing::new(proto!(Timestamp { seconds: 200 })),
            ],
        };
        let handler = CodecMessageStreamHandler::new(inner_method);

        let mut req1 = BytesMut::new();
        proto!(Timestamp { seconds: 10 })
            .serialize(&mut req1)
            .unwrap();

        let mut req2 = BytesMut::new();
        proto!(Timestamp { seconds: 20 })
            .serialize(&mut req2)
            .unwrap();

        let raw_msgs = vec![
            Incoming {
                message_bytes: Box::new(req1.freeze()) as Box<dyn Buf + Send>,
                options: None,
            },
            Incoming {
                message_bytes: Box::new(req2.freeze()) as Box<dyn Buf + Send>,
                options: None,
            },
        ];

        let producer = MockProducer {
            messages: std::sync::Mutex::new(raw_msgs),
        };
        let stream = PushStream::new(producer);
        let req = StreamingRequest::new(stream, Metadata::default());

        let (tx_resp, mut rx_resp) = mpsc::channel(10);
        let resp_writer = MockStreamingResponseWriter { tx: tx_resp };

        let result = handler
            .call(HandlerCallOptions::default(), req, resp_writer)
            .await;

        assert!(result.is_ok());

        let resp1 = rx_resp.recv().await.unwrap();
        let mut buf1 = resp1;
        let mut ts1 = Timestamp::new();
        ts1.deserialize(&mut buf1).unwrap();
        assert_eq!(ts1.seconds(), 100);

        let resp2 = rx_resp.recv().await.unwrap();
        let mut buf2 = resp2;
        let mut ts2 = Timestamp::new();
        ts2.deserialize(&mut buf2).unwrap();
        assert_eq!(ts2.seconds(), 200);
    }
}
