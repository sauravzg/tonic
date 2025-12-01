use crate::{
    server::call::{HandlerCallOptions, Lazy, Outgoing, StreamingRequest, StreamingResponseWriter},
    server::message::AsMut,
    server::method_handler::RpcResponseHolder,
    server::stream::PushStreamProducer,
    Status,
};

/// A unified trait for all streaming gRPC methods.
#[trait_variant::make(Send)]
pub trait MessageStreamHandler<Req, Resp>
where
    Req: Send + AsMut,
    Resp: Send + AsMut,
{
    /// The response holder type produced by this handler.
    type ResponseHolder: RpcResponseHolder<Resp>;

    /// Handles a streaming request.
    async fn call<P, W, L>(
        &self,
        options: HandlerCallOptions,
        req: StreamingRequest<L, P>,
        writer: W,
    ) -> Result<(), Status>
    where
        P: PushStreamProducer<Item = L> + Send,
        W: StreamingResponseWriter<Outgoing<Self::ResponseHolder>> + Send,
        L: Lazy<Req>;
}
