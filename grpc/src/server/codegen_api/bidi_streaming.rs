use crate::server::stream::{PushStream, PushStreamConsumer, PushStreamProducer, PushStreamWriter};
use crate::ServerStatus;

/// A trait for bidirectional streaming gRPC methods.
#[trait_variant::make(Send)]
pub trait BidiStreamingMethod<Req, Resp>
where
    Req: Send,
    Resp: Send,
{
    /// Handles a bidirectional streaming request.
    async fn bidi_streaming<P, C>(
        &self,
        req: PushStream<Req, P>,
        writer: PushStreamWriter<Resp, C>,
    ) -> Result<(), ServerStatus>
    where
        P: PushStreamProducer<Item = Req> + Send,
        C: PushStreamConsumer<Item = Resp> + Send;
}
