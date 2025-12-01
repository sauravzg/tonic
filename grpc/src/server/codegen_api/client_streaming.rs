use crate::server::message::AsMut;
use crate::server::stream::{PushStream, PushStreamProducer};
use crate::ServerStatus;

/// A trait for client streaming gRPC methods.
#[trait_variant::make(Send)]
pub trait ClientStreamingMethod<Req, Resp>
where
    Req: Send,
    Resp: AsMut + Send,
{
    /// Handles a client streaming request.
    async fn client_streaming<P>(
        &self,
        req: PushStream<Req, P>,
        resp: Resp::Mut<'_>,
    ) -> Result<(), ServerStatus>
    where
        P: PushStreamProducer<Item = Req> + Send;
}
