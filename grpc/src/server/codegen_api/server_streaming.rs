use crate::server::message::AsView;
use crate::server::stream::{PushStreamConsumer, PushStreamWriter};
use crate::ServerStatus;

/// A trait for server streaming gRPC methods.
#[trait_variant::make(Send)]
pub trait ServerStreamingMethod<Req, Resp>
where
    Req: AsView + Send,
    Resp: Send,
{
    /// Handles a server streaming request.
    async fn server_streaming<C>(
        &self,
        req: Req::View<'_>,
        writer: PushStreamWriter<Resp, C>,
    ) -> Result<(), ServerStatus>
    where
        C: PushStreamConsumer<Item = Resp> + Send;
}
