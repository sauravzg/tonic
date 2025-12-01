use crate::server::message::{AsMut, AsView};
use crate::{ServerStatus, Status};

/// A trait for unary gRPC methods.
#[trait_variant::make(Send)]
pub trait UnaryMethod<Req, Resp>
where
    Req: AsView + Send,
    Resp: AsMut + Send,
{
    /// Handles a unary request.
    async fn unary(&self, req: Req::View<'_>, resp: Resp::Mut<'_>) -> Result<(), ServerStatus>;
}
