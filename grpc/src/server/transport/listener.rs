mod tcp;

pub use tcp::TcpListenerWrapper;

use std::io;
use tokio::io::{AsyncRead, AsyncWrite};

/// A trait for accepting incoming connections.
#[trait_variant::make(Send)]
pub trait Listener {
    /// The I/O type for the accepted connection.
    type IO: AsyncRead + AsyncWrite + Send + Unpin + 'static;
    /// The address type for the accepted connection.
    type Addr: Send + Sync + 'static;

    /// Accepts a new connection.
    async fn accept(&mut self) -> Result<(Self::IO, Self::Addr), io::Error>;
}
