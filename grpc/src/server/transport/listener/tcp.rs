use super::Listener;
use std::io;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

/// A wrapper around `TcpListener` that implements `Listener`.
pub struct TcpListenerWrapper {
    inner: TcpListener,
}

impl TcpListenerWrapper {
    /// Creates a new `TcpListenerWrapper`.
    pub fn new(inner: TcpListener) -> Self {
        Self { inner }
    }
}

impl Listener for TcpListenerWrapper {
    type IO = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> Result<(Self::IO, Self::Addr), io::Error> {
        self.inner.accept().await
    }
}
