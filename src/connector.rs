//! Connector to a Redis server.
//!
//! For more information, see the [`Connector`](Connector) trait.

use std::fmt::Debug;
use std::marker::Unpin;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::{Path, PathBuf};

use futures_core::future::BoxFuture;
use tokio::io::{self, AsyncRead, AsyncWrite};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{lookup_host, TcpStream};

/// Connector to a Redis server.
///
/// Redis servers accept connection in various way
/// including TCP and Unix domain socket.
/// A `Connector` should have enough information to connect to a certain Redis server
/// and should be capable of producing multiple connections to it.
///
/// Used by the [`Client`](crate::Client) to make pooled connections.
pub trait Connector: Send + Sync {
    /// Connection stream this connector produces.
    type Stream: AsyncRead + AsyncWrite + Debug + Unpin + Send;

    /// Connect to the Redis server and return the stream to it.
    fn connect(&self) -> BoxFuture<'_, io::Result<Self::Stream>>;
}

/// TCP socket connector.
#[derive(Debug)]
pub struct TcpConnector {
    addr: SocketAddr,
}

/// Unix domain socket connector.
#[cfg(unix)]
#[derive(Debug)]
pub struct UnixConnector {
    path: PathBuf,
}

/// DNS lookup error
#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    /// IO error during DNS lookup
    #[error("IO error during DNS lookup")]
    Io(#[from] std::io::Error),
    /// DNS record is not found
    #[error("DNS record not found")]
    NotFound,
}

impl TcpConnector {
    /// Constructs a `TcpConnector` using IP address and port.
    pub fn new(addr: SocketAddr) -> Self {
        TcpConnector { addr }
    }

    /// Constructs a `TcpConnector` from a string representation of a socket address
    /// like `example.com:8080`, `localhost:6379`, or `192.168.0.7:18080`.
    pub async fn lookup(addr: &str) -> Result<Self, LookupError> {
        let addr = lookup_host(addr)
            .await?
            .next()
            .ok_or(LookupError::NotFound)?;
        Ok(TcpConnector::new(addr))
    }
}

impl Connector for TcpConnector {
    type Stream = TcpStream;

    fn connect(&self) -> BoxFuture<'_, io::Result<Self::Stream>> {
        Box::pin(TcpStream::connect(self.addr))
    }
}

#[cfg(unix)]
impl UnixConnector {
    /// Constructs a `UnixConnector`.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        UnixConnector {
            path: path.as_ref().to_owned(),
        }
    }
}

#[cfg(unix)]
impl Connector for UnixConnector {
    type Stream = UnixStream;

    fn connect(&self) -> BoxFuture<'_, io::Result<Self::Stream>> {
        Box::pin(UnixStream::connect(&self.path))
    }
}
