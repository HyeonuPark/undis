//! Connector to the Redis server.
//!
//! For more information, see the [`Connector`](self::Connector) trait.

use std::fmt::Debug;
use std::marker::Unpin;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::{Path, PathBuf};

pub use async_trait::async_trait;
use tokio::io::{self, AsyncRead, AsyncWrite};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{lookup_host, TcpStream};

/// Connector to the Redis server.
///
/// Redis server accepts connection in various way
/// including TCP and Unix domain socket.
/// A Connector has enough information to connect to certain Redis server
/// and may produces multiple connections to it.
///
/// It is used by the [`Client`](crate::Client) to make pooled connections.
///
/// ## Implement your own connector
///
/// This trait uses [`async_trait`](self::async_trait) to abstract over async operation.
/// At this point the rustdoc generates not-so-pretty document for it.
/// If you haven't used it before, it would be better to check its own documentation first.
#[async_trait]
pub trait Connector: Send + Sync {
    /// Connection stream this connector produces.
    type Stream: AsyncRead + AsyncWrite + Debug + Unpin + Send;

    /// Connect to the Redis server and return the stream to it.
    async fn connect(&self) -> io::Result<Self::Stream>;
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
    /// Create TCP socket connector using IP address + port.
    pub fn new(addr: SocketAddr) -> Self {
        TcpConnector { addr }
    }

    /// Create TCP socket connector from address string
    /// like `example.com:8080`, `localhost:6379`, or `192.168.0.7:18080`.
    pub async fn lookup(addr: &str) -> Result<Self, LookupError> {
        let addr = lookup_host(addr)
            .await?
            .next()
            .ok_or(LookupError::NotFound)?;
        Ok(TcpConnector::new(addr))
    }
}

#[async_trait]
impl Connector for TcpConnector {
    type Stream = TcpStream;

    async fn connect(&self) -> io::Result<Self::Stream> {
        Ok(TcpStream::connect(self.addr).await?)
    }
}

#[cfg(unix)]
impl UnixConnector {
    /// Create unix domain socket connector.
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        UnixConnector {
            path: path.as_ref().to_owned(),
        }
    }
}

#[cfg(unix)]
#[async_trait]
impl Connector for UnixConnector {
    type Stream = UnixStream;

    async fn connect(&self) -> io::Result<Self::Stream> {
        Ok(UnixStream::connect(&self.path).await?)
    }
}
