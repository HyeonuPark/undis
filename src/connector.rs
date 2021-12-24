use std::fmt::Debug;
use std::marker::Unpin;
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{lookup_host, TcpStream};

use crate::connection::Error;

#[async_trait]
pub trait Connector: Send + Sync {
    type Stream: AsyncRead + AsyncWrite + Debug + Unpin + Send;

    async fn connect(&self) -> Result<Self::Stream, Error>;
}

#[derive(Debug)]
pub struct TcpConnector {
    addr: SocketAddr,
}

#[cfg(unix)]
pub struct UnixConnector {
    path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum LookupError {
    #[error("IO error during DNS lookup")]
    Io(#[from] std::io::Error),
    #[error("DNS record not found")]
    NotFound,
}

impl TcpConnector {
    pub fn new(addr: SocketAddr) -> Self {
        TcpConnector { addr }
    }

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

    async fn connect(&self) -> Result<Self::Stream, Error> {
        Ok(TcpStream::connect(self.addr).await?)
    }
}

#[cfg(unix)]
impl UnixConnector {
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

    async fn connect(&self) -> Result<Self::Stream, Error> {
        Ok(UnixStream::connect(&self.path).await?)
    }
}
