use std::fmt::Debug;
use std::marker::Unpin;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{lookup_host, TcpStream};

use crate::connection::Error;

#[async_trait]
pub trait Connector: Send + Sync {
    type Connection: AsyncRead + AsyncWrite + Debug + Unpin + Send;

    async fn connect(&self) -> Result<Self::Connection, Error>;
}

#[derive(Debug)]
pub struct TcpConnector {
    addr: SocketAddr,
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
    type Connection = TcpStream;

    async fn connect(&self) -> Result<Self::Connection, Error> {
        Ok(TcpStream::connect(self.addr).await?)
    }
}
