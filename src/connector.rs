use std::fmt::Debug;
use std::marker::Unpin;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

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

impl TcpConnector {
    pub fn new(addr: SocketAddr) -> Self {
        TcpConnector { addr }
    }
}

#[async_trait]
impl Connector for TcpConnector {
    type Connection = TcpStream;

    async fn connect(&self) -> Result<Self::Connection, Error> {
        Ok(TcpStream::connect(self.addr).await?)
    }
}
