use std::net::SocketAddr;
use std::sync::atomic::{self, AtomicU64};
use std::time::Duration;

use async_trait::async_trait;
use deadpool::managed::{self, Pool, PoolError, TimeoutType};
use once_cell::sync::OnceCell;
use serde::{de::DeserializeOwned, Serialize};

use crate::connection::{self, Connection};
use crate::connector::{Connector, TcpConnector};
use crate::resp3::value::Value;

pub mod serde_helper;

pub mod hash;

#[derive(Debug)]
pub struct Client<T: Connector> {
    pool: Pool<Manager<T>>,
}

#[derive(Debug)]
pub struct Builder {
    connection_limit: usize,
    acquire_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    ping_needed_timeout: Option<Duration>,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(#[from] pub Box<ErrorKind>);

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("connection error")]
    Connection(#[from] connection::Error),
    #[error("connection acquire timeout")]
    AcquireTimeout,
    #[error("connection timeout")]
    ConnectionTimeout,
    #[error("ping-pong failed")]
    PingPongFailed,
}

#[derive(Debug)]
struct Manager<T> {
    connector: T,
    ping_counter: AtomicU64,
    first_hello: OnceCell<Value>,
}

impl Client<TcpConnector> {
    pub fn builder(connection_limit: usize) -> Builder {
        Builder {
            connection_limit,
            acquire_timeout: None,
            connect_timeout: None,
            ping_needed_timeout: None,
        }
    }
}

impl<T: Connector> Client<T> {
    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: &Req,
    ) -> Result<Resp, Error> {
        let mut conn = match self.pool.get().await {
            Ok(conn) => conn,
            Err(PoolError::Backend(err)) => return Err(err),
            Err(PoolError::Timeout(TimeoutType::Wait)) => {
                return Err(ErrorKind::AcquireTimeout.into())
            }
            Err(PoolError::Timeout(TimeoutType::Create | TimeoutType::Recycle)) => {
                return Err(ErrorKind::ConnectionTimeout.into())
            }
            Err(
                PoolError::Closed
                | PoolError::NoRuntimeSpecified
                | PoolError::PostCreateHook(_)
                | PoolError::PreRecycleHook(_)
                | PoolError::PostRecycleHook(_),
            ) => unreachable!(),
        };

        conn.raw_command(request).await.map_err(|err| {
            // remove the connection with error
            let _ = managed::Object::take(conn);
            err.into()
        })
    }
}

impl Builder {
    pub async fn bind(self, addr: SocketAddr) -> Result<(Client<TcpConnector>, Value), Error> {
        self.build(TcpConnector::new(addr)).await
    }

    pub async fn build<T: Connector>(self, connector: T) -> Result<(Client<T>, Value), Error> {
        let manager = Manager {
            connector,
            ping_counter: AtomicU64::new(0),
            first_hello: OnceCell::new(),
        };
        let pool = Pool::builder(manager)
            .runtime(deadpool::Runtime::Tokio1)
            .max_size(self.connection_limit)
            .wait_timeout(self.acquire_timeout)
            .create_timeout(self.connect_timeout)
            .recycle_timeout(self.ping_needed_timeout)
            .build();
        let pool = match pool {
            Ok(pool) => pool,
            Err(managed::BuildError::Backend(err)) => return Err(err),
            Err(managed::BuildError::NoRuntimeSpecified(_)) => unreachable!(),
        };

        let client = Client { pool };
        let pong: String = client.raw_command(&("PING",)).await?;
        if pong != "PONG" {
            return Err(ErrorKind::PingPongFailed.into());
        }
        let hello = client.pool.manager().first_hello.get().unwrap().clone();

        Ok((client, hello))
    }

    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = Some(timeout);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn ping_needed_timeout(mut self, timeout: Duration) -> Self {
        self.ping_needed_timeout = Some(timeout);
        self
    }
}

#[async_trait]
impl<T: Connector> managed::Manager for Manager<T> {
    type Type = Connection<T::Connection>;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = self.connector.connect().await?;
        let (conn, hello) = Connection::new(conn).await?;
        let _ = self.first_hello.set(hello); // always setted after this point
        Ok(conn)
    }

    async fn recycle(&self, conn: &mut Self::Type) -> Result<(), managed::RecycleError<Error>> {
        let count = self.ping_counter.fetch_add(1, atomic::Ordering::Relaxed);
        let pong: u64 = conn
            .raw_command(&("PING", count))
            .await
            .map_err(Error::from)?;
        if pong != count {
            return Err(Error::from(ErrorKind::PingPongFailed).into());
        }
        Ok(())
    }

    fn detach(&self, _conn: &mut Self::Type) {
        // do nothing
    }
}

impl From<connection::Error> for Error {
    fn from(err: connection::Error) -> Self {
        Error(Box::new(err.into()))
    }
}

impl From<ErrorKind> for Error {
    fn from(err: ErrorKind) -> Self {
        Box::new(err).into()
    }
}
