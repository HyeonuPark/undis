use std::ops;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use deadpool::managed::{self, Object, Pool, PoolError, TimeoutType};
use serde::{de::DeserializeOwned, Serialize};

use crate::connection::{self, Connection as RawConnection};
use crate::connector::{Connector, LookupError, TcpConnector};
use crate::resp3::{de, value::Value};

pub mod hash;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Client<T: Connector = TcpConnector> {
    pool: Pool<Manager<T>>,
}

#[derive(Debug)]
pub struct Connection<T: Connector>(Object<Manager<T>>);

#[derive(Debug)]
pub struct Builder {
    connection_limit: usize,
    acquire_timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    ping_timeout: Option<Duration>,
    auth: Option<(String, String)>,
    setname: Option<String>,
    select: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
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
    #[error("DNS lookup failed")]
    Lookup(#[from] LookupError),
}

#[derive(Debug)]
struct Manager<T> {
    connector: T,
    auth: Option<(String, String)>,
    setname: Option<String>,
    select: Option<u32>,
    ping_counter: AtomicU64,
    last_hello: RwLock<Option<Arc<Value>>>,
}

impl Client<TcpConnector> {
    /// Start client builder.
    ///
    /// The builder from this method is not limited to the `TcpConnector`.
    ///
    /// # Panic
    ///
    /// It panics if `connection_limit` is zero.
    pub fn builder(connection_limit: usize) -> Builder {
        assert_ne!(
            0, connection_limit,
            "Client needs to have at least one connection"
        );

        Builder {
            connection_limit,
            acquire_timeout: None,
            connect_timeout: None,
            ping_timeout: None,
            auth: None,
            setname: None,
            select: None,
        }
    }
}

impl<T: Connector> Client<T> {
    pub fn server_hello(&self) -> Arc<Value> {
        self.pool
            .manager()
            .last_hello
            .read()
            .unwrap() // propagate panic if occurred
            .clone()
            .unwrap() // hello response always exist after client start
    }

    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        let mut conn = self.connection().await?;
        let res = conn.raw_command(request).await?;
        Ok(res)
    }

    pub async fn connection(&self) -> Result<Connection<T>, Error> {
        match self.pool.get().await {
            Ok(conn) => Ok(Connection(conn)),
            Err(PoolError::Backend(err)) => Err(err),
            Err(PoolError::Timeout(TimeoutType::Wait)) => Err(ErrorKind::AcquireTimeout.into()),
            Err(PoolError::Timeout(TimeoutType::Create | TimeoutType::Recycle)) => {
                Err(ErrorKind::ConnectionTimeout.into())
            }
            Err(
                PoolError::Closed
                | PoolError::NoRuntimeSpecified
                | PoolError::PostCreateHook(_)
                | PoolError::PreRecycleHook(_)
                | PoolError::PostRecycleHook(_),
            ) => unreachable!(),
        }
    }
}

impl<T: Connector> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

impl<T: Connector> Connection<T> {
    pub fn detach(self) -> RawConnection<T::Stream> {
        Object::take(self.0)
    }
}

impl<T: Connector> ops::Deref for Connection<T> {
    type Target = RawConnection<T::Stream>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<T: Connector> ops::DerefMut for Connection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl Builder {
    pub async fn bind(self, addr: &str) -> Result<Client<TcpConnector>, Error> {
        self.build(
            TcpConnector::lookup(addr)
                .await
                .map_err(ErrorKind::Lookup)?,
        )
        .await
    }

    pub async fn build<T: Connector>(self, connector: T) -> Result<Client<T>, Error> {
        let manager = Manager {
            connector,
            auth: self.auth,
            setname: self.setname,
            select: self.select,
            ping_counter: AtomicU64::new(0),
            last_hello: RwLock::default(),
        };
        let pool = Pool::builder(manager)
            .runtime(deadpool::Runtime::Tokio1)
            .max_size(self.connection_limit)
            .wait_timeout(self.acquire_timeout)
            .create_timeout(self.connect_timeout)
            .recycle_timeout(self.ping_timeout)
            .build();
        let pool = match pool {
            Ok(pool) => pool,
            Err(managed::BuildError::Backend(err)) => return Err(err),
            Err(managed::BuildError::NoRuntimeSpecified(_)) => unreachable!(),
        };

        let client = Client { pool };
        // to validate connection info and fetch server hello
        client.connection().await?;

        Ok(client)
    }

    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = Some(timeout);
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    pub fn ping_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = Some(timeout);
        self
    }

    pub fn auth(mut self, username: &str, password: &str) -> Self {
        self.auth = Some((username.into(), password.into()));
        self
    }

    pub fn setname(mut self, clientname: &str) -> Self {
        self.setname = Some(clientname.into());
        self
    }

    pub fn select(mut self, db: u32) -> Self {
        self.select = Some(db);
        self
    }
}

#[async_trait]
impl<T: Connector> managed::Manager for Manager<T> {
    type Type = RawConnection<T::Stream>;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let conn = self.connector.connect().await?;
        let (conn, hello) = RawConnection::with_args(
            conn,
            self.auth
                .as_ref()
                .map(|(username, password)| (&username[..], &password[..])),
            self.setname.as_deref(),
            self.select,
        )
        .await?;
        let hello = Arc::new(hello);
        *self.last_hello.write().unwrap() = Some(hello);
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
}

impl From<de::Error> for Error {
    fn from(err: de::Error) -> Self {
        connection::Error::from(err).into()
    }
}

impl From<connection::Error> for Error {
    fn from(err: connection::Error) -> Self {
        ErrorKind::from(err).into()
    }
}

impl From<ErrorKind> for Error {
    fn from(err: ErrorKind) -> Self {
        Box::new(err).into()
    }
}
