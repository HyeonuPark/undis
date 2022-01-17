//! Redis client.
//!
//! For more information, see the [`Client`](Client) type.

use std::num::NonZeroUsize;
use std::ops;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};

use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit as Permit, Semaphore};

use crate::command::{self, Command};
use crate::connection::{Connection as RawConnection, Error};
use crate::connector::{Connector, LookupError, TcpConnector};
use crate::resp3::{de, value::Value};

#[cfg(test)]
mod tests;

/// A Redis Client.
///
/// The `Client` can use various channels to connect to the Redis server.
/// By default it uses TCP, but UDS and custom streams are available.
/// Connection parameters used by the `Client` can be configured using the [`Builder`](Client::builder).
///
/// The `Client` initiate connection to the Redis server and manage a pool of it.
/// If you clone the `Client` it will share the pool with the previous one.
///
/// This type is a thin wrapper over the [`Arc<Command<ClientShared>>`](ClientShared) type.
/// You don't need to re-wrap it with [`Arc`](std::sync::Arc) or simmilar.
#[derive(Debug)]
pub struct Client<T: Connector = TcpConnector> {
    shared: Arc<Command<ClientShared<T>>>,
}

/// Internal shared structure behind the [`Client`](Client). You may not need to use it directly.
///
/// Main purpose to expose this type is to make `Client` `Deref<Target = Command<ClientShared<T>>>`.
/// You can use it to reduce indirection, though.
#[derive(Debug)]
pub struct ClientShared<T: Connector> {
    connector: T,
    init: Init,
    ping_counter: AtomicU64,
    server_hello: RwLock<Arc<Value>>,
    sender: Sender<Entry<RawConnection<T::Stream>>>,
    receiver: Receiver<Entry<RawConnection<T::Stream>>>,
    semaphore: Arc<Semaphore>,
}

#[derive(Debug)]
struct Init {
    auth: Option<(String, String)>,
    setname: Option<String>,
    select: Option<u32>,
}

/// [`Connection`] tied to the [`Cilent`](Client)'s pool.
///
/// When it dropped without [`.into_inner()`](Connection::into_inner) call,
/// the connection will be returned to the connection pool.
///
/// To use helper methods on it, use [`.command()`](crate::command::RawCommandMut::command) method.
#[derive(Debug)]
pub struct Connection<T> {
    entry: Option<Entry<RawConnection<T>>>,
    sender: Sender<Entry<RawConnection<T>>>,
}

#[derive(Debug)]
struct Entry<T> {
    conn: T,
    _permit: Permit,
}

/// A `Builder` to construct [`Client`](Client) with parameters.
#[derive(Debug)]
pub struct Builder {
    max_connections: usize,
    init: Init,
}

/// Errors that occur when binding the client to some address.
#[derive(Debug, thiserror::Error)]
pub enum BindError {
    /// DNS lookup failed.
    #[error("DNS lookup failed")]
    Lookup(#[from] LookupError),
    /// Connection error.
    #[error("connection error")]
    Connection(#[from] Error),
}

impl Client<TcpConnector> {
    /// Create a client with default configurations.
    ///
    /// If you need more tweaks use [`Client::builder()`](Self::builder) instead.
    ///
    /// # Panic
    ///
    /// It panics if `max_connections` is zero.
    pub async fn new(max_connections: usize, addr: &str) -> Result<Self, BindError> {
        Self::builder(NonZeroUsize::new(max_connections).unwrap())
            .bind(addr)
            .await
    }

    /// Create a client builder with the `max_connections` parameter.
    ///
    /// To build client without the [`TcpConnector`](crate::connector::TcpConnector)
    /// you need to use this method.
    pub fn builder(max_connections: NonZeroUsize) -> Builder {
        Builder::new(max_connections)
    }
}

impl<T: Connector> Client<T> {
    /// Server hello response from the last connection created.
    pub fn server_hello(&self) -> Arc<Value> {
        self.shared.0.server_hello()
    }

    /// Send any command and get response of it.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// Check out the [serializer](crate::resp3::ser_cmd::CommandSerializer)
    /// and [deserializer](crate::resp3::de::Deserializer) documents for details.
    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        self.shared.0.raw_command(request).await
    }

    /// Get a connection from the pool, or create one if not available.
    ///
    /// Normally dropped [`Connection`](Connection) will be returned to the [`Client`](Client)'s pool.
    pub async fn connection(&self) -> Result<Connection<T::Stream>, Error> {
        self.shared.0.connection().await
    }
}

impl<T: Connector> ops::Deref for Client<T> {
    type Target = Command<ClientShared<T>>;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl<T: Connector> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

#[async_trait]
impl<T: Connector> command::RawCommand for Client<T> {
    async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        self.raw_command(request).await
    }
}

impl<T: Connector> From<ClientShared<T>> for Client<T> {
    fn from(shared: ClientShared<T>) -> Self {
        Client {
            shared: Arc::new(Command(shared)),
        }
    }
}

impl<T: Connector> From<Arc<Command<ClientShared<T>>>> for Client<T> {
    fn from(shared: Arc<Command<ClientShared<T>>>) -> Self {
        Client { shared }
    }
}

impl<T: Connector> From<Client<T>> for Arc<Command<ClientShared<T>>> {
    fn from(client: Client<T>) -> Self {
        client.shared
    }
}

impl Builder {
    /// Create a client builder with the `max_connections` parameter.
    pub fn new(max_connections: NonZeroUsize) -> Builder {
        Builder {
            max_connections: max_connections.get(),
            init: Init {
                auth: None,
                setname: None,
                select: None,
            },
        }
    }

    /// Bind the `Client` to the address string, may perform DNS lookup.
    pub async fn bind(self, addr: &str) -> Result<Client<TcpConnector>, BindError> {
        Ok(self.build(TcpConnector::lookup(addr).await?).await?)
    }

    /// Build the `Client` with the `connector`.
    pub async fn build<T: Connector>(self, connector: T) -> Result<Client<T>, Error> {
        Ok(self.build_shared(connector).await?.into())
    }

    /// Build the `ClientShared` with the `connector`.
    pub async fn build_shared<T: Connector>(self, connector: T) -> Result<ClientShared<T>, Error> {
        let (conn, hello) = make_connection(&connector, &self.init).await?;
        let semaphore = Arc::new(Semaphore::new(self.max_connections));
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should have at least 1 permits left");
        let (sender, receiver) = async_channel::bounded(self.max_connections);

        sender
            .try_send(Entry {
                conn,
                _permit: permit,
            })
            .expect("channel should have at least 1 caps");

        Ok(ClientShared {
            connector,
            init: self.init,
            ping_counter: AtomicU64::new(0),
            server_hello: RwLock::new(hello),
            sender,
            receiver,
            semaphore,
        })
    }

    /// Set the `username` and `password` parameters to be passed
    /// to [the `AUTH` parameter](https://redis.io/commands/hello)
    /// of the initial `HELLO` command.
    ///
    /// By default, `AUTH` parameter is not passed.
    pub fn auth(mut self, username: &str, password: &str) -> Self {
        self.init.auth = Some((username.into(), password.into()));
        self
    }

    /// Set the `clientname` parameter to be passed
    /// to [the `SETNAME` parameter](https://redis.io/commands/hello)
    /// of the initial `HELLO` command.
    ///
    /// By default, `SETNAME` parameter is not passed.
    pub fn setname(mut self, clientname: &str) -> Self {
        self.init.setname = Some(clientname.into());
        self
    }

    /// Set the `index` parameter to be passed
    /// to [the `SELECT` command](https://redis.io/commands/select)
    /// after the initial `HELLO` command.
    ///
    /// By default, `SELECT` command is not sent.
    pub fn select(mut self, index: u32) -> Self {
        self.init.select = Some(index);
        self
    }
}

async fn make_connection<T: Connector>(
    connector: &T,
    init: &Init,
) -> Result<(RawConnection<T::Stream>, Arc<Value>), Error> {
    let conn = connector.connect().await?;
    let (conn, hello) = RawConnection::with_args(
        conn,
        init.auth
            .as_ref()
            .map(|(username, password)| (&username[..], &password[..])),
        init.setname.as_deref(),
        init.select,
    )
    .await?;
    let hello = Arc::new(hello);

    Ok((conn, hello))
}

impl<T: Connector> ClientShared<T> {
    /// Server hello response from the last connection created.
    pub fn server_hello(&self) -> Arc<Value> {
        Arc::clone(&self.server_hello.read().unwrap())
    }

    /// Send any command and get response of it.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// Check out the [serializer](crate::resp3::ser_cmd::CommandSerializer)
    /// and [deserializer](crate::resp3::de::Deserializer) documents for details.
    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        let mut conn = self.connection().await?;
        Ok(conn.raw_command(request).await?)
    }

    /// Get a connection from the pool, or create one if not available.
    ///
    /// Normally dropped [`Connection`](Connection) will be returned to the [`Client`](Client)'s pool.
    pub async fn connection(&self) -> Result<Connection<T::Stream>, Error> {
        async fn wrap<T: Connector>(
            client: &ClientShared<T>,
            mut entry: Entry<RawConnection<T::Stream>>,
        ) -> Result<Connection<T::Stream>, Error> {
            let count = client.ping_counter.fetch_add(1, atomic::Ordering::Relaxed);
            let pong: u64 = entry.conn.raw_command(&("PING", count)).await?;

            if pong != count {
                return Err(de::Error::Serde(format!(
                    "Invalid PING response - exp: {}, got: {}",
                    count, pong
                ))
                .into());
            }

            Ok(Connection {
                entry: Some(entry),
                sender: client.sender.clone(),
            })
        }
        async fn connect<T: Connector>(
            client: &ClientShared<T>,
            permit: Permit,
        ) -> Result<Connection<T::Stream>, Error> {
            let (conn, hello) = make_connection(&client.connector, &client.init).await?;
            *client.server_hello.write().unwrap() = hello;

            Ok(Connection {
                sender: client.sender.clone(),
                entry: Some(Entry {
                    conn,
                    _permit: permit,
                }),
            })
        }

        let sem = self.semaphore.clone();

        tokio::select! {
            biased;
            entry = self.receiver.recv() => wrap(self, entry.unwrap()).await,
            permit = sem.acquire_owned() => connect(self, permit.unwrap()).await,
        }
    }
}

#[async_trait]
impl<T: Connector> command::RawCommand for ClientShared<T> {
    async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        self.raw_command(request).await
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    /// Send any command and get response of it.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// Check out the [serializer](crate::resp3::ser_cmd::CommandSerializer)
    /// and [deserializer](crate::resp3::de::Deserializer) documents for details.
    ///
    /// Returned response may contains a reference to the connection's internal receive buffer.
    pub async fn raw_command<'de, Req: Serialize, Resp: Deserialize<'de>>(
        &'de mut self,
        request: Req,
    ) -> Result<Resp, Error> {
        self.get_mut().raw_command(request).await
    }
}

impl<T> Connection<T> {
    /// Gets a shared reference to the underlying raw connection.
    pub fn get(&self) -> &RawConnection<T> {
        &self.entry.as_ref().unwrap().conn
    }

    /// Gets a mutable reference to the underlying raw connection.
    pub fn get_mut(&mut self) -> &mut RawConnection<T> {
        &mut self.entry.as_mut().unwrap().conn
    }

    /// Detach the connection from the pool and returns the underlying raw connection.
    /// It will reduce the connection count of the pool by 1.
    pub fn into_inner(mut self) -> RawConnection<T> {
        self.entry.take().unwrap().conn
    }
}

impl<T> ops::Drop for Connection<T> {
    fn drop(&mut self) {
        if let Some(entry) = self.entry.take() {
            self.sender.try_send(entry).unwrap()
        }
    }
}

#[async_trait]
impl<T: AsyncRead + AsyncWrite + Send + Unpin> command::RawCommandMut for Connection<T> {
    async fn raw_command<'de, Req, Resp>(&'de mut self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: Deserialize<'de>,
    {
        self.raw_command(request).await
    }
}
