//! Redis client.
//!
//! For more information, see the [`Client`](Client) type.

use std::ops;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};

use async_channel::{Receiver, Sender};
use futures_core::future::BoxFuture;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit as Permit, Semaphore};

use crate::command::{self, CommandHelper};
use crate::connection::{Connection as RawConnection, Error};
use crate::connector::{Connector, LookupError, TcpConnector};
use crate::resp3::{de, value::Value};

#[cfg(test)]
mod tests;

/// A Redis client.
///
/// A `Client` can use various channels to connect to a Redis server.
/// By default it uses TCP, but UDS and custom streams are available.
/// Connection parameters used by a `Client` can be configured using a [`Builder`](Client::builder).
///
/// A `Client` manages a pool of connections to a Redis server.
/// Cloning a `Client` will result in a copy that shares the pool with the original one.
///
/// You do *not* need to re-wrap it with [`Arc`](std::sync::Arc) or similar, since it already uses an `Arc` internally.
#[derive(Debug)]
pub struct Client<T: Connector = TcpConnector> {
    shared: Arc<CommandHelper<ClientShared<T>>>,
}

/// Internal shared structure behind the [`Client`](Client). You may not need to use it directly.
///
/// Main purpose of exposing this type is to make `Client` `Deref<Target = Command<ClientShared<T>>>`.
/// You may also use this to reduce indirection.
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

#[derive(Debug, Default)]
struct Init {
    auth: Option<(String, String)>,
    setname: Option<String>,
    select: Option<u32>,
}

/// `Connection` tied to the [`Client`](Client)'s pool.
///
/// When dropped without a [`.into_inner()`](Connection::into_inner) call,
/// the connection managed by this `Connection` will be returned to the connection pool.
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

/// A `Builder` for constructing a [`Client`](Client) with custom parameters.
#[derive(Debug, Default)]
pub struct Builder {
    init: Init,
}

/// Errors that occur when building the client.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// DNS lookup failed.
    #[error("DNS lookup failed")]
    Lookup(#[from] LookupError),
    /// Connection error.
    #[error("connection error")]
    Connection(#[from] Error),
    /// Max connections is set as 0
    #[error("max_connections is set as 0")]
    MaxConnectionsZero,
}

impl Client<TcpConnector> {
    /// Constructs a client with default configurations.
    ///
    /// If you need more tweaks use [`Client::builder()`](Self::builder) instead.
    pub async fn new(max_connections: usize, addr: &str) -> Result<Self, BuildError> {
        Self::builder().bind(max_connections, addr).await
    }

    /// Constructs a `Builder` to construct a `Client` with custom parameters.
    pub fn builder() -> Builder {
        Builder::new()
    }
}

impl<T: Connector> Client<T> {
    /// Server hello response from the last connection created.
    pub fn server_hello(&self) -> Arc<Value> {
        self.shared.0.server_hello()
    }

    /// Sends any command and get a response.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// See [`CommandSerializer`](crate::resp3::ser_cmd::CommandSerializer)
    /// and [`Deserializer`](crate::resp3::de::Deserializer) for details.
    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        self.shared.0.raw_command(request).await
    }

    /// Gets a connection from the pool, creating one if not available.
    ///
    /// Normally, a dropped [`Connection`](Connection) will be returned to the [`Client`](Client)'s pool.
    pub async fn connection(&self) -> Result<Connection<T::Stream>, Error> {
        self.shared.0.connection().await
    }
}

impl<T: Connector> ops::Deref for Client<T> {
    type Target = CommandHelper<ClientShared<T>>;

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

impl<T: Connector> command::RawCommand for Client<T> {
    fn raw_command<'a, Req, Resp>(&'a self, request: Req) -> BoxFuture<'a, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'a,
        Resp: DeserializeOwned + 'a,
    {
        Box::pin(self.raw_command(request))
    }
}

impl<T: Connector> From<ClientShared<T>> for Client<T> {
    fn from(shared: ClientShared<T>) -> Self {
        Client {
            shared: Arc::new(CommandHelper(shared)),
        }
    }
}

impl<T: Connector> From<Arc<CommandHelper<ClientShared<T>>>> for Client<T> {
    fn from(shared: Arc<CommandHelper<ClientShared<T>>>) -> Self {
        Client { shared }
    }
}

impl<T: Connector> From<Client<T>> for Arc<CommandHelper<ClientShared<T>>> {
    fn from(client: Client<T>) -> Self {
        client.shared
    }
}

impl Builder {
    /// Constructs a client builder.
    pub fn new() -> Builder {
        Builder::default()
    }

    /// Binds the `Client` from the `max_connections`
    /// and a string representation of a socket address. This may perform a DNS lookup.
    pub async fn bind(
        self,
        max_connections: usize,
        addr: &str,
    ) -> Result<Client<TcpConnector>, BuildError> {
        Ok(self
            .build(max_connections, TcpConnector::lookup(addr).await?)
            .await?)
    }

    /// Builds a `Client` with the `max_connections` and the `connector`.
    pub async fn build<T: Connector>(
        self,
        max_connections: usize,
        connector: T,
    ) -> Result<Client<T>, BuildError> {
        Ok(self.build_shared(max_connections, connector).await?.into())
    }

    /// Builds a `ClientShared` with the `max_connections` and the `connector`.
    pub async fn build_shared<T: Connector>(
        self,
        max_connections: usize,
        connector: T,
    ) -> Result<ClientShared<T>, BuildError> {
        if max_connections == 0 {
            return Err(BuildError::MaxConnectionsZero);
        }

        let (conn, hello) = make_connection(&connector, &self.init).await?;
        let semaphore = Arc::new(Semaphore::new(max_connections));
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should have at least 1 permits left");
        let (sender, receiver) = async_channel::bounded(max_connections);

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

    /// Sets the `username` and `password` parameters to be passed in
    /// as [the `AUTH` parameter](https://redis.io/commands/hello)
    /// of the initial `HELLO` command.
    ///
    /// By default, `AUTH` parameter is not passed.
    pub fn auth(mut self, username: &str, password: &str) -> Self {
        self.init.auth = Some((username.into(), password.into()));
        self
    }

    /// Sets the `clientname` parameter to be passed in
    /// as [the `SETNAME` parameter](https://redis.io/commands/hello)
    /// of the initial `HELLO` command.
    ///
    /// By default, `SETNAME` parameter is not passed.
    pub fn setname(mut self, clientname: &str) -> Self {
        self.init.setname = Some(clientname.into());
        self
    }

    /// Sets the `index` parameter to be passed in
    /// as [the `SELECT` command](https://redis.io/commands/select)
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

    /// Sends any command and get a response.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// See [`CommandSerializer`](crate::resp3::ser_cmd::CommandSerializer)
    /// and [`Deserializer`](crate::resp3::de::Deserializer) for details.
    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        let mut conn = self.connection().await?;
        Ok(conn.raw_command(request).await?)
    }

    /// Gets a connection from the pool, creating one if not available.
    ///
    /// Normally, dropped [`Connection`](Connection) will be returned to the [`Client`](Client)'s pool.
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

impl<T: Connector> command::RawCommand for ClientShared<T> {
    fn raw_command<'a, Req, Resp>(&'a self, request: Req) -> BoxFuture<'a, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'a,
        Resp: DeserializeOwned + 'a,
    {
        Box::pin(self.raw_command(request))
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    /// Sends any command and get a response.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// See [`CommandSerializer`](crate::resp3::ser_cmd::CommandSerializer)
    /// and [`Deserializer`](crate::resp3::de::Deserializer) for details.
    ///
    /// Returned response may contain a reference to the connection's internal receive buffer.
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

impl<T: AsyncRead + AsyncWrite + Send + Unpin> command::RawCommandMut for Connection<T> {
    fn raw_command<'de, Req, Resp>(
        &'de mut self,
        request: Req,
    ) -> BoxFuture<'de, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'de,
        Resp: Deserialize<'de> + 'de,
    {
        Box::pin(self.raw_command(request))
    }
}
