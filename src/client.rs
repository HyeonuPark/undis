//! Redis client.
//!
//! For more information, see the [`Client`](self::Client) type.

use std::num::NonZeroUsize;
use std::ops;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};

use async_channel::{Receiver, Sender};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit as Permit, Semaphore};

use crate::connection::{self, Connection as RawConnection};
use crate::connector::{Connector, LookupError, TcpConnector};
use crate::resp3::{de, value::Value};

pub mod hash;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct Client<T: Connector = TcpConnector> {
    shared: Arc<ClientShared<T>>,
}

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

#[derive(Debug)]
pub struct Builder {
    connection_limit: usize,
    init: Init,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorKind>);

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    /// Connection error.
    #[error("connection error")]
    Connection(#[from] connection::Error),
    /// Ping pong failed.
    #[error("ping-pong failed")]
    PingPongFailed,
    /// DNS lookup failed.
    #[error("DNS lookup failed")]
    Lookup(#[from] LookupError),
}

impl Client<TcpConnector> {
    /// Create a client with default configurations.
    ///
    /// If you need more tweaks use [`Client::builder()`](Self::builder) instead.
    ///
    /// # Panic
    ///
    /// It panics if `connection_limit` is zero.
    pub async fn new(connection_limit: usize, addr: &str) -> Result<Self, Error> {
        Self::builder(NonZeroUsize::new(connection_limit).unwrap())
            .bind(addr)
            .await
    }

    /// Create a client builder.
    ///
    /// The builder from this method is not limited to the `TcpConnector`.
    pub fn builder(connection_limit: NonZeroUsize) -> Builder {
        Builder::new(connection_limit)
    }
}

impl<T: Connector> Client<T> {
    pub fn server_hello(&self) -> Arc<Value> {
        self.shared.server_hello()
    }

    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        self.shared.raw_command(request).await
    }

    pub async fn connection(&self) -> Result<Connection<T::Stream>, Error> {
        self.shared.connection().await
    }
}

impl Builder {
    pub fn new(connection_limit: NonZeroUsize) -> Builder {
        Builder {
            connection_limit: connection_limit.get(),
            init: Init {
                auth: None,
                setname: None,
                select: None,
            },
        }
    }

    pub async fn bind(self, addr: &str) -> Result<Client<TcpConnector>, Error> {
        self.build(
            TcpConnector::lookup(addr)
                .await
                .map_err(ErrorKind::Lookup)?,
        )
        .await
    }

    pub async fn build<T: Connector>(self, connector: T) -> Result<Client<T>, Error> {
        Ok(self.build_shared(connector).await?.into())
    }

    pub async fn build_shared<T: Connector>(self, connector: T) -> Result<ClientShared<T>, Error> {
        let (conn, hello) = make_connection(&connector, &self.init).await?;
        let semaphore = Arc::new(Semaphore::new(self.connection_limit));
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore should have at least 1 permits left");
        let (sender, receiver) = async_channel::bounded(self.connection_limit);

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

    pub fn auth(mut self, username: &str, password: &str) -> Self {
        self.init.auth = Some((username.into(), password.into()));
        self
    }

    pub fn setname(mut self, clientname: &str) -> Self {
        self.init.setname = Some(clientname.into());
        self
    }

    pub fn select(mut self, db: u32) -> Self {
        self.init.select = Some(db);
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
    pub fn server_hello(&self) -> Arc<Value> {
        Arc::clone(&self.server_hello.read().unwrap())
    }

    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        request: Req,
    ) -> Result<Resp, Error> {
        let mut conn = self.connection().await?;
        Ok(conn.raw_command(request).await?)
    }

    pub async fn connection(&self) -> Result<Connection<T::Stream>, Error> {
        async fn wrap<T: Connector>(
            client: &ClientShared<T>,
            mut entry: Entry<RawConnection<T::Stream>>,
        ) -> Result<Connection<T::Stream>, Error> {
            let count = client.ping_counter.fetch_add(1, atomic::Ordering::Relaxed);
            let pong: u64 = entry.conn.raw_command(&("PING", count)).await?;

            if pong != count {
                return Err(ErrorKind::PingPongFailed.into());
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

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &mut self,
        request: Req,
    ) -> Result<Resp, Error> {
        Ok(self.inner_mut().raw_command(request).await?)
    }

    pub fn inner(&self) -> &RawConnection<T> {
        &self.entry.as_ref().unwrap().conn
    }

    pub fn inner_mut(&mut self) -> &mut RawConnection<T> {
        &mut self.entry.as_mut().unwrap().conn
    }

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

impl<T: Connector> From<ClientShared<T>> for Client<T> {
    fn from(shared: ClientShared<T>) -> Self {
        Client {
            shared: Arc::new(shared),
        }
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
