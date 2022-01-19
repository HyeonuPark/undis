//! Redis connection.
//!
//! For more information, see the [`Connection`](Connection) type.

use std::marker::Unpin;

use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::command;
use crate::resp3::{de, from_msg, ser_cmd, token, write_cmd, Reader, Value};

/// A stateful connection to the Redis server.
#[derive(Debug)]
pub struct Connection<T> {
    transport: T,
    sender: SendCtx,
    receiver: ReceiveCtx,
}

/// The sending-half of the connection.
/// Can be used as a building block for higher abstractions.
#[derive(Debug)]
pub struct ConnectionSendHalf<T> {
    transport: WriteHalf<T>,
    sender: SendCtx,
}

/// The receiving-half of the connection.
/// Can be used as a building block for higher abstractions.
#[derive(Debug)]
pub struct ConnectionReceiveHalf<T> {
    transport: ReadHalf<T>,
    receiver: ReceiveCtx,
}

#[derive(Debug)]
struct SendCtx {
    buf: Vec<u8>,
    count: u64,
}

#[derive(Debug)]
struct ReceiveCtx {
    reader: Reader,
    count: u64,
}

/// Errors that occur when communicating with the Redis server.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] pub Box<ErrorKind>);

/// Internal representation of the [`Error`](Error) type.
#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    /// IO error.
    #[error("io error")]
    Io(#[from] std::io::Error),
    /// Tokenize error.
    #[error("tokenize error")]
    Tokenize(#[from] token::Error),
    /// Serialize error.
    #[error("serialize error")]
    Serialize(#[from] ser_cmd::Error),
    /// Deserialize error.
    #[error("deserialize error")]
    Deserialize(#[from] de::Error),
    /// Invalid message order.
    /// The client receives a response from the server for the request that hasn't been sent yet.
    #[error("invalid message order")]
    InvalidMessageOrder,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    /// Connect to the Redis server using the `transport`.
    ///
    /// On success, returns the `Connection` and `HELLO` command's response
    /// as a [`Value`](crate::resp3::Value) which contains the server's metadata.
    pub async fn new(transport: T) -> Result<(Self, Value), Error> {
        Self::with_args(transport, None, None, None).await
    }

    /// Connects to the Redis server using the `transport` and parameters.
    /// See the [`Builder`](crate::client::Builder)'s methods for the usage of each parameter.
    pub async fn with_args(
        transport: T,
        auth: Option<(&str, &str)>,
        setname: Option<&str>,
        select: Option<u32>,
    ) -> Result<(Self, Value), Error> {
        let mut chan = Connection {
            transport,
            sender: SendCtx {
                buf: Vec::new(),
                count: 0,
            },
            receiver: ReceiveCtx {
                reader: Reader::new(),
                count: 0,
            },
        };

        let auth = auth.map(|(username, password)| ("AUTH", username, password));
        let setname = setname.map(|clientname| ("SETNAME", clientname));
        let resp = chan.raw_command(&("HELLO", 3, auth, setname)).await?;

        if let Some(db) = select {
            let serde::de::IgnoredAny = chan.raw_command(&("SELECT", db)).await?;
        }

        Ok((chan, resp))
    }

    /// Sends a command without waiting for a response.
    ///
    /// Returns a one-based index of the command sent from this connection.
    pub async fn send<Req: Serialize>(&mut self, request: Req) -> Result<u64, Error> {
        self.sender.send(&mut self.transport, request).await
    }

    /// Wait until a response arrives.
    ///
    /// Returns an optional one-based index of the response arrived from this connection.
    /// This index can be useful for matching a command and its corresponding response.
    ///
    /// The index is `None` for the [`Push`](crate::resp3::token::Token::Push) message
    /// since it does not have a corresponding command.
    ///
    /// It's guaranteed that `.message()` returns `Some(msg)` after this method returns `Ok(idx)`.
    pub async fn wait_response(&mut self) -> Result<Option<u64>, Error> {
        self.receiver.wait_response(&mut self.transport).await
    }

    /// Get the last response received.
    pub fn response(&self) -> Option<token::Message<'_>> {
        self.receiver.response()
    }

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
        let req_cnt = self.send(request).await?;

        loop {
            match self.wait_response().await? {
                // push message
                None => continue,
                // response from some previous request
                // maybe due to the cancelation
                Some(cnt) if cnt < req_cnt => continue,
                // response from a future request??
                Some(cnt) if cnt > req_cnt => return Err(ErrorKind::InvalidMessageOrder.into()),
                // response from the very request
                Some(_) => break,
            }
        }

        // at this point the receiver always stores the non-push message
        Ok(from_msg(self.receiver.response().unwrap())?)
    }

    /// Splits the connection into send/receive halves.
    /// Can be used as a building block for higher abstractions.
    pub fn split(self) -> (ConnectionSendHalf<T>, ConnectionReceiveHalf<T>) {
        let (read, write) = tokio::io::split(self.transport);
        (
            ConnectionSendHalf {
                transport: write,
                sender: self.sender,
            },
            ConnectionReceiveHalf {
                transport: read,
                receiver: self.receiver,
            },
        )
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

impl<T: AsyncWrite + Unpin> ConnectionSendHalf<T> {
    /// Sends a command without waiting for a response.
    ///
    /// Returns a one-based index of the command sent from this connection.
    pub async fn send<Req: Serialize>(&mut self, request: Req) -> Result<u64, Error> {
        self.sender.send(&mut self.transport, request).await
    }

    /// Checks if two halves are from the same [`Connection`](Connection).
    pub fn is_pair_of(&self, other: &ConnectionReceiveHalf<T>) -> bool {
        other.transport.is_pair_of(&self.transport)
    }

    /// Merge two halves that have been split.
    ///
    /// # Panic
    ///
    /// Panics if the `self` and the `other` are not from the same [`Connection`](Connection).
    pub fn unsplit(self, other: ConnectionReceiveHalf<T>) -> Connection<T> {
        let transport = other.transport.unsplit(self.transport);

        Connection {
            transport,
            sender: self.sender,
            receiver: other.receiver,
        }
    }
}

impl<T: AsyncRead + Unpin> ConnectionReceiveHalf<T> {
    /// Wait until a response arrives.
    ///
    /// Returns an optional one-based index of the response arrived from this connection.
    /// This index can be useful for matching a command and its corresponding response.
    ///
    /// The index is `None` for the [`Push`](crate::resp3::token::Token::Push) message
    /// since it does not have a corresponding command.
    ///
    /// It's guaranteed that `.message()` returns `Some(msg)` after this method returns `Ok(idx)`.
    pub async fn wait_response(&mut self) -> Result<Option<u64>, Error> {
        self.receiver.wait_response(&mut self.transport).await
    }

    /// Get the last response received.
    pub fn response(&self) -> Option<token::Message<'_>> {
        self.receiver.response()
    }
}

impl SendCtx {
    async fn send<T, Req>(&mut self, transport: &mut T, request: Req) -> Result<u64, Error>
    where
        T: AsyncWrite + Unpin,
        Req: Serialize,
    {
        let cmd = write_cmd(&mut self.buf, &request)?;
        transport.write_all(cmd).await?;

        self.count += 1;
        Ok(self.count)
    }
}

impl ReceiveCtx {
    async fn wait_response<T>(&mut self, transport: &mut T) -> Result<Option<u64>, Error>
    where
        T: AsyncRead + Unpin,
    {
        while self.reader.message_not_ready()? {
            transport.read_buf(self.reader.buf()).await?;
        }

        // at this point the reader always stores the message
        let msg = self.reader.message().unwrap();

        let count = match msg.head() {
            // server push message doesn't belong to the request-response mapping
            token::Token::Push(_) => None,
            _ => {
                self.count += 1;
                Some(self.count)
            }
        };

        Ok(count)
    }

    fn response(&self) -> Option<token::Message<'_>> {
        self.reader.message()
    }
}

impl From<ErrorKind> for Error {
    fn from(err: ErrorKind) -> Self {
        Self(Box::new(err))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self(Box::new(err.into()))
    }
}

impl From<token::Error> for Error {
    fn from(err: token::Error) -> Self {
        Self(Box::new(err.into()))
    }
}

impl From<ser_cmd::Error> for Error {
    fn from(err: ser_cmd::Error) -> Self {
        Self(Box::new(err.into()))
    }
}

impl From<de::Error> for Error {
    fn from(err: de::Error) -> Self {
        Self(Box::new(err.into()))
    }
}
