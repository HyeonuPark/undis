//! Redis connection.
//!
//! For more information, see the [`Connection`](Connection) type.

use std::marker::Unpin;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

use crate::command;
use crate::resp3::{de, from_msg, ser_cmd, token, write_cmd, Reader, Value};

/// A stateful connection to the Redis server.
///
/// To use helper methods on it, use [`.command()`](crate::command::RawCommandMut::command) method.
#[derive(Debug)]
pub struct Connection<T> {
    transport: T,
    sender: SendCtx,
    receiver: ReceiveCtx,
}

/// A send-half of the connection.
/// Can be used as a building block for higher abstraction.
#[derive(Debug)]
pub struct ConnectionSendHalf<T> {
    transport: WriteHalf<T>,
    sender: SendCtx,
}

/// A write-half of the connection.
/// Can be used as a building block for higher abstraction.
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
    last_is_push: bool,
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
    /// The client receives response from the server for the request haven't sent yet.
    #[error("invalid message order")]
    InvalidMessageOrder,
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    /// Connect to the Redis server using the `transport`.
    ///
    /// When success, returns the `Connection` and `HELLO` command's response
    /// as a [`Value`](crate::resp3::Value) which contains server's metadata.
    pub async fn new(transport: T) -> Result<(Self, Value), Error> {
        Self::with_args(transport, None, None, None).await
    }

    /// Connec to the Redis server using the `transport` and parameters.
    /// See the [`Builder`](crate::client::Builder)'s methods for the usage of each parameters.
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
                last_is_push: false,
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

    /// Send a command without waiting response.
    ///
    /// Returns one-based index of the command sent from this connection.
    pub async fn send<Req: Serialize>(&mut self, request: Req) -> Result<u64, Error> {
        self.sender.send(&mut self.transport, request).await
    }

    /// Receive a response.
    ///
    /// Returns a received `Message` and
    /// optional one-based index of the response received from this connection.
    /// This index can be useful to match command and its corresponding response.
    ///
    /// The index is `None` for the [`Push`](crate::resp3::token::Token::Push) message
    /// since it doesn't have corresponding command.
    pub async fn receive(&mut self) -> Result<(token::Message<'_>, Option<u64>), Error> {
        self.receiver.receive(&mut self.transport).await
    }

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
        let req_cnt = self.send(request).await?;

        loop {
            let (_msg, resp_cnt) = self.receive().await?;
            match resp_cnt {
                // push message
                None => continue,
                // response from some previous request
                // maybe due to the cancelation
                Some(cnt) if cnt < req_cnt => continue,
                // response from future request??
                Some(cnt) if cnt > req_cnt => return Err(ErrorKind::InvalidMessageOrder.into()),
                // respone from the very request
                Some(_) => break,
            }
        }

        // at this point the receiver always store the non-push message
        let (msg, _) = self.receiver.peek().unwrap();
        Ok(from_msg(msg)?)
    }

    /// Split the connection into read/write halves.
    /// Can be used as a building block for higher abstraction.
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

impl<T: AsyncWrite + Unpin> ConnectionSendHalf<T> {
    /// Send a command without waiting response.
    ///
    /// Returns one-based index of the command sent from this connection.
    pub async fn send<Req: Serialize>(&mut self, request: Req) -> Result<u64, Error> {
        self.sender.send(&mut self.transport, request).await
    }

    /// Check if two halves are from the same [`Connection`](Connection).
    pub fn is_pair_of(&self, other: &ConnectionReceiveHalf<T>) -> bool {
        other.transport.is_pair_of(&self.transport)
    }

    /// Merge previously split two halves back.
    ///
    /// # Panic
    ///
    /// It panic if the `self` and the `other` are not from the same [`Connection`](Connection).
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
    /// Receive a response.
    ///
    /// Returns a received `Message` and
    /// optional one-based index of the response received from this connection.
    /// This index can be useful to match command and its corresponding response.
    ///
    /// The index is `None` for the [`Push`](crate::resp3::token::Token::Push) message
    /// since it doesn't have corresponding command.
    pub async fn receive(&mut self) -> Result<(token::Message<'_>, Option<u64>), Error> {
        self.receiver.receive(&mut self.transport).await
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
    async fn receive<T>(
        &mut self,
        transport: &mut T,
    ) -> Result<(token::Message<'_>, Option<u64>), Error>
    where
        T: AsyncRead + Unpin,
    {
        self.reader.consume();

        while self.reader.read()?.is_none() {
            transport.read_buf(self.reader.buf()).await?;
        }

        // at this point the reader always stores the message
        let msg = self.reader.peek().unwrap();

        let count = match msg.head() {
            // server push message doesn't belong to the request-response mapping
            token::Token::Push(_) => {
                self.last_is_push = true;
                None
            }
            _ => {
                self.last_is_push = false;
                self.count += 1;
                Some(self.count)
            }
        };

        Ok((msg, count))
    }

    fn peek(&self) -> Option<(token::Message<'_>, Option<u64>)> {
        let count = (!self.last_is_push).then(|| self.count);
        self.reader.peek().map(|msg| (msg, count))
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
