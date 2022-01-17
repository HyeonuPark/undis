//! Helper methods for each Redis commands.
//!
//! For more information, see the [`Command`](Command) type.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::connection::Error;

mod base;
mod hash;
mod strings;

/// A wrapper to provide helper methods for each Redis commands.
#[derive(Debug)]
pub struct Command<T: RawCommand>(pub T);

/// A Mutex-based wrapper to adapt [`impl RawCommandMut`](RawCommandMut)
/// into the [`Command`](Command) as all the methods of it requires `&self`.
#[derive(Debug)]
pub struct Mutex<T>(tokio::sync::Mutex<T>);

/// To ensure response is constant "OK"
#[derive(Debug, Deserialize)]
enum OkResp {
    OK,
}

/// A trait to send raw command to the Redis server with `&self`.
#[async_trait]
pub trait RawCommand: Send + Sync {
    /// Send any command and get response of it.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// Check out the [serializer](crate::resp3::ser_cmd::CommandSerializer)
    /// and [deserializer](crate::resp3::de::Deserializer) documents for details.
    ///
    /// Implementations may also have inherent method with same name and functionality
    /// to reduce allocation/dynamic dispatch due to the current limitation
    /// of the [`async_trait`](::async_trait) macro.
    async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned;
}

/// A trait to send raw command to the Redis server with `&mut self`.
#[async_trait]
pub trait RawCommandMut: Send {
    /// Send any command and get response of it.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// Check out the [serializer](crate::resp3::ser_cmd::CommandSerializer)
    /// and [deserializer](crate::resp3::de::Deserializer) documents for details.
    ///
    /// Returned response may contains a reference to the connection's internal receive buffer.
    ///
    /// Implementations may also have inherent method with same name and functionality
    /// to reduce allocation/dynamic dispatch due to the current limitation
    /// of the [`async_trait`](::async_trait) macro.
    async fn raw_command<'de, Req, Resp>(&'de mut self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: Deserialize<'de>;

    /// Wrap the `self` with additional types to allow to call helper methods on it.
    fn command(self) -> Command<Mutex<Self>>
    where
        Self: Sized,
    {
        Command(Mutex::new(self))
    }
}

impl<T: RawCommand> Command<T> {
    /// Send any command and get response of it.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// Check out the [serializer](crate::resp3::ser_cmd::CommandSerializer)
    /// and [deserializer](crate::resp3::de::Deserializer) documents for details.
    pub async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        self.0.raw_command(request).await
    }
}

impl<T: RawCommand> From<T> for Command<T> {
    fn from(v: T) -> Self {
        Command(v)
    }
}

impl<T> Mutex<T> {
    /// Construct a mutex from the value.
    pub fn new(value: T) -> Self {
        Mutex(tokio::sync::Mutex::new(value))
    }

    /// Gets a mutable reference to the underlying data.
    ///
    /// It doesn't require any synchronization as it takes `&mut self`.
    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    /// Consumes this mutex and return the underlying data.
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

#[async_trait]
impl<T: RawCommandMut> RawCommand for Mutex<T> {
    async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        self.0.lock().await.raw_command(request).await
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(v: T) -> Self {
        Mutex(v.into())
    }
}

#[async_trait]
impl<'a, T: RawCommand> RawCommand for &'a T {
    async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        (**self).raw_command(request).await
    }
}

#[async_trait]
impl<'a, T: RawCommandMut> RawCommandMut for &'a mut T {
    async fn raw_command<'de, Req, Resp>(&'de mut self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: Deserialize<'de>,
    {
        (**self).raw_command(request).await
    }
}
