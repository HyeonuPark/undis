//! Helper methods for each Redis commands.
//!
//! For more information, see the [`CommandHelper`](CommandHelper) type.

use futures_core::future::BoxFuture;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::connection::Error;

mod base;
mod hash;
mod strings;

/// A wrapper to provide helper methods for each Redis command.
#[derive(Debug)]
pub struct CommandHelper<T: RawCommand>(pub T);

/// A Mutex-based wrapper to adapt [`impl RawCommandMut`](RawCommandMut)
/// into [`CommandHelper`](CommandHelper). This is required since all its methods require `&self`.
#[derive(Debug)]
pub struct Mutex<T>(tokio::sync::Mutex<T>);

// To ensure response is constant "OK"
#[derive(Debug, Deserialize)]
enum OkResp {
    OK,
}

/// A trait for sending raw commands to the Redis server with `&self`.
pub trait RawCommand: Send + Sync {
    /// Sends any command and get a response.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// See [`CommandSerializer`](crate::resp3::ser_cmd::CommandSerializer)
    /// and [`Deserializer`](crate::resp3::de::Deserializer) for details.
    ///
    /// Implementations may also supply an inherent method with the same name and functionality
    /// to reduce allocation/dynamic dispatch.
    fn raw_command<'a, Req, Resp>(&'a self, request: Req) -> BoxFuture<'a, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'a,
        Resp: DeserializeOwned + 'a;
}

/// A trait for sending raw commands to the Redis server with `&mut self`.
pub trait RawCommandMut: Send {
    /// Sends any command and get a response.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// See [`CommandSerializer`](crate::resp3::ser_cmd::CommandSerializer)
    /// and [`Deserializer`](crate::resp3::de::Deserializer) for details.
    ///
    /// Returned response may contain a reference to the connection's internal receive buffer.
    ///
    /// Implementations may also have inherent method with same name and functionality
    /// to reduce allocation/dynamic dispatch.
    fn raw_command<'de, Req, Resp>(
        &'de mut self,
        request: Req,
    ) -> BoxFuture<'de, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'de,
        Resp: Deserialize<'de> + 'de;

    /// Wraps `self` to allow calling helper methods.
    fn command(self) -> CommandHelper<Mutex<Self>>
    where
        Self: Sized,
    {
        CommandHelper(Mutex::new(self))
    }
}

impl<T: RawCommand> CommandHelper<T> {
    /// Sends any command and get a response.
    ///
    /// Both command and response are serialized/deserialized using [`serde`](serde).
    /// See [`CommandSerializer`](crate::resp3::ser_cmd::CommandSerializer)
    /// and [`Deserializer`](crate::resp3::de::Deserializer) for details.
    pub async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        self.0.raw_command(request).await
    }
}

impl<T: RawCommand> From<T> for CommandHelper<T> {
    fn from(v: T) -> Self {
        CommandHelper(v)
    }
}

impl<T> Mutex<T> {
    /// Constructs a mutex from the value.
    pub fn new(value: T) -> Self {
        Mutex(tokio::sync::Mutex::new(value))
    }

    /// Returns a mutable reference to the underlying data.
    ///
    /// It doesn't require any synchronization as it takes `&mut self`.
    pub fn get_mut(&mut self) -> &mut T {
        self.0.get_mut()
    }

    /// Consumes this mutex, returning the underlying data.
    pub fn into_inner(self) -> T {
        self.0.into_inner()
    }
}

impl<T: RawCommandMut> RawCommand for Mutex<T> {
    fn raw_command<'a, Req, Resp>(&'a self, request: Req) -> BoxFuture<'a, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'a,
        Resp: DeserializeOwned,
    {
        Box::pin(async move { self.0.lock().await.raw_command(request).await })
    }
}

impl<T> From<T> for Mutex<T> {
    fn from(v: T) -> Self {
        Mutex(v.into())
    }
}

impl<'r, T: RawCommand> RawCommand for &'r T {
    fn raw_command<'a, Req, Resp>(&'a self, request: Req) -> BoxFuture<'a, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'a,
        Resp: DeserializeOwned + 'a,
    {
        (**self).raw_command(request)
    }
}

impl<'a, T: RawCommandMut> RawCommandMut for &'a mut T {
    fn raw_command<'de, Req, Resp>(
        &'de mut self,
        request: Req,
    ) -> BoxFuture<'de, Result<Resp, Error>>
    where
        Req: Serialize + Send + 'de,
        Resp: Deserialize<'de> + 'de,
    {
        (**self).raw_command(request)
    }
}
