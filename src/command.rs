use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::connection::Error;

pub mod hash;

#[derive(Debug)]
pub struct Command<T: RawCommand>(pub T);

#[derive(Debug)]
pub struct Mutex<T>(tokio::sync::Mutex<T>);

#[async_trait]
pub trait RawCommand: Send + Sync {
    async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned;
}

#[async_trait]
pub trait RawCommandMut: Send {
    async fn raw_command<'de, Req, Resp>(&'de mut self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: Deserialize<'de>;

    fn command(self) -> Command<Mutex<Self>>
    where
        Self: Sized,
    {
        Command(Mutex::new(self))
    }
}

impl<T: RawCommand> Command<T> {
    pub async fn raw_command<Req, Resp>(&self, request: Req) -> Result<Resp, Error>
    where
        Req: Serialize + Send,
        Resp: DeserializeOwned,
    {
        self.0.raw_command(request).await
    }
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Self {
        Mutex(tokio::sync::Mutex::new(value))
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
