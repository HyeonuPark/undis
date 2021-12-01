use std::marker::Unpin;

use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::resp3::{self, value::Value, CommandWriter, Reader};

mod serde_helper;

mod hash;

#[derive(Debug)]
pub struct Connection<T> {
    transport: T,
    reader: Reader,
    writer: CommandWriter,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("tokenize error")]
    Tokenize(#[from] resp3::token::Error),
    #[error("serialize error")]
    Serialize(#[from] resp3::ser_cmd::Error),
    #[error("deserialize error")]
    Deserialize(#[from] resp3::de::Error),
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<T> {
    pub async fn new(transport: T) -> Result<(Self, Value), Error> {
        let mut chan = Connection {
            transport,
            reader: Reader::new(),
            writer: CommandWriter::new(),
        };

        let resp = chan.raw_command(&("HELLO", 3)).await?;

        Ok((chan, resp))
    }

    pub async fn raw_command<Req: Serialize, Resp: DeserializeOwned>(
        &mut self,
        request: &Req,
    ) -> Result<Resp, Error> {
        let req = self.writer.write(request)?;
        self.transport.write_all(req).await?;
        self.reader.consume();

        loop {
            self.transport.read_buf(self.reader.buf()).await?;
            if let Some(msg) = self.reader.read()? {
                return Ok(msg.deserialize()?);
            }
        }
    }
}
