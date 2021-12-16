use std::marker::Unpin;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::resp3::{de, ser_cmd, token, value::Value, CommandWriter, Reader};

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
    Tokenize(#[from] token::Error),
    #[error("serialize error")]
    Serialize(#[from] ser_cmd::Error),
    #[error("deserialize error")]
    Deserialize(#[from] de::Error),
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

    pub async fn send<Req: Serialize>(&mut self, request: Req) -> Result<(), Error> {
        self.transport
            .write_all(self.writer.write(request)?)
            .await?;
        Ok(())
    }

    pub async fn receive(&mut self) -> Result<token::Message<'_>, Error> {
        self.reader.consume();

        loop {
            self.transport.read_buf(self.reader.buf()).await?;
            if let Some(_msg) = self.reader.read()? {
                break;
            }
        }

        // at this point the reader always stores the message
        Ok(self.reader.read().unwrap().unwrap())
    }

    pub async fn raw_command<'de, Req: Serialize, Resp: Deserialize<'de>>(
        &'de mut self,
        request: Req,
    ) -> Result<Resp, Error> {
        self.send(request).await?;
        Ok(self.receive().await?.deserialize()?)
    }
}
