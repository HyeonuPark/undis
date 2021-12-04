use bytes::BufMut;

pub mod de;
pub mod ser_cmd;
pub mod token;
pub mod value;

use token::Tokenizer;

#[derive(Debug)]
pub struct Reader {
    tok: Tokenizer,
}

#[derive(Debug)]
pub struct CommandWriter {
    buf: Vec<u8>,
}

impl Reader {
    pub fn new() -> Self {
        Self {
            tok: Tokenizer::new(),
        }
    }

    pub fn buf(&mut self) -> &mut impl BufMut {
        self.tok.write_buf()
    }

    /// # NOTE
    /// Without `.clear_prev()` after the call this method will keep return same message
    pub fn read(&mut self) -> Result<Option<token::Message<'_>>, token::Error> {
        Ok(Some(match self.tok.message() {
            Ok(msg) => msg,
            Err(err) => {
                return match err {
                    None => Ok(None),
                    Some(err) => Err(err),
                }
            }
        }))
    }

    pub fn consume(&mut self) {
        self.tok.consume()
    }

    pub fn reset(&mut self) {
        self.tok.reset()
    }
}

impl Default for Reader {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> token::Message<'a> {
    pub fn deserialize<T: serde::Deserialize<'a>>(mut self) -> Result<T, de::Error> {
        T::deserialize(de::Deserializer::new(&mut self))
    }
}

impl CommandWriter {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn write<T: serde::Serialize>(&mut self, value: &T) -> Result<&[u8], ser_cmd::Error> {
        value.serialize(ser_cmd::CommandSerializer::new(&mut self.buf))?;
        Ok(&self.buf)
    }
}

impl Default for CommandWriter {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_str<T: std::str::FromStr>(msg: &[u8]) -> Option<T> {
    std::str::from_utf8(msg).ok()?.parse().ok()
}
