//! Parse and write RESP3 tokens and values.

use std::num::NonZeroUsize;

use bytes::{Buf, BufMut};
use memchr::memmem::Finder;
use once_cell::sync::Lazy;

use super::parse_str;

/// RESP3 token
///
/// <https://github.com/antirez/RESP3/blob/74adea588783e463c7e84793b325b088fe6edd1c/spec.md>
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Token<'a> {
    /// `*42\r\n` or `*?\r\n`
    ///
    /// Array token with optional length.
    /// If the length is not specified it's a streaming array.
    Array(Option<usize>),
    /// `$5\r\nhello\r\n` or `$?\r\n`
    ///
    /// Blob string token which can be a streaming one.
    /// If the content is missing it's a header of a streaming blob.
    Blob(Option<&'a [u8]>),
    /// `;5\r\n`
    ///
    /// Element of a streaming blob string.
    BlobStream(&'a [u8]),
    /// `+hello\r\n`
    ///
    /// Simple string token.
    Simple(&'a [u8]),
    /// `-error\r\n`
    ///
    /// Simple error token.
    SimpleError(&'a [u8]),
    /// `:42\r\n`
    ///
    /// Number token.
    /// Its value should be representable as a i64.
    Number(i64),
    /// `_\r\n`
    ///
    /// Null token.
    Null,
    /// `,3.1415\r\n`
    ///
    /// Double token.
    /// Its value should be representable as a f64.
    /// It can't hold the `NaN` value.
    Double(super::Double),
    /// `#t\r\n` or `#f\r\n`
    ///
    /// Boolean token.
    Boolean(bool),
    /// `!5\r\nerror\r\n`
    ///
    /// Blob error token.
    BlobError(&'a [u8]),
    /// `=9\r\n**hello**\r\n`
    ///
    /// Verbatim string token.
    /// It may contains markdown document.
    Verbatim(&'a [u8]),
    /// `%2\r\n` or `%?\r\n`
    ///
    /// Map token with optional length.
    /// The length is amount of entries if specified,
    /// so the number of values followed by this token woule be double of it.
    /// If the length is missing it's a streaming map.
    Map(Option<usize>),
    /// `~2\r\n` or `~?\r\n`
    ///
    /// Set token with optional length.
    /// Order of its entries should not be considered stable.
    Set(Option<usize>),
    /// `|2\r\n`
    ///
    /// Attribute token is like a non-streaming Map,
    /// but its value should be ignored during parsing normal values.
    Attribute(usize),
    /// `>2\r\n`
    ///
    /// Push token is like a non-streaming Map,
    /// but it is sent from the server without any request.
    /// ex) pubsub event
    Push(usize),
    /// `(10000000000000000000\r\n`
    ///
    /// Big number token contains integer which can't be represented as a `i64`.
    BigNumber(&'a [u8]),
    /// `.\r\n`
    ///
    /// Stream end token signals the streaming aggregate type is ended.
    StreamEnd,
}

use Token::*;

/// Top level value which flows over the connection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Message<'a> {
    buf: &'a [u8],
    tokens: usize,
}

/// Iterate tokens within a message.
#[derive(Debug, Clone)]
pub struct MessageIter<'a> {
    msg: Message<'a>,
}

/// Reader accepts bytes from the transport and returns complete message when available.
///
/// ```
/// # use undis::resp3::token::{Reader, Message, Error};
/// # use bytes::BufMut;
/// let mut reader = Reader::new();
/// reader.buf().put_slice(b"$12\r\nHello w");
/// assert_eq!(None, reader.read()?);
/// reader.buf().put_slice(b"orld!\r\n*2\r");
/// assert_eq!(b"$12\r\nHello world!\r\n", reader.read()?.unwrap().as_ref());
/// reader.buf().put_slice(b"\n:1\r\n:2\r\n-wh");
/// assert_eq!(b"$12\r\nHello world!\r\n", reader.read()?.unwrap().as_ref());
/// reader.consume();
/// assert_eq!(b"*2\r\n:1\r\n:2\r\n", reader.read()?.unwrap().as_ref());
/// reader.consume();
/// assert_eq!(None, reader.read()?);
/// # Ok::<_, Error>(())
/// ```
#[derive(Debug)]
pub struct Reader {
    buf: Vec<u8>,
    parsed_offset: usize,
    parsed_tokens: usize,
    stack_remainings: Vec<Option<NonZeroUsize>>,
}

/// Errors that occur when parsing the RESP3 protocol.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid prefix byte.
    #[error("invalid prefix byte")]
    InvalidPrefix,
    /// Expected `\r\n` bytes, but found something else.
    #[error("expected \\r\\n bytes, but found something else")]
    ExpectedCrlf,
    /// Failed to parse integer.
    #[error("failed to parse integer")]
    ParseIntFailed,
    /// Failed to parse decimal number.
    #[error("failed to parse decimal number")]
    ParseFloatFailed,
    /// Failed to parse boolean.
    #[error("failed to parse boolean")]
    ParseBoolFailed,
    /// Found stream end token outside from the streaming context.
    #[error("found stream end token outside from the streaming context")]
    UnexpectedStreamEnd,
}

const CRLF: &[u8] = b"\r\n";
const BOOL_TRUE: &[u8] = b"t";
const BOOL_FALSE: &[u8] = b"f";

const ARRAY: u8 = b'*';
const BLOB: u8 = b'$';
const BLOB_STREAM: u8 = b';';
const SIMPLE: u8 = b'+';
const ERROR: u8 = b'-';
const NUMBER: u8 = b':';
const NULL: u8 = b'_';
const DOUBLE: u8 = b',';
const BOOLEAN: u8 = b'#';
const BLOB_ERROR: u8 = b'!';
const VERBATIM: u8 = b'=';
const MAP: u8 = b'%';
const SET: u8 = b'~';
const ATTRIBUTE: u8 = b'|';
const PUSH: u8 = b'>';
const BIG_NUMBER: u8 = b'(';
const STREAM_START: u8 = b'?';
const STREAM_END: u8 = b'.';

const ONE: NonZeroUsize = match NonZeroUsize::new(1) {
    Some(one) => one,
    None => panic!("surprisingly, 1 is zero"),
};

impl<'a> Token<'a> {
    /// Write a token to the buffer.
    pub fn put<T: BufMut>(&self, buf: &mut T) {
        fn aggr_type<T: BufMut>(buf: &mut T, tag: u8, len: &Option<usize>) {
            buf.put_u8(tag);
            if let Some(len) = len {
                let mut nbuf = itoa::Buffer::new();
                buf.put_slice(nbuf.format(*len).as_bytes());
                buf.put_slice(CRLF);
            } else {
                buf.put_u8(STREAM_START);
                buf.put_slice(CRLF);
            }
        }
        let mut nbuf = itoa::Buffer::new();
        match self {
            Array(len) => aggr_type(buf, ARRAY, len),
            Blob(msg) => {
                buf.put_u8(BLOB);
                if let Some(msg) = msg {
                    buf.put_slice(nbuf.format(msg.len()).as_bytes());
                    buf.put_slice(CRLF);
                    buf.put_slice(msg);
                    buf.put_slice(CRLF);
                } else {
                    buf.put_u8(STREAM_START);
                    buf.put_slice(CRLF);
                }
            }
            BlobStream(msg) => {
                buf.put_u8(BLOB_STREAM);
                buf.put_slice(nbuf.format(msg.len()).as_bytes());
                buf.put_slice(CRLF);
                if !msg.is_empty() {
                    buf.put_slice(msg);
                    buf.put_slice(CRLF);
                }
            }
            Simple(msg) => {
                debug_assert!(
                    msg.iter().all(|&b| b != b'\r' && b != b'\n'),
                    "RESP Simple String can't have \\r or \\n character"
                );
                buf.put_u8(SIMPLE);
                buf.put_slice(msg);
                buf.put_slice(CRLF);
            }
            SimpleError(msg) => {
                debug_assert!(
                    msg.iter().all(|&b| b != b'\r' && b != b'\n'),
                    "RESP Simple Error can't have \\r or \\n character"
                );
                buf.put_u8(ERROR);
                buf.put_slice(msg);
                buf.put_slice(CRLF);
            }
            Number(num) => {
                buf.put_u8(NUMBER);
                buf.put_slice(nbuf.format(*num).as_bytes());
                buf.put_slice(CRLF);
            }
            Null => {
                buf.put_u8(NULL);
                buf.put_slice(CRLF);
            }
            Double(num) => {
                let mut fbuf = ryu::Buffer::new();
                buf.put_u8(DOUBLE);
                buf.put_slice(fbuf.format(num.get()).as_bytes());
                buf.put_slice(CRLF);
            }
            Boolean(b) => {
                buf.put_u8(BOOLEAN);
                if *b {
                    buf.put_slice(BOOL_TRUE);
                } else {
                    buf.put_slice(BOOL_FALSE);
                }
                buf.put_slice(CRLF);
            }
            BlobError(msg) => {
                buf.put_u8(BLOB_ERROR);
                buf.put_slice(nbuf.format(msg.len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(msg);
                buf.put_slice(CRLF);
            }
            Verbatim(msg) => {
                buf.put_u8(VERBATIM);
                buf.put_slice(nbuf.format(msg.len()).as_bytes());
                buf.put_slice(CRLF);
                buf.put_slice(msg);
                buf.put_slice(CRLF);
            }
            Map(len) => aggr_type(buf, MAP, len),
            Set(len) => aggr_type(buf, SET, len),
            Attribute(len) => {
                buf.put_u8(ATTRIBUTE);
                buf.put_slice(nbuf.format(*len).as_bytes());
                buf.put_slice(CRLF);
            }
            Push(len) => {
                buf.put_u8(PUSH);
                buf.put_slice(nbuf.format(*len).as_bytes());
                buf.put_slice(CRLF);
            }
            BigNumber(digits) => {
                debug_assert!(
                    digits
                        .iter()
                        .all(|b| b == &b'-' || (b'0'..=b'9').contains(b)),
                    "RESP Big Number can only have digits and a sign"
                );
                buf.put_u8(BIG_NUMBER);
                buf.put_slice(digits);
                buf.put_slice(CRLF);
            }
            StreamEnd => {
                buf.put_u8(STREAM_END);
                buf.put_slice(CRLF);
            }
        }
    }

    /// Process recursive type stack with self token.
    /// Start with the stack with `vec![Some(amt)]`
    /// where `amt` is the amount of token trees
    /// and keep call this function with new tokens
    /// until the stack being emptied.
    pub fn process_stack(&self, stack: &mut Vec<Option<NonZeroUsize>>) -> Result<(), Error> {
        let is_attr = matches!(self, Attribute(_));
        let is_stream_end = matches!(self, StreamEnd);

        // don't count the attribute element on counting sequence length
        if !is_attr {
            match (is_stream_end, stack.pop().unwrap()) {
                // stream is ended
                (true, None) => {}
                // stream not ended yet
                (false, None) => stack.push(None),
                // unexpected stream end token
                (true, Some(_)) => return Err(Error::UnexpectedStreamEnd),
                (false, Some(len)) => match NonZeroUsize::new(len.get() - 1) {
                    // the finite sequence is ended
                    None => {}
                    // advancing the finite sequence
                    Some(len) => stack.push(Some(len)),
                },
            }
        }

        // push the protocol stack frame if needed
        match self {
            // scalars don't push any frame
            Blob(Some(_)) | BlobStream(_) | Simple(_) | SimpleError(_) | Number(_) | Null
            | Double(_) | Boolean(_) | BlobError(_) | Verbatim(_) | BigNumber(_) | StreamEnd => {}
            // sequences push frame if they're not empty
            Array(Some(len)) | Set(Some(len)) | Attribute(len) | Push(len) => {
                if let Some(len) = NonZeroUsize::new(*len) {
                    stack.push(Some(len))
                }
            }
            // Map pushes the double of its length
            Map(Some(len)) => {
                if let Some(len) = NonZeroUsize::new(len * 2) {
                    stack.push(Some(len))
                }
            }
            // streaming sequence push frame without length
            Array(None) | Blob(None) | Map(None) | Set(None) => stack.push(None),
        }

        Ok(())
    }
}

impl<'a> Message<'a> {
    /// Peek the first token within this message.
    pub fn head(&self) -> Token<'a> {
        // Message never be empty
        self.clone().into_iter().next().unwrap()
    }
}

impl<'a> IntoIterator for Message<'a> {
    type Item = Token<'a>;
    type IntoIter = MessageIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        MessageIter { msg: self }
    }
}

impl<'a> AsRef<[u8]> for Message<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buf
    }
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.msg.tokens == 0 {
            debug_assert!(self.msg.buf.is_empty());
            None
        } else {
            self.msg.tokens -= 1;
            Some(next_token(&mut self.msg.buf).unwrap())
        }
    }
}

impl<'a> AsRef<[u8]> for MessageIter<'a> {
    fn as_ref(&self) -> &[u8] {
        self.msg.buf
    }
}

impl Reader {
    /// Constructs a new `Reader`.
    pub fn new() -> Self {
        Reader {
            buf: vec![],
            parsed_offset: 0,
            parsed_tokens: 0,
            stack_remainings: vec![Some(ONE)], // to parse 1 msg on start
        }
    }

    /// Get the buffer to feed incoming bytes.
    pub fn buf(&mut self) -> &mut impl BufMut {
        &mut self.buf
    }

    /// Returns message view stored within the internal buffer on success.
    /// Returns `Ok(None)` if more bytes needed to parse.
    /// To get next message after this call, you need to call `.consume()`
    /// otherwise the same message would be returned again.
    pub fn read(&mut self) -> Result<Option<Message<'_>>, Error> {
        while !self.stack_remainings.is_empty() {
            let mut buf = &self.buf[self.parsed_offset..];
            let token = match next_token(&mut buf) {
                Ok(tok) => tok,
                Err(None) => return Ok(None),
                Err(Some(err)) => return Err(err),
            };
            self.parsed_tokens += 1;
            self.parsed_offset = self.buf.len() - buf.len();

            token.process_stack(&mut self.stack_remainings)?;
        }

        let msg = self.peek();
        assert!(msg.is_some(), "msg should exist at this point");
        Ok(msg)
    }

    /// Peek a message stored within the `Reader` without modifying internal state.
    pub fn peek(&self) -> Option<Message<'_>> {
        self.stack_remainings.is_empty().then(|| {
            debug_assert!(self.parsed_tokens > 0, "Message should never be empty");
            Message {
                buf: &self.buf[..self.parsed_offset],
                tokens: self.parsed_tokens,
            }
        })
    }

    /// Consume currently parsed message.
    pub fn consume(&mut self) {
        if self.stack_remainings.is_empty() {
            self.buf.drain(..self.parsed_offset);
            self.parsed_tokens = 0;
            self.parsed_offset = 0;
            self.stack_remainings.push(Some(ONE)); // to parse 1 msg next time
        }
    }

    /// Reset the internal state of the `Reader`.
    pub fn reset(&mut self) {
        self.stack_remainings.clear();
        self.stack_remainings.push(Some(ONE));
        self.buf.clear();
        self.parsed_tokens = 0;
        self.parsed_offset = 0;
    }
}

impl Default for Reader {
    fn default() -> Self {
        Self::new()
    }
}

fn next_token<'a>(buf: &mut &'a [u8]) -> Result<Token<'a>, Option<Error>> {
    fn until_crlf<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], Option<Error>> {
        static CRLF_SEARCH: Lazy<Finder> = Lazy::new(|| Finder::new(CRLF));

        let idx = CRLF_SEARCH.find(buf).ok_or(None)?;
        let res = &buf[..idx];
        buf.advance(idx + CRLF.len());

        Ok(res)
    }

    fn until_len_crlf<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8], Option<Error>> {
        if buf.len() < len + CRLF.len() {
            return Err(None);
        }
        let (msg_crlf, remains) = buf.split_at(len + CRLF.len());
        *buf = remains;
        let (msg, crlf) = msg_crlf.split_at(len);
        if crlf != CRLF {
            return Err(Error::ExpectedCrlf.into());
        }
        Ok(msg)
    }

    fn parse_blob_like<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], Option<Error>> {
        let len = parse_len(buf)?;
        until_len_crlf(buf, len)
    }

    fn parse_len(buf: &mut &[u8]) -> Result<usize, Option<Error>> {
        let msg = until_crlf(buf)?;
        Ok(parse_str::<usize>(msg).ok_or(Error::ParseIntFailed)?)
    }

    fn parse_opt_len(buf: &mut &[u8]) -> Result<Option<usize>, Option<Error>> {
        let msg = until_crlf(buf)?;
        Ok(if msg == std::slice::from_ref(&STREAM_START) {
            None
        } else {
            Some(parse_str::<usize>(msg).ok_or(Error::ParseIntFailed)?)
        })
    }

    if buf.is_empty() {
        return Err(None);
    }

    Ok(match buf.get_u8() {
        ARRAY => Array(parse_opt_len(buf)?),
        BLOB => Blob(
            parse_opt_len(buf)?
                .map(|len| until_len_crlf(buf, len))
                .transpose()?,
        ),
        BLOB_STREAM => BlobStream(parse_blob_like(buf)?),
        SIMPLE => Simple(until_crlf(buf)?),
        ERROR => SimpleError(until_crlf(buf)?),
        NUMBER => Number({
            let msg = until_crlf(buf)?;
            parse_str(msg).ok_or(Error::ParseIntFailed)?
        }),
        NULL => {
            until_len_crlf(buf, 0)?;
            Null
        }
        DOUBLE => Double({
            let msg = until_crlf(buf)?;
            parse_str(msg).ok_or(Error::ParseFloatFailed)?
        }),
        BOOLEAN => Boolean(match until_crlf(buf)? {
            BOOL_TRUE => true,
            BOOL_FALSE => false,
            _ => return Err(Error::ParseBoolFailed.into()),
        }),
        BLOB_ERROR => BlobError(parse_blob_like(buf)?),
        VERBATIM => Verbatim(parse_blob_like(buf)?),
        MAP => Map(parse_opt_len(buf)?),
        SET => Set(parse_opt_len(buf)?),
        ATTRIBUTE => Attribute(parse_len(buf)?),
        PUSH => Push(parse_len(buf)?),
        BIG_NUMBER => BigNumber({
            let msg = until_crlf(buf)?;
            if msg.is_empty() || msg == b"-" {
                return Err(Error::ParseIntFailed.into());
            }
            let mut exclude_sign = msg;
            if exclude_sign[0] == b'-' {
                exclude_sign = &exclude_sign[1..];
            }
            for digit in exclude_sign {
                if !(b'0'..=b'9').contains(digit) {
                    return Err(Error::ParseIntFailed.into());
                }
            }
            msg
        }),
        STREAM_END => {
            until_len_crlf(buf, 0)?;
            StreamEnd
        }
        _ => return Err(Error::InvalidPrefix.into()),
    })
}
