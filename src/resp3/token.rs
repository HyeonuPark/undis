use std::num::NonZeroUsize;

use bytes::{Buf, BufMut};
use memchr::memmem::Finder;
use once_cell::sync::Lazy;

use super::parse_str;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub enum Token<'a> {
    Array(Option<usize>),
    Blob(Option<&'a [u8]>),
    BlobStream(&'a [u8]),
    Simple(&'a [u8]),
    SimpleError(&'a [u8]),
    Number(i64),
    Null,
    Double(f64),
    Boolean(bool),
    BlobError(&'a [u8]),
    Verbatim(&'a [u8]),
    Map(Option<usize>),
    Set(Option<usize>),
    Attribute(usize),
    Push(usize),
    BigNumber(&'a [u8]),
    StreamEnd,
}

use Token::*;

#[derive(Debug, Clone)]
pub struct Message<'a> {
    buf: &'a [u8],
    length: usize,
}

#[derive(Debug)]
pub struct Reader {
    buf: Vec<u8>,
    parsed_offset: usize,
    parsed_tokens: usize,
    stack_remainings: Vec<Option<NonZeroUsize>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid prefix byte")]
    InvalidPrefix,
    #[error("expected \\r\\n bytes, but found something else")]
    ExpectedCrlf,
    #[error("failed to parse integer")]
    ParseIntFailed,
    #[error("failed to parse decimal number")]
    ParseFloatFailed,
    #[error("failed to parse boolean")]
    ParseBoolFailed,
    #[error("found stream END token outside from the streaming context")]
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
                buf.put_slice(fbuf.format(*num).as_bytes());
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

impl<'a> Iterator for Message<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.length == 0 {
            debug_assert!(self.buf.is_empty());
            None
        } else {
            self.length -= 1;
            Some(next_token(&mut self.buf).unwrap())
        }
    }
}

impl<'a> AsRef<[u8]> for Message<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buf
    }
}

impl Reader {
    pub fn new() -> Self {
        Reader {
            buf: vec![],
            parsed_offset: 0,
            parsed_tokens: 0,
            stack_remainings: vec![Some(ONE)], // to parse 1 msg on start
        }
    }

    pub fn buf(&mut self) -> &mut impl BufMut {
        &mut self.buf
    }

    /// Returns message view stored within the internal buffer on success.
    /// Returns `Err(None)` if more bytes needed to parse.
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

    pub fn peek(&self) -> Option<Message<'_>> {
        self.stack_remainings.is_empty().then(|| Message {
            buf: &self.buf[..self.parsed_offset],
            length: self.parsed_tokens,
        })
    }

    pub fn consume(&mut self) {
        if self.stack_remainings.is_empty() {
            self.buf.drain(..self.parsed_offset);
            self.parsed_tokens = 0;
            self.parsed_offset = 0;
            self.stack_remainings.push(Some(ONE)); // to parse 1 msg next time
        }
    }

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
