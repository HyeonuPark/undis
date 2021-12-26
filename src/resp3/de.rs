use std::borrow::Cow;
use std::{fmt, str};

use paste::paste;
use serde::de;

use super::parse_str;
use super::token::{Message, Token};

#[derive(Debug)]
pub struct Deserializer<'a, 'de> {
    msg: &'a mut Message<'de>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("custom: {0}")]
    Custom(String),
    #[error("unexpected end of the message")]
    Eom,
    #[error("error sent from the remote peer: {0}")]
    Remote(String),
    #[error("integer overflowed")]
    IntegerOverflow,
    #[error("unexpected streaming blob token")]
    UnexpectedStreamingBlob,
    #[error("unexpected stream end token")]
    UnexpectedStreamEnd,
    #[error("parse int failed")]
    ParseIntFailed,
    #[error("parse decimal number failed")]
    ParseDecimalFailed,
    #[error("parse single character failed")]
    ParseCharFailed,
    #[error("parse string failed")]
    ParseStrFailed,
    #[error("cannot parse variant fields from single blob")]
    BlobVariantRequestFields,
    #[error("requested length is not match with the message")]
    LengthMismatch,
}

#[derive(Debug)]
enum Kind<'a> {
    Blob(&'a [u8]),
    BlobOwned(Vec<u8>),
    Num(i64),
    BigNum(&'a [u8]),
    Null,
    Double(f64),
    Bool(bool),
    Array(Option<usize>),
    Set(Option<usize>),
    Map(Option<usize>),
    Push(usize),
    StreamEnd,
}

#[derive(Debug)]
struct SeqAccess<'a, 'de> {
    des: Deserializer<'a, 'de>,
    len: Option<usize>,
}

#[derive(Debug)]
struct BlobAccess<'de> {
    blob: Cow<'de, [u8]>,
}

impl<'a, 'de> Deserializer<'a, 'de> {
    pub fn new(msg: &'a mut Message<'de>) -> Self {
        Self { msg }
    }

    fn seq(self, len: Option<usize>) -> SeqAccess<'a, 'de> {
        SeqAccess { len, des: self }
    }

    fn peek(&mut self) -> Result<Kind<'de>, Error> {
        let msg = self.msg.clone();
        let res = self.tok();
        *self.msg = msg;
        res
    }

    fn tok(&mut self) -> Result<Kind<'de>, Error> {
        loop {
            return match self.msg.next() {
                None => Err(Error::Eom),
                Some(Token::Attribute(len)) => {
                    self.consume(len)?;
                    continue;
                }
                Some(Token::SimpleError(msg) | Token::BlobError(msg)) => {
                    Err(Error::Remote(String::from_utf8_lossy(msg).into_owned()))
                }
                Some(Token::Simple(msg) | Token::Blob(Some(msg)) | Token::Verbatim(msg)) => {
                    Ok(Kind::Blob(msg))
                }
                Some(Token::Blob(None)) => {
                    let mut buf = vec![];
                    loop {
                        match self.tok()? {
                            Kind::Blob(&[]) => return Ok(Kind::BlobOwned(buf)),
                            Kind::Blob(msg) => buf.extend_from_slice(msg),
                            other => return invalid_token(other, "streaming blobs"),
                        }
                    }
                }
                Some(Token::BlobStream(_)) => {
                    return Err(Error::UnexpectedStreamingBlob);
                }
                Some(Token::Number(num)) => Ok(Kind::Num(num)),
                Some(Token::BigNumber(digits)) => Ok(Kind::BigNum(digits)),
                Some(Token::Null) => Ok(Kind::Null),
                Some(Token::Double(num)) => Ok(Kind::Double(num)),
                Some(Token::Boolean(b)) => Ok(Kind::Bool(b)),
                Some(Token::Array(len)) => Ok(Kind::Array(len)),
                Some(Token::Set(len)) => Ok(Kind::Set(len)),
                Some(Token::Map(len)) => Ok(Kind::Map(len)),
                Some(Token::Push(len)) => Ok(Kind::Push(len)),
                Some(Token::StreamEnd) => Ok(Kind::StreamEnd),
            };
        }
    }

    fn consume(&mut self, amt: usize) -> Result<(), Error> {
        let mut stack = vec![Some(amt)];

        while !stack.is_empty() {
            match self.msg.next() {
                None => return Err(Error::Eom),
                Some(token) => match token.process_stack(&mut stack) {
                    Ok(()) => {}
                    Err(super::token::Error::UnexpectedStreamEnd) => return Err(Error::Eom),
                    Err(_) => unreachable!(),
                },
            }
        }

        Ok(())
    }
}

impl<'a, 'de> SeqAccess<'a, 'de> {
    fn child(&mut self) -> Deserializer<'_, 'de> {
        Deserializer { msg: self.des.msg }
    }
}

impl<'de> BlobAccess<'de> {
    fn new<T: Into<Cow<'de, [u8]>>>(msg: T) -> Self {
        BlobAccess { blob: msg.into() }
    }
}

macro_rules! deserialize_small_int {
    ($($int:ident)*) => {paste!{$(
        fn [<deserialize_ $int>]<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            match self.tok()? {
                Kind::Num(num) => visitor.[<visit_ $int>](num.try_into().map_err(|_| Error::IntegerOverflow)?),
                Kind::Blob(msg) => visitor.[<visit_ $int>](parse_str(msg).ok_or(Error::ParseIntFailed)?),
                Kind::BlobOwned(msg) => visitor.[<visit_ $int>](parse_str(&msg).ok_or(Error::ParseIntFailed)?),
                other => return invalid_token(other, "integer"),
            }
        }
    )*}};
}

macro_rules! deserialize_large_int {
    ($($int:ident)*) => {paste!{$(
        fn [<deserialize_ $int>]<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            match self.tok()? {
                Kind::Num(num) => visitor.[<visit_ $int>](num.try_into().map_err(|_| Error::IntegerOverflow)?),
                Kind::BigNum(digits) => visitor.[<visit_ $int>](parse_str(digits).ok_or(Error::IntegerOverflow)?),
                Kind::Blob(msg) => visitor.[<visit_ $int>](parse_str(msg).ok_or(Error::ParseIntFailed)?),
                Kind::BlobOwned(msg) => visitor.[<visit_ $int>](parse_str(&msg).ok_or(Error::ParseIntFailed)?),
                other => return invalid_token(other, "integer"),
            }
        }
    )*}};
}

impl<'a, 'de> de::Deserializer<'de> for Deserializer<'a, 'de> {
    type Error = Error;

    fn deserialize_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => match std::str::from_utf8(msg) {
                Ok(msg) => visitor.visit_borrowed_str(msg),
                Err(_) => visitor.visit_borrowed_bytes(msg),
            },
            Kind::BlobOwned(msg) => match String::from_utf8(msg) {
                Ok(msg) => visitor.visit_string(msg),
                Err(err) => visitor.visit_byte_buf(err.into_bytes()),
            },
            Kind::Num(num) => visitor.visit_i64(num),
            Kind::BigNum(digits) => {
                if digits[0] == b'-' {
                    if let Some(num) = parse_str(digits) {
                        visitor.visit_i128(num)
                    } else {
                        Err(Error::IntegerOverflow)
                    }
                } else if let Some(num) = parse_str(digits) {
                    visitor.visit_u64(num)
                } else if let Some(num) = parse_str(digits) {
                    visitor.visit_u128(num)
                } else {
                    Err(Error::IntegerOverflow)
                }
            }
            Kind::Null => visitor.visit_unit(),
            Kind::Double(num) => visitor.visit_f64(num),
            Kind::Bool(b) => visitor.visit_bool(b),
            Kind::Array(len) | Kind::Set(len) => visitor.visit_seq(self.seq(len)),
            Kind::Map(len) => visitor.visit_map(self.seq(len)),
            Kind::Push(len) => visitor.visit_map(self.seq(Some(len))),
            Kind::StreamEnd => Err(Error::UnexpectedStreamEnd),
        }
    }

    fn deserialize_bool<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        fn parse(msg: &[u8]) -> Result<bool, Error> {
            match msg {
                b"1" => Ok(true),
                b"0" => Ok(false),
                other => invalid_token(Kind::Blob(other), "boolean"),
            }
        }
        match self.tok()? {
            Kind::Bool(b) => visitor.visit_bool(b),
            Kind::Blob(msg) => visitor.visit_bool(parse(msg)?),
            Kind::BlobOwned(msg) => visitor.visit_bool(parse(&msg)?),
            Kind::Num(1) => visitor.visit_bool(true),
            Kind::Num(0) => visitor.visit_bool(false),
            other => invalid_token(other, "boolean"),
        }
    }

    deserialize_small_int!(i8 i16 i32 i64 u8 u16 u32);
    deserialize_large_int!(i128 u64 u128);

    fn deserialize_f32<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Double(num) => visitor.visit_f32(num as _),
            Kind::Blob(msg) => visitor.visit_f32(parse_str(msg).ok_or(Error::ParseDecimalFailed)?),
            Kind::BlobOwned(msg) => {
                visitor.visit_f32(parse_str(&msg).ok_or(Error::ParseDecimalFailed)?)
            }
            other => invalid_token(other, "decimal number"),
        }
    }

    fn deserialize_f64<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Double(num) => visitor.visit_f64(num),
            Kind::Blob(msg) => visitor.visit_f64(parse_str(msg).ok_or(Error::ParseDecimalFailed)?),
            Kind::BlobOwned(msg) => {
                visitor.visit_f64(parse_str(&msg).ok_or(Error::ParseDecimalFailed)?)
            }
            other => invalid_token(other, "decimal number"),
        }
    }

    fn deserialize_char<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => visitor.visit_char(parse_str(msg).ok_or(Error::ParseCharFailed)?),
            Kind::BlobOwned(msg) => {
                visitor.visit_char(parse_str(&msg).ok_or(Error::ParseCharFailed)?)
            }
            other => invalid_token(other, "single character"),
        }
    }

    fn deserialize_str<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => {
                visitor.visit_str(str::from_utf8(msg).map_err(|_| Error::ParseStrFailed)?)
            }
            Kind::BlobOwned(msg) => {
                visitor.visit_str(str::from_utf8(&msg).map_err(|_| Error::ParseStrFailed)?)
            }
            other => invalid_token(other, "string"),
        }
    }

    fn deserialize_string<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => visitor.visit_string(
                str::from_utf8(msg)
                    .map_err(|_| Error::ParseStrFailed)?
                    .into(),
            ),
            Kind::BlobOwned(msg) => {
                visitor.visit_string(String::from_utf8(msg).map_err(|_| Error::ParseStrFailed)?)
            }
            other => invalid_token(other, "string"),
        }
    }

    fn deserialize_bytes<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => visitor.visit_bytes(msg),
            Kind::BlobOwned(msg) => visitor.visit_bytes(&msg),
            other => invalid_token(other, "bytes"),
        }
    }

    fn deserialize_byte_buf<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => visitor.visit_byte_buf(msg.into()),
            Kind::BlobOwned(msg) => visitor.visit_byte_buf(msg),
            other => invalid_token(other, "bytes"),
        }
    }

    fn deserialize_option<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.peek()? {
            Kind::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Null | Kind::Blob(b"") => visitor.visit_unit(),
            Kind::BlobOwned(msg) if msg.is_empty() => visitor.visit_unit(),
            other => invalid_token(other, "unit type"),
        }
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Array(len) | Kind::Set(len) => visitor.visit_seq(self.seq(len)),
            other => invalid_token(other, "sequence"),
        }
    }

    fn deserialize_tuple<V>(mut self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Array(Some(seq_len)) if len == seq_len => visitor.visit_seq(self.seq(Some(len))),
            Kind::Array(None) => visitor.visit_seq(self.seq(Some(len))),
            other => invalid_token(other, "sequence"),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Map(len) => visitor.visit_map(self.seq(len)),
            Kind::Push(len) => visitor.visit_map(self.seq(Some(len))),
            other => invalid_token(other, "map"),
        }
    }

    fn deserialize_struct<V>(
        mut self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Array(Some(seq_len)) if fields.len() == seq_len => {
                visitor.visit_seq(self.seq(Some(seq_len)))
            }
            Kind::Array(None) => visitor.visit_seq(self.seq(Some(fields.len()))),
            Kind::Map(Some(map_len)) | Kind::Push(map_len) if fields.len() == map_len => {
                visitor.visit_map(self.seq(Some(map_len)))
            }
            Kind::Map(None) => visitor.visit_map(self.seq(Some(fields.len()))),
            other => invalid_token(other, "struct"),
        }
    }

    fn deserialize_enum<V>(
        mut self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.tok()? {
            Kind::Blob(msg) => visitor.visit_enum(BlobAccess::new(msg)),
            Kind::BlobOwned(msg) => visitor.visit_enum(BlobAccess::new(msg)),
            Kind::Array(len) => visitor.visit_enum(self.seq(len)),
            other => invalid_token(other, "enum"),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.consume(1)?;
        visitor.visit_unit()
    }
}

impl<'a, 'de> de::SeqAccess<'de> for SeqAccess<'a, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        match self.len {
            Some(0) => Ok(None),
            Some(len) => {
                self.len = Some(len - 1);
                seed.deserialize(self.child()).map(Some)
            }
            None => match self.des.peek()? {
                Kind::StreamEnd => {
                    let _ = self.des.tok();
                    Ok(None)
                }
                _ => seed.deserialize(self.child()).map(Some),
            },
        }
    }
}

impl<'a, 'de> de::MapAccess<'de> for SeqAccess<'a, 'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        // same logic with seq next
        de::SeqAccess::next_element_seed(self, seed)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self.child())
    }
}

impl<'de> de::EnumAccess<'de> for BlobAccess<'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        use de::IntoDeserializer;

        let variant = match &self.blob {
            Cow::Owned(msg) => seed.deserialize(msg[..].into_deserializer())?,
            Cow::Borrowed(msg) => {
                seed.deserialize(de::value::BorrowedBytesDeserializer::new(*msg))?
            }
        };
        Ok((variant, self))
    }
}

impl<'de> de::VariantAccess<'de> for BlobAccess<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        Err(Error::BlobVariantRequestFields)
    }

    fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        Err(Error::BlobVariantRequestFields)
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        Err(Error::BlobVariantRequestFields)
    }
}

impl<'a, 'de> de::EnumAccess<'de> for SeqAccess<'a, 'de> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(mut self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let variant = match self.len {
            Some(0) => return Err(Error::LengthMismatch),
            Some(len) => {
                self.len = Some(len - 1);
                seed.deserialize(self.child())?
            }
            None => match self.des.peek()? {
                Kind::StreamEnd => return Err(Error::UnexpectedStreamEnd),
                _ => seed.deserialize(self.child())?,
            },
        };

        Ok((variant, self))
    }
}

impl<'a, 'de> de::VariantAccess<'de> for SeqAccess<'a, 'de> {
    type Error = Error;

    fn unit_variant(mut self) -> Result<(), Self::Error> {
        match self.len {
            Some(0) => Ok(()),
            Some(_) => Err(Error::LengthMismatch),
            None => match self.des.peek()? {
                Kind::StreamEnd => Ok(()),
                other => invalid_token(other, "stream end"),
            },
        }
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self)
    }

    fn tuple_variant<V>(self, tuple_len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.len.map_or(false, |len| len != tuple_len) {
            return Err(Error::LengthMismatch);
        }

        visitor.visit_seq(self)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.len.map_or(false, |len| len != fields.len()) {
            return Err(Error::LengthMismatch);
        }

        visitor.visit_seq(self)
    }
}

macro_rules! forward_single {
    ($($kind:ident)*) => {paste!{$(
        fn [<deserialize_ $kind>]<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            if self.len.map_or(false, |len| len != 1) {
                return Err(Error::LengthMismatch);
            }

            let res = self.child().[<deserialize_ $kind>](visitor)?;

            if self.len.is_none() {
                // streaming seq
                match self.des.tok()? {
                    Kind::StreamEnd => {}
                    other => return invalid_token(other, "stream end"),
                }
            }

            Ok(res)
        }
    )*}};
}

impl<'a, 'de> de::Deserializer<'de> for SeqAccess<'a, 'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    forward_single!(bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64);
    forward_single!(char str string bytes byte_buf option unit map);

    fn deserialize_unit_struct<V>(
        mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.len.map_or(false, |len| len != 1) {
            return Err(Error::LengthMismatch);
        }

        let res = self.child().deserialize_unit_struct(name, visitor)?;

        if self.len.is_none() {
            // streaming seq
            match self.des.tok()? {
                Kind::StreamEnd => {}
                other => return invalid_token(other, "stream end"),
            }
        }

        Ok(res)
    }

    fn deserialize_newtype_struct<V>(
        mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.len.map_or(false, |len| len != 1) {
            return Err(Error::LengthMismatch);
        }

        let res = self.child().deserialize_newtype_struct(name, visitor)?;

        if self.len.is_none() {
            // streaming seq
            match self.des.tok()? {
                Kind::StreamEnd => {}
                other => return invalid_token(other, "stream end"),
            }
        }

        Ok(res)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, tuple_len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.len.map_or(false, |len| len != tuple_len) {
            return Err(Error::LengthMismatch);
        }

        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if self.len.map_or(false, |len| len != fields.len()) {
            return Err(Error::LengthMismatch);
        }

        visitor.visit_seq(self)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(mut self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        if let Some(len) = self.len {
            self.des.consume(len)?;
        } else {
            loop {
                match self.des.peek()? {
                    Kind::StreamEnd => {
                        let _ = self.des.tok();
                        break;
                    }
                    _ => self.des.consume(1)?,
                }
            }
        }

        visitor.visit_unit()
    }
}

impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Error::Custom(msg.to_string())
    }
}

fn invalid_token<T>(kind: Kind<'_>, expected: &'static str) -> Result<T, Error> {
    use de::{Error as _, Unexpected};

    let unexpected = match kind {
        Kind::Blob(msg) => Unexpected::Bytes(msg),
        Kind::BlobOwned(ref msg) => Unexpected::Bytes(msg),
        Kind::Num(n) => Unexpected::Signed(n),
        Kind::BigNum(_) => Unexpected::Other("BigNumber"),
        Kind::Null => Unexpected::Unit,
        Kind::Double(n) => Unexpected::Float(n),
        Kind::Bool(b) => Unexpected::Bool(b),
        Kind::Array(_) | Kind::Set(_) => Unexpected::Seq,
        Kind::Map(_) | Kind::Push(_) => Unexpected::Map,
        Kind::StreamEnd => Unexpected::Other("StreamEnd"),
    };

    Err(Error::invalid_type(unexpected, &expected))
}
