//! Serialize Rust data structures into Redis command.

use std::fmt;

use bytes::BufMut;
use paste::paste;
use serde::ser::{self, Serialize};

use super::token::Token;

/// Write the command to the buffer.
///
/// *Do not use* the content of the buffer itself; use the returned slice instead.
/// For efficiency, the buffer may contain invalid bytes at the beginning after this operation,
/// and therefore using its content as command may result in failure.
pub fn write_cmd<'a, T: serde::Serialize>(
    buf: &'a mut Vec<u8>,
    value: &T,
) -> Result<&'a [u8], Error> {
    // u32::MAX == 4,294,967,295
    const MAX_DIGITS: usize = 10;
    // *999\r\n
    const CMD_HEADER_CAP: usize = 1 + MAX_DIGITS + 2;

    buf.resize(CMD_HEADER_CAP, 0);
    let count = value.serialize(CommandSerializer::new(buf))?;

    let mut hbuf = &mut buf[..CMD_HEADER_CAP];
    Token::Array(Some(count)).put(&mut hbuf);
    let offset = hbuf.len();
    buf.copy_within(..CMD_HEADER_CAP - offset, offset);

    Ok(&buf[offset..])
}

/// Errors that may occur when serializing commands.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Serde related error.
    #[error("serde: {0}")]
    Serde(String),
    /// Only sequence-like types are supported as a command root.
    #[error("only sequence-like types are supported as a command root")]
    NotSequenceRoot,
    /// Types nested more than 2 depth are not supported.
    #[error("types nested more than 2 depth are not supported")]
    NestedTooDeep,
}

/// A structure for serializing Rust values into Redis command.
#[derive(Debug)]
pub struct CommandSerializer<'a, B: BufMut> {
    buf: &'a mut B,
    count: usize,
}

#[derive(Debug)]
struct FlatArraySerializer<'a, B: BufMut> {
    buf: &'a mut B,
    count: usize,
}

#[derive(Debug)]
struct BlobSerializer<'a, B: BufMut> {
    buf: &'a mut B,
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Error::Serde(msg.to_string())
    }
}

impl<'a, B: BufMut> CommandSerializer<'a, B> {
    /// Creates a new Redis command serializer.
    pub fn new(buf: &'a mut B) -> Self {
        CommandSerializer { buf, count: 0 }
    }

    fn put<T: AsRef<[u8]> + ?Sized>(&mut self, blob: &T) {
        Token::Blob(Some(blob.as_ref())).put(self.buf);
        self.count += 1;
    }

    fn elem<T: Serialize + ?Sized>(&mut self, elem: &T) -> Result<(), Error> {
        let szr = FlatArraySerializer {
            buf: self.buf,
            count: 0,
        };
        self.count += elem.serialize(szr)?;
        Ok(())
    }

    fn done(self) -> Result<usize, Error> {
        Ok(self.count)
    }
}

impl<'a, B: BufMut> FlatArraySerializer<'a, B> {
    fn put<T: AsRef<[u8]> + ?Sized>(&mut self, blob: &T) {
        Token::Blob(Some(blob.as_ref())).put(self.buf);
        self.count += 1;
    }

    fn blob(&mut self) -> BlobSerializer<'_, B> {
        self.count += 1;
        BlobSerializer { buf: self.buf }
    }

    fn done(self) -> Result<usize, Error> {
        Ok(self.count)
    }
}

impl<'a, B: BufMut> BlobSerializer<'a, B> {
    fn put<T: AsRef<[u8]> + ?Sized>(&mut self, blob: &T) {
        Token::Blob(Some(blob.as_ref())).put(self.buf);
    }
}

macro_rules! unsupported_not_seq {
    ($($name:ident)*) => {paste!{$(
        fn [<serialize_ $name>](self, _v: $name) -> Result<Self::Ok, Self::Error> {
            Err(Error::NotSequenceRoot)
        }
    )*}};
}

impl<'a, B: BufMut> ser::Serializer for CommandSerializer<'a, B> {
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = ser::Impossible<usize, Error>;
    type SerializeStruct = ser::Impossible<usize, Error>;
    type SerializeStructVariant = ser::Impossible<usize, Error>;

    unsupported_not_seq!(bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64);

    // char, str, bytes and unit-variant are allowed to write command without arguments

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        self.put(v.encode_utf8(&mut [0; 4]).as_bytes());
        self.done()
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.put(v.as_bytes());
        self.done()
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.put(v);
        self.done()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_unit_variant(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.put(variant);
        self.done()
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.put(variant);
        self.elem(value)?;
        self.done()
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.put(variant);
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::NotSequenceRoot)
    }
}

impl<'a, B: BufMut> ser::SerializeSeq for CommandSerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elem(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeTuple for CommandSerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elem(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeTupleStruct for CommandSerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elem(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeTupleVariant for CommandSerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        self.elem(value)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

macro_rules! serialize_single {
    ($($target:ident)*) => {paste!{$(
        fn [<serialize_ $target>](mut self, v: $target) -> Result<Self::Ok, Self::Error> {
            self.blob().[<serialize_ $target>](v)?;
            Ok(1)
        }
    )*}};
}

impl<'a, B: BufMut> ser::Serializer for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    serialize_single!(bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char);

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.blob().serialize_str(v)?;
        Ok(1)
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.blob().serialize_bytes(v)?;
        Ok(1)
    }

    /// Doesn't put anything on None
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    /// Doesn't put anything on Unit-like types
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(0)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.put(variant);
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.put(variant);
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_struct_variant(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.put(variant);
        Ok(self)
    }
}

impl<'a, B: BufMut> ser::SerializeSeq for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeTuple for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeTupleStruct for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeTupleVariant for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeMap for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        key.serialize(self.blob())
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeStruct for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        key.serialize(self.blob())?;
        value.serialize(self.blob())?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a, B: BufMut> ser::SerializeStructVariant for FlatArraySerializer<'a, B> {
    type Ok = usize;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        key.serialize(self.blob())?;
        value.serialize(self.blob())?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

macro_rules! serialize_int {
    ($($int:ident)*) => {paste!{$(
        fn [<serialize_ $int>](mut self, v: $int) -> Result<Self::Ok, Self::Error> {
            let mut buf = itoa::Buffer::new();
            self.put(buf.format(v));
            Ok(())
        }
    )*}};
}

macro_rules! serialize_float {
    ($($float:ident)*) => {paste!{$(
        fn [<serialize_ $float>](mut self, v: $float) -> Result<Self::Ok, Self::Error> {
            let mut buf = ryu::Buffer::new();
            self.put(buf.format(v));
            Ok(())
        }
    )*}};
}

impl<'a, B: BufMut> ser::Serializer for BlobSerializer<'a, B> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_bool(mut self, v: bool) -> Result<Self::Ok, Self::Error> {
        let msg = match v {
            true => b"1",
            false => b"0",
        };
        self.put(msg);
        Ok(())
    }

    serialize_int!(i8 i16 i32 i64 i128 u8 u16 u32 u64 u128);
    serialize_float!(f32 f64);

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        let mut buf = [0; 4];
        self.put(v.encode_utf8(&mut buf));
        Ok(())
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.put(v);
        Ok(())
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.put(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(mut self) -> Result<Self::Ok, Self::Error> {
        self.put("");
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.put(variant);
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(Error::NestedTooDeep)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(Error::NestedTooDeep)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(Error::NestedTooDeep)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(Error::NestedTooDeep)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::NestedTooDeep)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::NestedTooDeep)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::NestedTooDeep)
    }
}
