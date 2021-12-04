use std::fmt;

use paste::paste;
use serde::ser::{self, Serialize};

use super::token::Token;

#[derive(Debug)]
pub struct CommandSerializer<'a> {
    buf: &'a mut Vec<u8>,
    count: usize,
}

#[derive(Debug)]
pub struct FlatArraySerializer<'a> {
    buf: &'a mut Vec<u8>,
    count: &'a mut usize,
}

#[derive(Debug)]
pub struct BlobSerializer<'a> {
    buf: &'a mut Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("custom: {0}")]
    Custom(String),
    #[error("only sequence-like types are supported as a command root")]
    NotSequenceRoot,
    #[error("deeply nested types are not supported")]
    NestedTooDeep,
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: fmt::Display,
    {
        Error::Custom(msg.to_string())
    }
}

impl<'a> CommandSerializer<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        buf.clear();
        Token::Array(None).put(buf);
        CommandSerializer { buf, count: 0 }
    }

    fn seq(&mut self) -> FlatArraySerializer<'_> {
        FlatArraySerializer {
            buf: self.buf,
            count: &mut self.count,
        }
    }

    fn put<T: AsRef<[u8]> + ?Sized>(&mut self, blob: &T) {
        self.count += 1;
        Token::Blob(Some(blob.as_ref())).put(self.buf);
    }

    fn done(self) -> Result<(), Error> {
        if self.count <= 9 {
            self.buf[1] = b'0' + self.count as u8;
        } else {
            Token::StreamEnd.put(self.buf);
        }

        Ok(())
    }
}

impl<'a> FlatArraySerializer<'a> {
    fn put<T: AsRef<[u8]> + ?Sized>(&mut self, blob: &T) {
        *self.count += 1;
        Token::Blob(Some(blob.as_ref())).put(self.buf);
    }

    fn blob(&mut self) -> BlobSerializer<'_> {
        *self.count += 1;
        BlobSerializer { buf: self.buf }
    }
}

impl<'a> BlobSerializer<'a> {
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

impl<'a> ser::Serializer for CommandSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    unsupported_not_seq!(bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char);

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSequenceRoot)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(Error::NotSequenceRoot)
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
        value.serialize(self.seq())
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

impl<'a> ser::SerializeSeq for CommandSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.seq())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a> ser::SerializeTuple for CommandSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.seq())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a> ser::SerializeTupleStruct for CommandSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.seq())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

impl<'a> ser::SerializeTupleVariant for CommandSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.seq())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.done()
    }
}

macro_rules! serialize_single {
    ($($target:ident)*) => {paste!{$(
        fn [<serialize_ $target>](mut self, v: $target) -> Result<Self::Ok, Self::Error> {
            self.blob().[<serialize_ $target>](v)
        }
    )*}};
}

impl<'a> ser::Serializer for FlatArraySerializer<'a> {
    type Ok = ();
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
        self.blob().serialize_str(v)
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.blob().serialize_bytes(v)
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

impl<'a> ser::SerializeSeq for FlatArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for FlatArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for FlatArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for FlatArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.blob())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> ser::SerializeMap for FlatArraySerializer<'a> {
    type Ok = ();
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
        Ok(())
    }
}

impl<'a> ser::SerializeStruct for FlatArraySerializer<'a> {
    type Ok = ();
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
        Ok(())
    }
}

impl<'a> ser::SerializeStructVariant for FlatArraySerializer<'a> {
    type Ok = ();
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
        Ok(())
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

impl<'a> ser::Serializer for BlobSerializer<'a> {
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

    serialize_int!(i8 i16 i32 i64 u8 u16 u32 u64);
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
