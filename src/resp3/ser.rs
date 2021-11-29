use paste::paste;
use serde::ser;

use super::token::Token;

scoped_tls::scoped_thread_local!(pub static DEPTH_LIMIT: usize);

fn depth_limit() -> Option<usize> {
    DEPTH_LIMIT.is_set().then(|| DEPTH_LIMIT.with(usize::clone))
}

#[derive(Debug)]
pub struct ClientSerializer<'a> {
    buf: &'a mut Vec<u8>,
    depth: usize,
}

#[derive(Debug)]
pub struct ArraySerializer<'a> {
    ser: ClientSerializer<'a>,
    len: Option<usize>,
    count: usize,
    len_idx: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("custom: {0}")]
    Custom(String),
    #[error("sending NaN is not supported")]
    Nan,
    // TODO: remove this variant
    #[error("wow it failed")]
    Misc,
}

impl<'a> ClientSerializer<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> Self {
        ClientSerializer { buf, depth: 0 }
    }

    fn array(self, len: Option<usize>) -> Result<ArraySerializer<'a>, Error> {
        match depth_limit() {
            Some(limit) if limit <= self.depth => return Err(Error::Misc),
            _ => {}
        }

        let mut ser = ArraySerializer {
            len,
            count: 0,
            len_idx: self.buf.len() + 1, // skip prefix byte
            ser: ClientSerializer {
                buf: self.buf,
                depth: self.depth + 1,
            },
        };

        ser.ser.put(Token::Array(len));

        Ok(ser)
    }

    fn put(&mut self, token: Token<'_>) {
        token.put(&mut self.buf)
    }

    fn put_bytes(&mut self, bytes: &[u8]) {
        self.put(Token::Blob(Some(bytes)))
    }
}

impl<'a> ArraySerializer<'a> {
    fn child(&mut self) -> Result<ClientSerializer<'_>, Error> {
        self.count += 1;

        if self.len.map_or(false, |len| self.count > len) {
            // too many children
            return Err(Error::Misc);
        }

        Ok(ClientSerializer {
            buf: &mut self.ser.buf,
            depth: self.ser.depth,
        })
    }

    fn cleanup(mut self) -> Result<(), Error> {
        match self.len {
            Some(len) => {
                if len != self.count {
                    // invalid length specified
                    return Err(Error::Misc);
                }
            }
            None if self.count <= 9 => {
                // single digit count, overwrite the `?` with the length
                let digit = b'0' + self.count as u8;
                self.ser.buf[self.len_idx] = digit;
            }
            None => {
                self.ser.put(Token::StreamEnd);
            }
        }

        Ok(())
    }
}

macro_rules! serialize_int {
    ($($int:ident)*) => {paste!{$(
        fn [<serialize_ $int>](mut self, v: $int) -> Result<Self::Ok, Self::Error> {
            let mut buf = itoa::Buffer::new();
            self.put_bytes(buf.format(v).as_bytes());
            Ok(())
        }
    )*}};
}

macro_rules! serialize_float {
    ($($float:ident)*) => {paste!{$(
        fn [<serialize_ $float>](mut self, v: $float) -> Result<Self::Ok, Self::Error> {
            if v.is_nan() {
                return Err(Error::Nan);
            }
            let mut buf = ryu::Buffer::new();
            self.put_bytes(buf.format(v).as_bytes());
            Ok(())
        }
    )*}};
}

impl<'a> ser::Serializer for ClientSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ArraySerializer<'a>;
    type SerializeTuple = ArraySerializer<'a>;
    type SerializeTupleStruct = ArraySerializer<'a>;
    type SerializeTupleVariant = ArraySerializer<'a>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_bool(mut self, v: bool) -> Result<Self::Ok, Self::Error> {
        let msg = match v {
            true => b"1",
            false => b"0",
        };
        self.put_bytes(msg);
        Ok(())
    }

    serialize_int!(i8 i16 i32 i64 u8 u16 u32 u64);
    serialize_float!(f32 f64);

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        let mut buf = [0; 4];
        self.put_bytes(v.encode_utf8(&mut buf).as_bytes());
        Ok(())
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.put_bytes(v.as_bytes());
        Ok(())
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.put_bytes(v);
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
        self.put_bytes(b"");
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
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.array(None)?
            .serialize_newtype_variant(name, variant_index, variant, value)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.array(len)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.array(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.array(None)?
            .serialize_tuple_variant(name, variant_index, variant, len)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::Misc)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::Misc)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::Misc)
    }
}

impl<'a> ser::SerializeSeq for ArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.child()?)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.cleanup()
    }
}

impl<'a> ser::SerializeTuple for ArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.child()?)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.cleanup()
    }
}

impl<'a> ser::SerializeTupleStruct for ArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.child()?)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.cleanup()
    }
}

impl<'a> ser::SerializeTupleVariant for ArraySerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        value.serialize(self.child()?)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.cleanup()
    }
}

macro_rules! serialize_single {
    ($($target:ident)*) => {paste!{$(
        fn [<serialize_ $target>](mut self, v: $target) -> Result<Self::Ok, Self::Error> {
            self.child()?.[<serialize_ $target>](v)?;
            self.cleanup()
        }
    )*}};
}

impl<'a> ser::Serializer for ArraySerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    serialize_single!(bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char);

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.child()?.serialize_str(v)?;
        self.cleanup()
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.child()?.serialize_bytes(v)?;
        self.cleanup()
    }

    fn serialize_none(mut self) -> Result<Self::Ok, Self::Error> {
        self.child()?.serialize_none()?;
        self.cleanup()
    }

    fn serialize_some<T: ?Sized>(mut self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.child()?.serialize_some(value)?;
        self.cleanup()
    }

    fn serialize_unit(mut self) -> Result<Self::Ok, Self::Error> {
        self.child()?.serialize_unit()?;
        self.cleanup()
    }

    fn serialize_unit_struct(mut self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.child()?.serialize_unit_struct(name)?;
        self.cleanup()
    }

    fn serialize_unit_variant(
        mut self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.child()?
            .serialize_unit_variant(name, variant_index, variant)?;
        self.cleanup()
    }

    fn serialize_newtype_struct<T: ?Sized>(
        mut self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.child()?.serialize_newtype_struct(name, value)?;
        self.cleanup()
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
        self.child()?.serialize_str(variant)?;
        value.serialize(self)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        match (self.len, len) {
            (Some(len), Some(seq_len)) if len != seq_len => return Err(Error::Misc),
            _ => {}
        }

        Ok(self)
    }

    fn serialize_tuple(self, tuple_len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        if self.len.map_or(false, |len| len != tuple_len) {
            // length mismatch
            return Err(Error::Misc);
        }

        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(
        mut self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.child()?.serialize_str(variant)?;
        self.serialize_tuple(len)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(Error::Misc)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(Error::Misc)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(Error::Misc)
    }
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Custom(msg.to_string())
    }
}
