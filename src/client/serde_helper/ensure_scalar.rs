use paste::paste;
use serde::ser;

#[derive(Debug)]
pub struct EnsureScalar<T>(pub T);

#[derive(Debug)]
pub struct EnsureScalarSerializer<S>(S);

impl<T: ser::Serialize> ser::Serialize for EnsureScalar<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(EnsureScalarSerializer(serializer))
    }
}

macro_rules! forward_scalar_types {
    ($($target:ident)*) => {paste!{$(
        fn [<serialize_ $target>](self, v: $target) -> Result<Self::Ok, Self::Error> {
            self.0.[<serialize_ $target>](v)
        }
    )*}};
}

impl<S: ser::Serializer> ser::Serializer for EnsureScalarSerializer<S> {
    type Ok = S::Ok;
    type Error = S::Error;
    type SerializeSeq = ser::Impossible<S::Ok, S::Error>;
    type SerializeTuple = ser::Impossible<S::Ok, S::Error>;
    type SerializeTupleStruct = ser::Impossible<S::Ok, S::Error>;
    type SerializeTupleVariant = ser::Impossible<S::Ok, S::Error>;
    type SerializeMap = ser::Impossible<S::Ok, S::Error>;
    type SerializeStruct = ser::Impossible<S::Ok, S::Error>;
    type SerializeStructVariant = ser::Impossible<S::Ok, S::Error>;

    forward_scalar_types!(bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char);

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_str(v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_none()
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.0.serialize_some(value)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_struct(name)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.0.serialize_unit_variant(name, variant_index, variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        self.0.serialize_newtype_struct(name, value)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: serde::Serialize,
    {
        Err(ser::Error::custom(
            "variant types with fields are not supported",
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Err(ser::Error::custom("sequence types are not supported"))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(ser::Error::custom("tuple types are not supported"))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(ser::Error::custom("tuple types are not supported"))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(ser::Error::custom("tuple types are not supported"))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(ser::Error::custom("map types are not supported"))
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Err(ser::Error::custom("struct types are not supported"))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(ser::Error::custom("struct variant types are not supported"))
    }
}
