use serde::{de, forward_to_deserialize_any};

/// Extract fields names from `T` if it's a `#[derive(Deserialize)]`-ed struct with named fields.
///
/// ```
/// # use undis::serde_helper::extract_struct_fields;
/// #[derive(serde::Deserialize)]
/// struct MyStruct {
///     foo: String,
///     bar: i32,
///     baz: bool,
/// }
/// assert_eq!(Some(&["foo", "bar", "baz"][..]), extract_struct_fields::<MyStruct>());
/// ```
pub fn extract_struct_fields<'de, T: de::Deserialize<'de>>() -> Option<&'static [&'static str]> {
    let mut res = None;

    let _ = T::deserialize(FakeDeserializer { out: &mut res });

    res
}

#[derive(Debug)]
struct FakeDeserializer<'a> {
    out: &'a mut Option<&'static [&'static str]>,
}

#[derive(Debug, thiserror::Error)]
#[error("as expected")]
struct NormalError;

impl de::Error for NormalError {
    fn custom<T>(_: T) -> Self
    where
        T: std::fmt::Display,
    {
        NormalError
    }
}

impl<'a, 'de> de::Deserializer<'de> for FakeDeserializer<'a> {
    type Error = NormalError;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        Err(NormalError)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct seq tuple
        tuple_struct map enum identifier ignored_any
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

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        *self.out = Some(fields);
        Err(NormalError)
    }
}
