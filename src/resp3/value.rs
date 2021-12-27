//! The loosely typed Value enum to represent RESP3 value.
//!
//! For more information, see the [`Value`](self::Value) type.

use std::cmp::{Eq, Ord, PartialEq, PartialOrd};
use std::fmt;
use std::hash::Hash;

use bstr::BString;
use indexmap::IndexMap;
use serde::de;

// TODO: impl Serialize/Deserializer for Value

/// Represents any valid RESP3 value.
///
/// This is useful to represent some flexible message like `HELLO` response
/// or to _see_ the structure of some [`.raw_command()`](crate::Client::raw_command) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Null value.
    Null,
    /// Binary string. Can be obtained from simple string, blog string, blog string stream.
    /// It conventionally but not necessarily is a UTF-8 encoded string.
    Blob(BString),
    /// Boolean value.
    Boolean(bool),
    /// Integer value in the form of i128.
    /// This type doesn't supports numbers which can't be represented within this range
    /// though the RESP3 protocol itself supports arbitrary big integers.
    Number(i128),
    /// Double precision floating point number which can't be NaN.
    Double(Double),
    /// Array of values.
    Array(Vec<Value>),
    /// Map of values, keyed by binary strings.
    /// Order is preserved to print hello message nicely.
    Map(IndexMap<BString, Value>),
}

/// Double precision floating point number which can't be NaN.
#[derive(Debug, Clone, Copy, Default)]
pub struct Double(f64);

impl Double {
    /// Construct `Double` from the `f64`.
    ///
    /// # Panic
    ///
    /// Panics if the `num` is NaN.
    pub fn new(num: f64) -> Self {
        assert!(!num.is_nan(), "RESP3 Double can't be NaN");
        Double(num)
    }

    /// Get the underlying `f64` value.
    pub fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Double {}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Hash for Double {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.0 == 0.0 {
            // -0.0 == 0.0
            0.0f64.to_bits().hash(state);
        } else {
            self.0.to_bits().hash(state)
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl<'de> de::Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = Value;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any valid RESP3 value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Boolean(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Number(v.into()))
            }

            fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Number(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Number(v.into()))
            }

            fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Number(v.try_into().map_err(|_| {
                    E::custom("number cannot be represented within the i128 range")
                })?))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if v.is_nan() {
                    return Err(E::custom("NaN is not allowed"));
                }

                Ok(Value::Double(Double::new(v)))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Blob(v.into()))
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Blob(v.into()))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Blob(v.into()))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Blob(v.into()))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Value::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                de::Deserialize::deserialize(deserializer)
            }

            fn visit_seq<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut vec = Vec::new();

                while let Some(elem) = access.next_element()? {
                    vec.push(elem);
                }

                Ok(Value::Array(vec))
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut map = IndexMap::new();

                while let Some((key, value)) = access.next_entry()? {
                    map.insert(key, value);
                }

                Ok(Value::Map(map))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}
