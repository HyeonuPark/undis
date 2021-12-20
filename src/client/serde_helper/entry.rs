use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Entry<K, V>(pub K, pub V);

impl<K: Serialize, V: Serialize> Serialize for Entry<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(Some((&self.0, &self.1)))
    }
}

impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for Entry<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor<K, V>(PhantomData<(K, V)>);

        impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> serde::de::Visitor<'de> for Visitor<K, V> {
            type Value = Entry<K, V>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "a map of single entry")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                use serde::de::Error;

                let (k, v) = map
                    .next_entry()?
                    .ok_or_else(|| A::Error::custom("map is empty"))?;
                if map.next_key::<K>()?.is_some() {
                    return Err(A::Error::custom("map has more than one enntry"));
                }
                Ok(Entry(k, v))
            }
        }
        deserializer.deserialize_map(Visitor(PhantomData::<(K, V)>))
    }
}
