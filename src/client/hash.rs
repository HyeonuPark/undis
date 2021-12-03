use serde::{de::DeserializeOwned, Serialize};

use crate::connector::Connector;

use super::serde_helper::{get_struct_fields, EnsureMapLike, EnsureScalar, EnsureSequence};
use super::{Client, Error};

impl<T: Connector> Client<T> {
    pub async fn hdel<K, F>(&self, key: &K, fields: &F) -> Result<usize, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
    {
        self.raw_command(&("HDEL", EnsureScalar(key), EnsureSequence(fields)))
            .await
    }

    pub async fn hexists<K, F>(&self, key: &K, field: &F) -> Result<bool, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
    {
        self.raw_command(&("HEXISTS", EnsureScalar(key), EnsureScalar(field)))
            .await
    }

    pub async fn hget<K, F, R>(&self, key: &K, field: &F) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&("HGET", EnsureScalar(key), EnsureScalar(field)))
            .await
    }

    pub async fn hgetall<K, R>(&self, key: &K) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&("HGETALL", EnsureScalar(key))).await
    }

    pub async fn hincrby<K, F>(&self, key: &K, field: &F, increment: i64) -> Result<i64, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
    {
        self.raw_command(&("HINCRBY", EnsureScalar(key), EnsureScalar(field), increment))
            .await
    }

    pub async fn hincrbyfloat<K, F>(&self, key: &K, field: &F, increment: f64) -> Result<f64, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
    {
        self.raw_command(&(
            "HINCRBYFLOAT",
            EnsureScalar(key),
            EnsureScalar(field),
            increment,
        ))
        .await
    }

    pub async fn hkeys<K, R>(&self, key: &K) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&("HKEYS", EnsureScalar(key))).await
    }

    pub async fn hlen<K, R>(&self, key: &K) -> Result<usize, Error>
    where
        K: Serialize + ?Sized,
    {
        self.raw_command(&("HLEN", EnsureScalar(key))).await
    }

    pub async fn hmget<K, R>(&self, key: &K) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let fields = get_struct_fields::<R>().ok_or_else(|| {
            super::ErrorKind::Connection(
                crate::resp3::ser_cmd::Error::Custom(
                    "hmget_struct can only return struct with named fields \
                        with `#[derive(serde::Deserialize)]` attribute"
                        .into(),
                )
                .into(),
            )
        })?;
        self.raw_command(&("HMGET", EnsureScalar(key), fields))
            .await
    }

    pub async fn hrandfield<K, R>(&self, key: &K) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&("HRANDFIELD", EnsureScalar(key))).await
    }

    pub async fn hrandfield_count<K, R>(
        &self,
        key: &K,
        count: isize,
        withvalues: bool,
    ) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&(
            "HRANDFIELD",
            EnsureScalar(key),
            count,
            withvalues.then(|| "WITHVALUES"),
        ))
        .await
    }

    pub async fn hscan<K, R>(
        &self,
        key: &K,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, R), Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&(
            "HSCAN",
            EnsureScalar(key),
            cursor,
            match_pattern.map(|pattern| ("MATCH", pattern)),
            count.map(|count| ("COUNT", count)),
        ))
        .await
    }

    pub async fn hset<K, E>(&self, key: &K, entries: &E) -> Result<usize, Error>
    where
        K: Serialize + ?Sized,
        E: Serialize + ?Sized,
    {
        self.raw_command(&("HSET", EnsureScalar(key), EnsureMapLike(entries)))
            .await
    }

    pub async fn hsetnx<K, F, V>(&self, key: &K, field: &F, value: &V) -> Result<bool, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
        V: Serialize + ?Sized,
    {
        self.raw_command(&(
            "HSETNX",
            EnsureScalar(key),
            EnsureScalar(field),
            EnsureScalar(value),
        ))
        .await
    }

    pub async fn hstrlen<K, F, R>(&self, key: &K, field: &F) -> Result<usize, Error>
    where
        K: Serialize + ?Sized,
        F: Serialize + ?Sized,
    {
        self.raw_command(&("HSTRLEN", EnsureScalar(key), EnsureScalar(field)))
            .await
    }

    pub async fn hvals<K, R>(&self, key: &K) -> Result<R, Error>
    where
        K: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.raw_command(&("HVALS", EnsureScalar(key))).await
    }
}
