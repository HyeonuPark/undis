use serde::{de::DeserializeOwned, Serialize};

use crate::serde_helper::{extract_struct_fields, EnsureMapLike, EnsureScalar, EnsureSequence};

use super::{Command, RawCommand};

impl<T: RawCommand> Command<T> {
    /// <https://redis.io/commands/hdel>
    ///
    /// The `key` should be a scalar type, and the `fields` should be a sequence-like type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hdel_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: i32,
    /// }
    /// let res = client.hset(key, &Fields { field1: "foo".into(), field2: 42 }).await?;
    /// assert_eq!(2, res);
    /// let res = client.hdel(key, ("field1", "field2")).await?;
    /// assert_eq!(2, res);
    /// let res = client.hdel(key, "field1").await?;
    /// assert_eq!(0, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hdel<K, F>(&self, key: K, fields: F) -> Result<usize, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
    {
        self.raw_command(("HDEL", EnsureScalar(key), EnsureSequence(fields)))
            .await
    }

    /// <https://redis.io/commands/hexists>
    ///
    /// The `key` and the `field` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hexists_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    /// }
    /// let res = client.hset(key, &Fields { field1: "foo".into() }).await?;
    /// assert_eq!(1, res);
    /// let res = client.hexists(key, "field1").await?;
    /// assert!(res);
    /// let res = client.hexists(key, "field2").await?;
    /// assert!(!res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hexists<K, F>(&self, key: K, field: F) -> Result<bool, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
    {
        self.raw_command(("HEXISTS", EnsureScalar(key), EnsureScalar(field)))
            .await
    }

    /// <https://redis.io/commands/hget>
    ///
    /// The `key` and the `field` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hget_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    /// }
    /// let res = client.hset(key, &Fields { field1: "foo".into() }).await?;
    /// assert_eq!(1, res);
    /// let res: String = client.hget(key, "field1").await?;
    /// assert_eq!("foo", res);
    /// let res: Option<String> = client.hget(key, "field2").await?;
    /// assert!(res.is_none());
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hget<K, F, R>(&self, key: K, field: F) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("HGET", EnsureScalar(key), EnsureScalar(field)))
            .await
    }

    /// <https://redis.io/commands/hgetall>
    ///
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hgetall_1";
    /// # use std::collections::HashMap;
    /// #[derive(serde::Serialize, PartialEq)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: String,
    /// }
    /// let res = client.hset(key, &Fields { field1: "Hello".into(), field2: "World".into() }).await?;
    /// assert_eq!(2, res);
    /// let res: HashMap<String, String> = client.hgetall(key).await?;
    /// assert_eq!(2, res.len());
    /// assert_eq!("Hello", &res["field1"]);
    /// assert_eq!("World", &res["field2"]);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hgetall<K, R>(&self, key: K) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("HGETALL", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/hincrby>
    ///
    /// The `key` and the `field` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hincrby_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: i32,
    /// }
    /// let res = client.hset(key, &Fields { field1: 5 }).await?;
    /// assert_eq!(1, res);
    /// let res = client.hincrby(key, "field1", 1).await?;
    /// assert_eq!(6, res);
    /// let res = client.hincrby(key, "field1", -1).await?;
    /// assert_eq!(5, res);
    /// let res = client.hincrby(key, "field1", -10).await?;
    /// assert_eq!(-5, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hincrby<K, F>(&self, key: K, field: F, increment: i64) -> Result<i64, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
    {
        self.raw_command(("HINCRBY", EnsureScalar(key), EnsureScalar(field), increment))
            .await
    }

    /// <https://redis.io/commands/hincrbyfloat>
    ///
    /// The `key` and the `field` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hincrbyfloat_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field: f64,
    /// }
    /// let res = client.hset(key, &Fields { field: 10.50 }).await?;
    /// assert_eq!(1, res);
    /// let res = client.hincrbyfloat(key, "field", 0.1).await?;
    /// assert_eq!(10.6, res);
    /// let res = client.hincrbyfloat(key, "field", -5.0).await?;
    /// assert_eq!(5.6, res);
    /// let res = client.hset(key, &Fields { field: 5.0e3 }).await?;
    /// assert_eq!(0, res);
    /// let res = client.hincrbyfloat(key, "field", 2.0e2).await?;
    /// assert_eq!(5200.0, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hincrbyfloat<K, F>(
        &self,
        key: K,
        field: F,
        increment: f64,
    ) -> Result<f64, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
    {
        self.raw_command((
            "HINCRBYFLOAT",
            EnsureScalar(key),
            EnsureScalar(field),
            increment,
        ))
        .await
    }

    /// <https://redis.io/commands/hkeys>
    ///
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hkeys_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: String,
    /// }
    /// let res = client.hset(key, &Fields { field1: "Hello".into(), field2: "World".into() }).await?;
    /// assert_eq!(2, res);
    /// let res: Vec<String> = client.hkeys(key).await?;
    /// assert_eq!(&["field1".to_owned(), "field2".to_owned()][..], &res[..]);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hkeys<K, R>(&self, key: K) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("HKEYS", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/hlen>
    ///
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hlen_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: String,
    /// }
    /// let res = client.hset(key, &Fields { field1: "Hello".into(), field2: "World".into() }).await?;
    /// assert_eq!(2, res);
    /// let res = client.hlen(key).await?;
    /// assert_eq!(2, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hlen<K>(&self, key: K) -> Result<usize, T::Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("HLEN", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/hmget>
    ///
    /// The `key` should be scalar type and the returned `R` should be struct type
    /// with `#[derive(serde::Deserialize)]`-ed struct or manual implementation with similar behavior.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hmget_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: String,
    /// }
    /// #[derive(serde::Deserialize, PartialEq, Debug)]
    /// struct Query {
    ///     field1: String,
    ///     field2: String,
    ///     nofield: Option<i32>,
    /// }
    /// let res = client.hset(key, &Fields { field1: "Hello".into(), field2: "World".into() }).await?;
    /// assert_eq!(2, res);
    /// for _ in 0..10 {
    ///     let res: Query = client.hmget(key).await?;
    ///     assert_eq!(Query { field1: "Hello".into(), field2: "World".into(), nofield: None }, res);
    /// }
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hmget<K, R>(&self, key: K) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        let fields = extract_struct_fields::<R>().ok_or_else(|| {
            super::Error::from(crate::resp3::ser_cmd::Error::Custom(
                "hmget_struct can only return struct with named fields \
                        with `#[derive(serde::Deserialize)]` attribute"
                    .into(),
            ))
        })?;
        self.raw_command(("HMGET", EnsureScalar(key), fields)).await
    }

    /// <https://redis.io/commands/hrandfield>
    ///
    /// `HRANDFIELD` with just the key argument.
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hrandfield_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     head: String,
    ///     tails: String,
    ///     edge: String,
    /// }
    /// let res = client.hset(key, &Fields { head: "obverse".into(), tails: "reverse".into(), edge: "null".into() }).await?;
    /// assert_eq!(3, res);
    /// let res: String = client.hrandfield(key).await?;
    /// assert!(["head".to_owned(), "tails".to_owned(), "edge".to_owned()].contains(&res), "res: {}", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hrandfield<K, R>(&self, key: K) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("HRANDFIELD", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/hrandfield>
    ///
    /// `HRANDFIELD` with count and optional `WITHVALUES` specifier.
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hrandfield_2";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     head: String,
    /// }
    /// let res = client.hset(key, &Fields { head: "obverse".into() }).await?;
    /// assert_eq!(1, res);
    /// let res: Vec<String> = client.hrandfield_count(key, 3).await?;
    /// assert_eq!(vec!["head".to_owned()], res);
    /// let res: Vec<String> = client.hrandfield_count(key, -3).await?;
    /// assert_eq!(vec!["head".to_owned(), "head".to_owned(), "head".to_owned()], res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hrandfield_count<K, R>(&self, key: K, count: isize) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("HRANDFIELD", EnsureScalar(key), count))
            .await
    }

    /// <https://redis.io/commands/hscan>
    ///
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hscan_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     name: String,
    ///     age: i32,
    /// }
    /// let res = client.hset(key, &Fields { name: "Jack".into(), age: 33 }).await?;
    /// assert_eq!(2, res);
    /// let (cursor, res): (u64, Vec<String>) = client.hscan(key, 0, None, None).await?;
    /// assert_eq!(0, cursor);
    /// assert_eq!(vec!["name".to_owned(), "Jack".to_owned(), "age".to_owned(), "33".to_owned()], res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hscan<K, R>(
        &self,
        key: K,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, R), T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command((
            "HSCAN",
            EnsureScalar(key),
            cursor,
            match_pattern.map(|pattern| ("MATCH", pattern)),
            count.map(|count| ("COUNT", count)),
        ))
        .await
    }

    /// <https://redis.io/commands/hset>
    ///
    /// The `key` should be a scalar type, and the `entries` should be a map-like type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hset_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: i32,
    /// }
    /// let res = client.hset(key, &Fields { field1: "foo".into(), field2: 42 }).await?;
    /// assert_eq!(2, res);
    /// let res: String = client.hget(key, "field1").await?;
    /// assert_eq!("foo", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hset<K, E>(&self, key: K, entries: E) -> Result<usize, T::Error>
    where
        K: Serialize + Send,
        E: Serialize + Send,
    {
        self.raw_command(("HSET", EnsureScalar(key), EnsureMapLike(entries)))
            .await
    }

    /// <https://redis.io/commands/hsetnx>
    ///
    /// The `key`, `field` and the `value` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hsetnx_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     field: f64,
    /// }
    /// let res = client.hsetnx(key, "field", "Hello").await?;
    /// assert!(res);
    /// let res = client.hsetnx(key, "field", "World").await?;
    /// assert!(!res);
    /// let res: String = client.hget(key, "field").await?;
    /// assert_eq!("Hello", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hsetnx<K, F, V>(&self, key: K, field: F, value: V) -> Result<bool, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
        V: Serialize + Send,
    {
        self.raw_command((
            "HSETNX",
            EnsureScalar(key),
            EnsureScalar(field),
            EnsureScalar(value),
        ))
        .await
    }

    /// <https://redis.io/commands/hstrlen>
    ///
    /// The `key` and the `field` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hstrlen_1";
    /// #[derive(serde::Serialize)]
    /// struct Fields {
    ///     f1: String,
    ///     f2: usize,
    ///     f3: i32,
    /// }
    /// let res = client.hset(key, &Fields { f1: "HelloWorld".into(), f2: 99, f3: -256 }).await?;
    /// assert_eq!(3, res);
    /// let res = client.hstrlen(key, "f1").await?;
    /// assert_eq!(10, res);
    /// let res = client.hstrlen(key, "f2").await?;
    /// assert_eq!(2, res);
    /// let res = client.hstrlen(key, "f3").await?;
    /// assert_eq!(4, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hstrlen<K, F>(&self, key: K, field: F) -> Result<usize, T::Error>
    where
        K: Serialize + Send,
        F: Serialize + Send,
    {
        self.raw_command(("HSTRLEN", EnsureScalar(key), EnsureScalar(field)))
            .await
    }

    /// <https://redis.io/commands/hvals>
    ///
    /// The `key` should be scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_hash_hvals_1";
    /// # use std::collections::HashMap;
    /// #[derive(serde::Serialize, PartialEq)]
    /// struct Fields {
    ///     field1: String,
    ///     field2: String,
    /// }
    /// let res = client.hset(key, &Fields { field1: "Hello".into(), field2: "World".into() }).await?;
    /// assert_eq!(2, res);
    /// let res: Vec<String> = client.hvals(key).await?;
    /// assert_eq!(vec!["Hello".to_owned(), "World".to_owned()], res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn hvals<K, R>(&self, key: K) -> Result<R, T::Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("HVALS", EnsureScalar(key))).await
    }
}
