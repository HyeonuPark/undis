use serde::{de::DeserializeOwned, Serialize};

use crate::connection::Error;
use crate::serde_helper::{EnsureMapLike, EnsureScalar, EnsureSequence};

use super::{CommandHelper, OkResp, RawCommand};

impl<T: RawCommand> CommandHelper<T> {
    /// <https://redis.io/commands/append>
    ///
    /// `key` and `value` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_base_append_1";
    /// let res = client.exists_one(key).await?;
    /// assert!(!res);
    /// let res = client.append(key, "Hello").await?;
    /// assert_eq!(5, res);
    /// let res = client.append(key, " World").await?;
    /// assert_eq!(11, res);
    /// let res: String = client.get(key).await?;
    /// assert_eq!("Hello World", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn append<K, V>(&self, key: K, value: V) -> Result<usize, Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        self.raw_command(("APPEND", EnsureScalar(key), EnsureScalar(value)))
            .await
    }

    /// <https://redis.io/commands/decr>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_decr_1";
    /// client.set(key, "10").await?;
    /// let res = client.decr(key).await?;
    /// assert_eq!(9, res);
    /// client.set(key, "234293482390480948029348230948").await?;
    /// let res = client.decr(key).await;
    /// assert!(res.is_err());
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn decr<K>(&self, key: K) -> Result<i64, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("DECR", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/decrby>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_decrby_1";
    /// client.set(key, "10").await?;
    /// let res = client.decrby(key, 3).await?;
    /// assert_eq!(7, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn decrby<K>(&self, key: K, decrement: i64) -> Result<i64, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("DECRBY", EnsureScalar(key), decrement))
            .await
    }

    /// <https://redis.io/commands/get>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_get_1";
    /// let res: Option<i32> = client.get("nonexisting").await?;
    /// assert!(res.is_none());
    /// client.set(key, "Hello").await?;
    /// let res: String = client.get(key).await?;
    /// assert_eq!("Hello", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn get<K, R>(&self, key: K) -> Result<R, Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("GET", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/getrange>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_getrange_1";
    /// client.set(key, "This is a string").await?;
    /// let res: String = client.getrange(key, 0, 3).await?;
    /// assert_eq!("This", res);
    /// let res: String = client.getrange(key, -3, -1).await?;
    /// assert_eq!("ing", res);
    /// let res: String = client.getrange(key, 0, -1).await?;
    /// assert_eq!("This is a string", res);
    /// let res: String = client.getrange(key, 10, 100).await?;
    /// assert_eq!("string", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn getrange<K, R>(&self, key: K, start: isize, end: isize) -> Result<R, Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("GETRANGE", EnsureScalar(key), start, end))
            .await
    }

    /// <https://redis.io/commands/incr>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_incr_1";
    /// client.set(key, "10").await?;
    /// let res = client.incr(key).await?;
    /// assert_eq!(11, res);
    /// let res: i64 = client.get(key).await?;
    /// assert_eq!(11, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn incr<K>(&self, key: K) -> Result<i64, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("INCR", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/incrby>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_incrby_1";
    /// client.set(key, "10").await?;
    /// let res = client.incrby(key, 5).await?;
    /// assert_eq!(15, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn incrby<K>(&self, key: K, increment: i64) -> Result<i64, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("INCRBY", EnsureScalar(key), increment))
            .await
    }

    /// <https://redis.io/commands/incrbyfloat>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_incrbyfloat_1";
    /// client.set(key, 10.50f64).await?;
    /// let res = client.incrbyfloat(key, 0.1).await?;
    /// assert_eq!(10.6, res);
    /// let res = client.incrbyfloat(key, -5.0).await?;
    /// assert_eq!(5.6, res);
    /// client.set(key, "5.0e3").await?;
    /// let res = client.incrbyfloat(key, 200.0).await?;
    /// assert_eq!(5200.0, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn incrbyfloat<K>(&self, key: K, increment: f64) -> Result<f64, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("INCRBYFLOAT", EnsureScalar(key), increment))
            .await
    }

    /// <https://redis.io/commands/mget>
    ///
    /// `keys` should be a sequence type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key1 = "doctest_client_string_mget_1";
    /// # let key2 = "doctest_client_string_mget_2";
    /// client.set(key1, "Hello").await?;
    /// client.set(key2, "World").await?;
    /// let res: Vec<Option<String>> = client.mget([key1, key2, "nonexisting"]).await?;
    /// assert_eq!(vec![
    ///     Some("Hello".to_owned()),
    ///     Some("World".to_owned()),
    ///     None,
    /// ], res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn mget<K, R>(&self, keys: K) -> Result<R, Error>
    where
        K: Serialize + Send,
        R: DeserializeOwned,
    {
        self.raw_command(("MGET", EnsureSequence(keys))).await
    }

    /// <https://redis.io/commands/mset>
    ///
    /// `entries` should be a map-like type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # use std::collections::HashMap;
    /// # let key1 = "doctest_client_string_mset_1";
    /// # let key2 = "doctest_client_string_mset_2";
    /// let mut map = HashMap::new();
    /// map.insert(key1, "Hello");
    /// map.insert(key2, "World");
    /// client.mset(&map).await?;
    /// let res: String = client.get(key1).await?;
    /// assert_eq!("Hello", res);
    /// let res: String = client.get(key2).await?;
    /// assert_eq!("World", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn mset<E>(&self, entries: E) -> Result<(), Error>
    where
        E: Serialize + Send,
    {
        let _: super::OkResp = self.raw_command(("MSET", EnsureMapLike(entries))).await?;
        Ok(())
    }

    /// <https://redis.io/commands/set>
    ///
    /// `key` and `value` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_set_1";
    /// client.set(key, "Hello").await?;
    /// let res: String = client.get(key).await?;
    /// assert_eq!("Hello", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn set<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        let _: OkResp = self
            .raw_command(("SET", EnsureScalar(key), EnsureScalar(value)))
            .await?;
        Ok(())
    }

    /// <https://redis.io/commands/setrange>
    ///
    /// `key` and `value` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key1 = "doctest_client_string_setrange_1";
    /// # let key2 = "doctest_client_string_setrange_2";
    /// client.set(key1, "Hello World").await?;
    /// let res = client.setrange(key1, 6, "Redis").await?;
    /// assert_eq!(11, res);
    /// let res: String = client.get(key1).await?;
    /// assert_eq!("Hello Redis", res);
    ///
    /// let res = client.setrange(key2, 6, "Redis").await?;
    /// assert_eq!(11, res);
    /// let res: String = client.get(key2).await?;
    /// assert_eq!("\0\0\0\0\0\0Redis", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn setrange<K, V>(&self, key: K, offset: usize, value: V) -> Result<usize, Error>
    where
        K: Serialize + Send,
        V: Serialize + Send,
    {
        self.raw_command(("SETRANGE", EnsureScalar(key), offset, EnsureScalar(value)))
            .await
    }

    /// <https://redis.io/commands/strlen>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_string_strlen_1";
    /// client.set(key, "Hello world").await?;
    /// let res = client.strlen(key).await?;
    /// assert_eq!(11, res);
    /// let res = client.strlen("nonexisting").await?;
    /// assert_eq!(0, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn strlen<K>(&self, key: K) -> Result<usize, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("STRLEN", EnsureScalar(key))).await
    }
}
