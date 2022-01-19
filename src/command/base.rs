use std::time::{Duration, SystemTime};

use serde::{de::DeserializeOwned, Serialize};

use crate::connection::Error;
use crate::serde_helper::{EnsureScalar, EnsureSequence};

use super::{CommandHelper, RawCommand};

impl<T: RawCommand> CommandHelper<T> {
    /// <https://redis.io/commands/copy>
    ///
    /// `source` and the `destination` should be scalar types.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key1 = "doctest_client_base_copy_1";
    /// # let key2 = "doctest_client_base_copy_2";
    /// client.set(key1, "sheep").await?;
    /// let res = client.copy(key1, key2, None, false).await?;
    /// assert!(res);
    /// let res: String = client.get(key2).await?;
    /// assert_eq!("sheep", res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn copy<S, D>(
        &self,
        source: S,
        destination: D,
        db: Option<u32>,
        replace: bool,
    ) -> Result<bool, Error>
    where
        S: Serialize + Send,
        D: Serialize + Send,
    {
        self.raw_command((
            "COPY",
            EnsureScalar(source),
            EnsureScalar(destination),
            db.map(|index| ("DB", index)),
            replace.then(|| "REPLACE"),
        ))
        .await
    }

    /// <https://redis.io/commands/del>
    ///
    /// `keys` should be a sequence type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key1 = "doctest_client_base_del_1";
    /// # let key2 = "doctest_client_base_del_2";
    /// client.set(key1, "Hello").await?;
    /// client.set(key2, "World").await?;
    /// let res = client.del([key1, key2]).await?;
    /// assert_eq!(2, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn del<K>(&self, keys: K) -> Result<usize, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("DEL", EnsureSequence(keys))).await
    }

    /// <https://redis.io/commands/exists>
    ///
    /// `keys` should be a sequence type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key1 = "doctest_client_base_exists_1";
    /// # let key2 = "doctest_client_base_exists_2";
    /// client.set(key1, "Hello").await?;
    /// let res = client.exists(key1).await?;
    /// assert_eq!(1, res);
    /// let res = client.exists("nosuchkey").await?;
    /// assert_eq!(0, res);
    /// client.set(key2, "World").await?;
    /// let res = client.exists([key1, key2, "nosuchkey"]).await?;
    /// assert_eq!(2, res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn exists<K>(&self, keys: K) -> Result<usize, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("EXISTS", EnsureSequence(keys))).await
    }

    /// Same as [`.exists()`](CommandHelper::exists),
    /// but takes single key and returns `bool`.
    ///
    /// `key` should be a scalar type.
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key = "doctest_client_base_exists_3";
    /// let res = client.exists_one(key).await?;
    /// assert!(!res);
    /// client.set(key, "Hello").await?;
    /// let res = client.exists_one(key).await?;
    /// assert!(res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn exists_one<K>(&self, key: K) -> Result<bool, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("EXISTS", EnsureScalar(key))).await
    }

    /// <https://redis.io/commands/pexpire>
    ///
    /// Despite the name, this method uses `PEXPIRE`, not `EXPIRE`, for millisecond-level accuracy.
    ///
    /// `keys` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # use std::time::Duration;
    /// # let key = "doctest_client_base_expire_1";
    /// client.set(key, "Hello").await?;
    /// let res = client.expire(key, Duration::from_secs(10)).await?;
    /// assert!(res);
    /// let res = client.ttl(key).await?;
    /// assert!(helper::mostly_eq(res.unwrap(), Duration::from_secs(10)));
    /// client.set(key, "Hello World").await?;
    ///
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn expire<K>(&self, key: K, timeout: Duration) -> Result<bool, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command(("PEXPIRE", EnsureScalar(key), timeout.as_millis() as u64))
            .await
    }

    /// <https://redis.io/commands/pexpireat>
    ///
    /// Despite the name, this method uses `PEXPIREAT`, not `EXPIREAT`, for millisecond-level accuracy.
    ///
    /// `keys` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # use std::time::SystemTime;
    /// # let key = "doctest_client_base_expireat_1";
    /// client.set(key, "Hello").await?;
    /// let res = client.exists_one(key).await?;
    /// assert!(res);
    /// let res = client.expireat(key, SystemTime::UNIX_EPOCH).await?;
    /// assert!(res);
    /// let res = client.exists_one(key).await?;
    /// assert!(!res);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn expireat<K>(&self, key: K, timestamp: SystemTime) -> Result<bool, Error>
    where
        K: Serialize + Send,
    {
        self.raw_command((
            "PEXPIREAT",
            EnsureScalar(key),
            timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ))
        .await
    }

    /// <https://redis.io/commands/pttl>
    ///
    /// Despite the name, this method uses `PTTL`, not `TTL`, for millisecond-level accuracy.
    ///
    /// `keys` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # use std::time::Duration;
    /// # let key = "doctest_client_base_ttl_1";
    /// client.set(key, "Hello").await?;
    /// let res = client.expire(key, Duration::from_secs(10)).await?;
    /// assert!(res);
    /// let res = client.ttl(key).await?;
    /// assert!(helper::mostly_eq(res.unwrap(), Duration::from_secs(10)));
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn ttl<K>(&self, key: K) -> Result<Option<Duration>, Error>
    where
        K: Serialize + Send,
    {
        Ok(timestamp(
            self.raw_command(("PTTL", EnsureScalar(key))).await?,
        ))
    }

    /// <https://redis.io/commands/scan>
    ///
    /// `key` should be a scalar type.
    ///
    /// ```
    /// # helper::with_client(|client| async move {
    /// # let key1 = "doctest_client_base_scan_1";
    /// # let key2 = "doctest_client_base_scan_2";
    /// # let key3 = "doctest_client_base_scan_3";
    /// # let pattern = "doctest_client_base_scan_*";
    /// client.set(key1, "value1").await?;
    /// client.set(key2, "value2").await?;
    /// client.set(key3, "value3").await?;
    /// let mut cursor = 0;
    /// let mut output = vec![];
    /// loop {
    ///     let (next, res): (u64, Vec<String>) = client.scan(cursor, Some(pattern), None).await?;
    ///     output.extend(res);
    ///     cursor = next;
    ///     if next == 0 {
    ///         break;
    ///     }
    /// }
    /// output.sort();
    /// assert_eq!(vec![key1.to_owned(), key2.into(), key3.into()], output);
    /// # Ok(())})?; Ok::<(), helper::BoxError>(())
    pub async fn scan<R>(
        &self,
        cursor: u64,
        match_pattern: Option<&str>,
        count: Option<usize>,
    ) -> Result<(u64, R), Error>
    where
        R: DeserializeOwned,
    {
        self.raw_command((
            "SCAN",
            cursor,
            match_pattern.map(|pattern| ("MATCH", pattern)),
            count.map(|count| ("COUNT", count)),
        ))
        .await
    }
}

fn timestamp(num: i64) -> Option<Duration> {
    num.try_into().ok().map(Duration::from_millis)
}
