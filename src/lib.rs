//! undis
//! =======
//!
//! Undis is a serde-compatible redis library for Rust.
//!
//! ## Making a query
//!
//! For most use cases the [`Client`](crate::Client) is the only thing you need to know.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")] async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use undis::Client;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Debug, PartialEq, Serialize, Deserialize)]
//! struct Table {
//!     foo: String,
//!     bar: i32,
//!     baz: bool,
//! }
//!
//! let client = Client::new(20, "localhost:6379").await?;
//!
//! let value = Table { foo: "foo".into(), bar: 42, baz: true };
//! client.hset("my-key", &value).await?;
//! let fetched: Table = client.hmget("my-key").await?;
//!
//! assert_eq!(value, fetched);
//! # Ok(()) }
//! ```
//!
//! ## Making a custom query
//!
//! You may want to call some query which is not supported as a method.
//! Please make a PR to this crate if you think it make sense,
//! but you can still call it without merge.
//!
//! ```no_run
//! # helper::with_client(|client| async move {
//! # #[derive(serde::Deserialize)] struct MyStruct;
//! let res: MyStruct = client.raw_command(("SOMECUSTOMCOMMAND", "ARG1", 42, "ARG2", "FOO")).await?;
//! # Ok(())})?; Ok::<(), helper::BoxError>(())
//! ```

// #![deny(missing_docs)]
#![deny(missing_debug_implementations)]

pub mod client;
pub mod connection;
pub mod connector;
pub mod resp3;
pub mod serde_helper;

pub use client::Client;
