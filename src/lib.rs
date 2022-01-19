//! undis
//! =======
//!
//! `undis` is a serde-compatible Redis library for Rust.
//!
//! ## Sending a request
//!
//! For most use cases the [`Client`](crate::Client) is the only thing you need to know.
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")] async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let addr = &std::env::var("REDIS_URL").unwrap_or_else(|_| std::process::exit(0));
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
//! let client = Client::new(20, addr).await?;
//!
//! let value = Table { foo: "foo".into(), bar: 42, baz: true };
//! client.hset("my-key", &value).await?;
//! let fetched: Table = client.hmget("my-key").await?;
//!
//! assert_eq!(value, fetched);
//! # Ok(()) }
//! ```
//!
//! ## Sending a custom request
//!
//! You may want to send some requests which are not supported as a method.
//! This is possible using [`raw_command`](Client::raw_command).
//!
//! ```no_run
//! # helper::with_client(|client| async move {
//! # #[derive(serde::Deserialize)] struct MyStruct;
//! let res: MyStruct = client.raw_command(("CUSTOMCOMMAND", "ARG1", 42, "ARG2", "FOO")).await?;
//! # Ok(())})?; Ok::<(), helper::BoxError>(())
//! ```

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

pub mod client;
pub mod command;
pub mod connection;
pub mod connector;
pub mod resp3;
pub mod serde_helper;

pub use client::Client;
