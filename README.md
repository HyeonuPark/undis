Undis
=========

[<img alt="badge-github"    src="https://img.shields.io/badge/github.com-HyeonuPark/undis-green">](https://github.com/HyeonuPark/undis)
[<img alt="badge-crates.io" src="https://img.shields.io/crates/v/undis.svg">](https://crates.io/crates/undis)
[<img alt="badge-docs.rs"   src="https://docs.rs/undis/badge.svg">](https://docs.rs/undis)
[<img alt="badge-ci"        src="https://img.shields.io/github/workflow/status/HyeonuPark/undis/CI/main">](https://github.com/HyeonuPark/undis/actions?query=branch%3Amain)

Undis is a serde-compatible redis library for Rust.

## Sending a request

For most use cases the `Client` is the only thing you need to know.

```rust
use undis::Client;
use serde::{Serialize, Deserialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Table {
    foo: String,
    bar: i32,
    baz: bool,
}

let client = Client::new(20, addr).await?;

let value = Table { foo: "foo".into(), bar: 42, baz: true };
client.hset("my-key", &value).await?;
let fetched: Table = client.hmget("my-key").await?;

assert_eq!(value, fetched);
```

## Sending a custom request

You may want to send some requests which are not supported as a method.
This is possible using `raw_command`.

```rust
let res: MyStruct = client.raw_command(("CUSTOMCOMMAND", "ARG1", 42, "ARG2", "FOO")).await?;
```

## Future topics

- Pubsub.
- Pipeline.
- Separate the `resp3` module into its own crate, with better multi-crate project layout.
- Redis server mock helper.
- Full-featured Redis server implementation.
- Redis cluster proxy.

## License

Undis is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE-APACHE](https://github.com/HyeonuPark/undis/blob/main/LICENSE-APACHE)
and [LICENSE-MIT](https://github.com/HyeonuPark/undis/blob/main/LICENSE-MIT) for details.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
