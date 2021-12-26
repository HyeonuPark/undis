Undis
=========

[<img alt="badge-github"    src="https://img.shields.io/badge/github.com-HyeonuPark/undis-green">](https://github.com/HyeonuPark/undis)
[<img alt="badge-crates.io" src="https://img.shields.io/crates/v/undis.svg">](https://crates.io/crates/undis)
[<img alt="badge-docs.rs"   src="https://docs.rs/undis/badge.svg">](https://docs.rs/undis)
[<img alt="badge-ci"        src="https://img.shields.io/github/workflow/status/HyeonuPark/undis/CI/main">](https://github.com/HyeonuPark/undis/actions?query=branch%3Amain)

Undis is a serde-compatible redis library for Rust.

## WIP

This project is currently under heavy development. Use it at your own risk.

## Todo

- Add `#[deny(missing_docs)]`.
- More command helper methods.
- Pubsub.
- Pipeline, and figure out how to share helper methods with the Client.

## Future considerations

- Separate the `resp3` module into its own crate, with better multi-crate project layout.
- Redis server mock helper.
- Full-featured Redis server implementation.
- Redis cluster proxy.

## License

Undis is distributed under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE-APACHE](https://github.com/HyeonuPark/undis/blob/main/LICENSE-APACHE) and [LICENSE-MIT](https://github.com/HyeonuPark/undis/blob/main/LICENSE-MIT) for details.
