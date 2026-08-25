# iroh DHT experiment

## Tests

```sh
cargo test
```

These are small in-memory protocol tests (plus one tiny iroh swarm) with real
assertions.

## Visualizations

Swarm plots and gifs live in `src/tests/viz.rs`. They are ignored by default;
they print stats and write files under `img/` rather than asserting DHT
behaviour.

```sh
cargo test --lib viz -- --ignored --nocapture
```

Run a single scenario:

```sh
cargo test --lib viz::perfect_routing_tables_1k -- --ignored --nocapture
```

Scenarios:

- just_bootstrap_1k
- perfect_routing_tables_1k
- perfect_routing_tables_10k
- no_routing_1k
- self_and_random_lookup_strategy
- self_lookup_strategy
- random_lookup_strategy
- remove_1k
- partition_1k
- random_vs_blended_1k
- iroh_perfect_routing_tables_500

`--release` helps for the 1k-node runs. `perfect_routing_tables_100k` and
`iroh_perfect_routing_tables_10k` are extra-heavy; skip them unless you mean it:

```sh
cargo test --lib viz -- --ignored --nocapture --skip 100k --skip 10k
```

## License

Copyright 2025 N0, INC.

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
