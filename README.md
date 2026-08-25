# iroh DHT experiment

Experimental [Kademlia] DHT over [iroh] connections, with a 32-byte keyspace
so [BLAKE3] hashes and Ed25519 public keys fit without folding.

It follows mainline ([BEP-5], [BEP-44]) in spirit, with a few intentional
deviations:

- **No ping.** Liveness is a `FindNode` (a random id if you only care that they
  answer). Same round-trip, and you get contacts.
- **No routing-table babysitting.** The table is only updated as a side effect
  of `FindNode`: success → `nodes_seen`, timeout/fail → `nodes_dead`. Periodic
  random (and optional self/candidate) lookups are that traffic on a timer.
  There is no LRU, no ping-oldest, no per-bucket refresh.
- **Full k-buckets reject newcomers.** Favour stability. A stored node that
  dies is dropped the next time a probe hits it, which frees a slot. Do not
  evict a live contact for a stranger you have not queried.
- **Records are ephemeral.** Storage is in-memory and may drop values at any
  time. Republish is required; DHT nodes come and go.
- **`Kind` is the namespace.** `Value` / `Kind` are a closed enum (providers,
  signed messages, small immutable blobs). Variant order is the wire
  discriminator: only append, keep the two enums in lockstep, unknown kind
  fails to decode. No plugin traits.
- **Lookup is round-based** (`lookup`): up to `alpha` FindNodes, wait for that
  whole batch, then maybe another. A round may include ids that are not closer
  than the current k-th success (leftover `alpha` slots): FindNode is the
  liveness test, so those probes hedge against a closer candidate being dead.
  `lookup_stream` uses the same launch rule but pipelines `alpha` so a slow
  peer does not block starting the next FindNode. Both return only the final
  k-closest set.

Routing is XOR-only and does not see `Kind`. Set may refuse a key that is
far from the node (`ErrDistance`).

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

[Kademlia]: https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
[iroh]: https://docs.rs/iroh/latest/iroh/
[BLAKE3]: https://docs.rs/blake3/latest/blake3/
[BEP-5]: https://www.bittorrent.org/beps/bep_0005.html
[BEP-44]: https://www.bittorrent.org/beps/bep_0044.html
