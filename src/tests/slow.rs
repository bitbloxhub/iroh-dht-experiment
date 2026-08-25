//! Lookup behaviour when some peers are slow or hung.
//!
//! [`NodeMode`] is set per id on the shared [`NodeModes`] map. Iterative lookup
//! only sheds a hung peer if [`Config::query_timeout`] is set.

use std::time::Duration;

use super::*;
use crate::{routing::K, rpc::Id};

const TINY: usize = 24;

async fn swarm(config: Config) -> (Vec<NodeId>, Nodes, NodeModes) {
    let ids = create_node_ids(&create_secrets(0, TINY));
    let (nodes, _, modes) = create_nodes_and_clients(&ids, next_n(0), config).await;
    init_routing_tables(&nodes, &ids, Some(0)).await.unwrap();
    (ids, nodes, modes)
}

fn other_closest(querier: NodeId, closest: &[NodeId]) -> NodeId {
    closest
        .iter()
        .copied()
        .find(|id| *id != querier)
        .expect("need a closest node other than the querier")
}

#[tokio::test(start_paused = true)]
async fn lookup_skips_hung_peer() {
    let timeout = Duration::from_millis(100);
    let (ids, nodes, modes) = swarm(Config::default().query_timeout(timeout)).await;
    let querier = nodes[0].0;
    let key = Id::blake3_hash(b"slow-lookup");
    let closest = expected_ids(&ids, key, K);
    let hung = other_closest(querier, &closest);
    modes.set(hung, NodeMode::Hung);

    let (_, api) = &nodes[0].1;
    let got = api.lookup(key, None).await.unwrap();
    assert!(
        !got.contains(&hung),
        "hung peer {hung} should not be in the lookup result"
    );
    assert!(!got.is_empty());

    let table = api.get_routing_table().await.unwrap();
    assert!(
        table.iter().all(|bucket| !bucket.contains(&hung)),
        "timed-out peer should be marked dead"
    );
}

#[tokio::test(start_paused = true)]
async fn lookup_includes_peer_within_query_timeout() {
    let timeout = Duration::from_millis(100);
    let (ids, nodes, modes) = swarm(Config::default().query_timeout(timeout)).await;
    let querier = nodes[0].0;
    let key = Id::blake3_hash(b"delayed-lookup");
    let closest = expected_ids(&ids, key, K);
    let slow = other_closest(querier, &closest);
    modes.set(slow, NodeMode::Slow(Duration::from_millis(10)));

    let (_, api) = &nodes[0].1;
    let got = api.lookup(key, None).await.unwrap();
    assert!(
        got.contains(&slow),
        "peer delayed by less than query_timeout should still be used"
    );
}

/// Iterative lookup sends `alpha` FindNodes, then waits for that whole batch
/// before starting the next. A slow peer in the first batch therefore delays
/// querying the 4th-closest. If that 4th-closest is hung, the hung timeout
/// stacks on top of the slow delay (serial) instead of overlapping (pipelined
/// alpha).
///
/// Virtual time: Slow(50ms) then Hung timeout(100ms) ≈ 150ms round-based,
/// ≈ 100ms if a free alpha slot queried the hung peer immediately.
#[tokio::test(start_paused = true)]
#[ignore = "lookup waits for the whole alpha round before querying further"]
async fn lookup_does_not_stack_slow_peer_behind_next_round() {
    let timeout = Duration::from_millis(100);
    let (ids, nodes, modes) = swarm(Config::default().query_timeout(timeout)).await;
    let key = Id::blake3_hash(b"round-stall");
    // Query from the farthest node so the globally closest ids are the ones
    // contacted first (self is not among them).
    let querier = *expected_ids(&ids, key, TINY).last().unwrap();
    let closest: Vec<NodeId> = expected_ids(&ids, key, K)
        .into_iter()
        .filter(|id| *id != querier)
        .collect();
    let slow = closest[2];
    let hung = closest[3];
    modes.set(slow, NodeMode::Slow(Duration::from_millis(50)));
    modes.set(hung, NodeMode::Hung);

    let (_, api) = nodes
        .iter()
        .find(|(id, _)| *id == querier)
        .unwrap()
        .1
        .clone();
    let start = tokio::time::Instant::now();
    api.lookup(key, None).await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(120),
        "slow peer stalled the next round: elapsed {elapsed:?} (round-based stacks ~50ms delay + 100ms hung timeout)"
    );
}

#[tokio::test(start_paused = true)]
async fn lookup_stream_does_not_stack_slow_peer_behind_next_round() {
    let timeout = Duration::from_millis(100);
    let (ids, nodes, modes) = swarm(Config::default().query_timeout(timeout)).await;
    let key = Id::blake3_hash(b"round-stall-stream");
    let querier = *expected_ids(&ids, key, TINY).last().unwrap();
    let closest: Vec<NodeId> = expected_ids(&ids, key, K)
        .into_iter()
        .filter(|id| *id != querier)
        .collect();
    let slow = closest[2];
    let hung = closest[3];
    modes.set(slow, NodeMode::Slow(Duration::from_millis(50)));
    modes.set(hung, NodeMode::Hung);

    let (_, api) = nodes
        .iter()
        .find(|(id, _)| *id == querier)
        .unwrap()
        .1
        .clone();
    let start = tokio::time::Instant::now();
    let mut rx = api.lookup_stream(key, None).await.unwrap();
    while let Ok(Some(_)) = rx.recv().await {}
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(120),
        "streaming lookup stacked rounds: elapsed {elapsed:?}"
    );
}
