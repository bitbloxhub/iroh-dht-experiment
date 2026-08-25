//! Standalone comparison of round-based [`ApiClient::lookup`] vs pipelined
//! [`ApiClient::lookup_stream`]: recall vs the globally closest ids, and how
//! many RPCs each walk spends.
//!
//! These are not visualizations. Thresholds are calibrated on seed 0.

use std::collections::HashSet;

use rand::{Rng, seq::SliceRandom};

use super::*;
use crate::{routing::K, rpc::Id};

const N: usize = 128;
const KEYS: usize = 40;
const SEED: u64 = 0;
/// Ring bootstrap width. Small on purpose so a cold lookup cannot see everyone.
const BOOTSTRAP: usize = 3;

async fn collect_stream(api: &ApiClient, key: Id) -> Vec<NodeId> {
    let mut rx = api.lookup_stream(key, None).await.unwrap();
    let mut out = Vec::new();
    while let Ok(Some(id)) = rx.recv().await {
        out.push(id);
    }
    out
}

fn overlap(got: &[NodeId], expected: &[NodeId]) -> usize {
    got.iter().filter(|id| expected.contains(id)).count()
}

struct Score {
    recall: f64,
    dials: f64,
}

async fn score(
    nodes: &Nodes,
    _ids: &[NodeId],
    modes: &NodeModes,
    expected: impl Fn(&Id) -> Vec<NodeId>,
    stream: bool,
) -> Score {
    // One lookup per querier so earlier walks do not fill later tables.
    let mut rng = rng(SEED);
    let mut recall = 0usize;
    let mut dials = 0usize;
    for i in 0..KEYS {
        let querier = &nodes[i].1.1;
        let key = Id::from(rng.r#gen::<[u8; 32]>());
        let want = expected(&key);
        let before = modes.dials();
        let got = if stream {
            collect_stream(querier, key).await
        } else {
            querier.lookup(key, None).await.unwrap()
        };
        dials += modes.dials() - before;
        recall += overlap(&got, &want);
    }
    Score {
        recall: recall as f64 / KEYS as f64,
        dials: dials as f64 / KEYS as f64,
    }
}

async fn two_swarms(n_bootstrap: usize, config: Config) -> [(Nodes, NodeModes); 2] {
    let ids = create_node_ids(&create_secrets(SEED, N));
    let mut out = Vec::with_capacity(2);
    for _ in 0..2 {
        let (nodes, _, modes) =
            create_nodes_and_clients(&ids, next_n(n_bootstrap), config.clone()).await;
        out.push((nodes, modes));
    }
    out.try_into().unwrap()
}

fn report(label: &str, round: &Score, stream: &Score) {
    println!(
        "{label}: round recall={:.2} dials={:.1} | stream recall={:.2} dials={:.1}",
        round.recall, round.dials, stream.recall, stream.dials
    );
}

#[tokio::test]
async fn perfect_tables_both_find_k_closest() {
    let ids = create_node_ids(&create_secrets(SEED, N));
    let [(nodes_a, modes_a), (nodes_b, modes_b)] = two_swarms(0, Config::default()).await;
    init_routing_tables(&nodes_a, &ids, Some(SEED))
        .await
        .unwrap();
    init_routing_tables(&nodes_b, &ids, Some(SEED))
        .await
        .unwrap();
    let expected = |key: &Id| expected_ids(&ids, *key, K);
    let round = score(&nodes_a, &ids, &modes_a, expected, false).await;
    let stream = score(&nodes_b, &ids, &modes_b, expected, true).await;
    report("perfect", &round, &stream);
    assert_eq!(round.recall, K as f64);
    assert_eq!(stream.recall, K as f64);
}

#[tokio::test]
async fn bootstrap_only_recall_beats_chance() {
    // Chance overlap if we picked k random ids: k^2 / n ≈ 6.25.
    let ids = create_node_ids(&create_secrets(SEED, N));
    let [(nodes_a, modes_a), (nodes_b, modes_b)] = two_swarms(BOOTSTRAP, Config::default()).await;
    let expected = |key: &Id| expected_ids(&ids, *key, K);
    let round = score(&nodes_a, &ids, &modes_a, expected, false).await;
    let stream = score(&nodes_b, &ids, &modes_b, expected, true).await;
    report("bootstrap-only", &round, &stream);
    // Chance ≈ k²/n ≈ 3.1. Both walks should beat that; they need not match.
    assert!(
        round.recall >= 5.0,
        "round-based recall {} too close to chance",
        round.recall
    );
    assert!(
        stream.recall >= 5.0,
        "stream recall {} too close to chance",
        stream.recall
    );
}

#[tokio::test(start_paused = true)]
async fn bootstrap_only_with_hung_peers_still_finds_live_closest() {
    let timeout = Duration::from_millis(50);
    let ids = create_node_ids(&create_secrets(SEED, N));
    let [(nodes_a, modes_a), (nodes_b, modes_b)] =
        two_swarms(BOOTSTRAP, Config::default().query_timeout(timeout)).await;
    let mut order = ids.clone();
    order.shuffle(&mut rng(SEED + 1));
    let mut hung = HashSet::new();
    for id in order {
        if hung.len() >= N / 8 {
            break;
        }
        // Keep the queriers used by `score` (nodes[0..KEYS]) reachable.
        if ids.iter().position(|x| *x == id).unwrap() < KEYS {
            continue;
        }
        hung.insert(id);
        modes_a.set(id, NodeMode::Hung);
        modes_b.set(id, NodeMode::Hung);
    }
    let expected = |key: &Id| {
        let live: Vec<NodeId> = ids
            .iter()
            .copied()
            .filter(|id| !hung.contains(id))
            .collect();
        expected_ids(&live, *key, K)
    };
    let round = score(&nodes_a, &ids, &modes_a, expected, false).await;
    let stream = score(&nodes_b, &ids, &modes_b, expected, true).await;
    report("bootstrap+hung", &round, &stream);
    assert!(
        round.recall >= 5.0,
        "round-based live recall {} too low",
        round.recall
    );
    assert!(
        stream.recall >= 5.0,
        "stream live recall {} too low",
        stream.recall
    );
}
