//! Lookup behaviour when some peers are slow or hung.
//!
//! Faults are injected in [`super::TestPool`]: delay or never return a client.
//! Iterative lookup only sheds those peers if [`Config::query_timeout`] is set.

use std::time::Duration;

use super::*;
use crate::{routing::K, rpc::Id};

const TINY: usize = 24;

async fn swarm(config: Config) -> (Vec<NodeId>, Nodes, NetFaults) {
    let ids = create_node_ids(&create_secrets(0, TINY));
    let (nodes, _, faults) = create_nodes_and_clients(&ids, next_n(0), config).await;
    init_routing_tables(&nodes, &ids, Some(0)).await.unwrap();
    (ids, nodes, faults)
}

fn pick_hung(querier: NodeId, closest: &[NodeId]) -> NodeId {
    closest
        .iter()
        .copied()
        .find(|id| *id != querier)
        .expect("need a closest node other than the querier")
}

#[tokio::test(start_paused = true)]
async fn lookup_skips_hung_peer() {
    let timeout = Duration::from_millis(100);
    let (ids, nodes, faults) = swarm(Config::default().query_timeout(timeout)).await;
    let querier = nodes[0].0;
    let key = Id::blake3_hash(b"slow-lookup");
    let closest = expected_ids(&ids, key, K);
    let hung = pick_hung(querier, &closest);
    faults.hang(hung);

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
    let (ids, nodes, faults) = swarm(Config::default().query_timeout(timeout)).await;
    let querier = nodes[0].0;
    let key = Id::blake3_hash(b"delayed-lookup");
    let closest = expected_ids(&ids, key, K);
    let slow = pick_hung(querier, &closest);
    faults.delay(slow, Duration::from_millis(10));

    let (_, api) = &nodes[0].1;
    let got = api.lookup(key, None).await.unwrap();
    assert!(
        got.contains(&slow),
        "peer delayed by less than query_timeout should still be used"
    );
}
