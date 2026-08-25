//! Protocol tests on small in-memory swarms (and one tiny iroh swarm).
//!
//! These assert behaviour. Swarm plots live in [`super::viz`] and are ignored
//! by default.

use std::{num::NonZeroU64, time::Duration};

use iroh::SecretKey;
use rand::Rng;
use testresult::TestResult;

use super::*;
use crate::{
    routing::{K, RoutingTable},
    rpc::{Blake3Immutable, Kind, SetResponse, Value},
};

const TINY: usize = 24;

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

fn immutable(data: &[u8]) -> Value {
    Value::Blake3Immutable(Blake3Immutable {
        timestamp: 1,
        data: data.to_vec(),
    })
}

async fn collect_values(
    rpc: &RpcClient,
    key: Id,
    kind: Kind,
    seed: Option<NonZeroU64>,
    n: Option<NonZeroU64>,
) -> Vec<Value> {
    let mut rx = rpc.get_all(key, kind, seed, n).await.unwrap();
    let mut out = Vec::new();
    while let Ok(Some(v)) = rx.recv().await {
        out.push(v);
    }
    out
}

async fn tiny_swarm(n: usize) -> (Vec<NodeId>, Nodes) {
    let ids = create_node_ids(&create_secrets(0, n));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    init_routing_tables(&nodes, &ids, Some(0)).await.unwrap();
    (ids, nodes)
}

#[test]
fn tiny_swarm_tables_hold_everyone() {
    let ids = create_node_ids(&create_secrets(0, TINY));
    let mut rt = RoutingTable::new(ids[0], None);
    for id in &ids[1..] {
        assert!(rt.add_node(*id), "dropped {id}");
    }
    assert_eq!(rt.nodes().count(), TINY - 1);
}

#[test]
fn find_closest_nodes_xor_order() {
    let ids = create_node_ids(&create_secrets(0, TINY));
    let mut rt = RoutingTable::new(ids[0], None);
    for id in &ids[1..] {
        assert!(rt.add_node(*id));
    }
    let key = Id::from([0x55u8; 32]);
    let got = rt.find_closest_nodes(&key, 5);
    let expected = expected_ids(&ids[1..], key, 5);
    assert_eq!(got, expected);
}

#[test]
fn full_bucket_drops_newcomers() {
    // Intended stability policy: once a k-bucket is full, later nodes are not
    // inserted and existing members are left alone.
    let local = create_node_ids(&create_secrets(0, 1))[0];
    let mut rt = RoutingTable::new(local, None);
    let mut rng = rng(1);
    let mut dropped = None;
    for _ in 0..10_000 {
        let id = SecretKey::from_bytes(&rng.r#gen()).public();
        if !rt.add_node(id) {
            dropped = Some(id);
            break;
        }
    }
    let dropped = dropped.expect("a k-bucket should fill");
    let size = rt.nodes().count();
    assert!(!rt.contains(&dropped));
    assert!(!rt.add_node(dropped));
    assert_eq!(rt.nodes().count(), size);
}

#[test]
fn u256_shift_by_zero_is_identity() {
    let x = crate::u256::U256::from_le_bytes([0x5au8; 32]);
    assert_eq!(x << 0, x);
    assert_eq!(x >> 0, x);
}

#[test]
fn blend_zero_is_left_operand() {
    let a = crate::u256::U256::from_le_bytes([1u8; 32]);
    let b = crate::u256::U256::from_le_bytes([2u8; 32]);
    assert_eq!(crate::blend(a, b, 0), a);
    assert_eq!(crate::blend(a, b, 256), b);
}

#[tokio::test]
async fn nodes_seen_and_dead_update_routing_table() {
    let ids = create_node_ids(&create_secrets(0, 3));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (_, api) = &nodes[0].1;
    let peer = ids[1];

    api.nodes_seen(&[peer]).await.unwrap();
    let table = api.get_routing_table().await.unwrap();
    assert!(table.iter().any(|bucket| bucket.contains(&peer)));

    api.nodes_dead(&[peer]).await.unwrap();
    let table = api.get_routing_table().await.unwrap();
    assert!(table.iter().all(|bucket| !bucket.contains(&peer)));
}

#[tokio::test]
async fn lookup_with_full_tables_returns_k_closest() {
    let (ids, nodes) = tiny_swarm(TINY).await;
    let (_, api) = &nodes[0].1;
    let key = Id::blake3_hash(b"lookup-target");
    let mut got = api.lookup(key, None).await.unwrap();
    let mut expected = expected_ids(&ids, key, K);
    got.sort();
    expected.sort();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn set_rejects_when_k_closer_nodes_are_known() {
    let (ids, nodes) = tiny_swarm(TINY).await;
    let key = Id::blake3_hash(b"placement");
    let closest = expected_ids(&ids, key, K);
    let value = immutable(b"placement");

    let close = closest[0];
    let (close_rpc, _) = nodes.iter().find(|(id, _)| *id == close).unwrap().1.clone();
    assert!(matches!(
        close_rpc.set(key, value.clone()).await.unwrap(),
        SetResponse::Ok
    ));

    let far = ids
        .iter()
        .copied()
        .find(|id| !closest.contains(id))
        .expect("tiny swarm should have nodes outside the k closest");
    let (far_rpc, _) = nodes.iter().find(|(id, _)| *id == far).unwrap().1.clone();
    assert!(matches!(
        far_rpc.set(key, value).await.unwrap(),
        SetResponse::ErrDistance
    ));
}

#[tokio::test]
async fn put_immutable_only_reports_nodes_that_accepted() {
    let (ids, nodes) = tiny_swarm(TINY).await;
    let (_, api) = &nodes[0].1;
    let data = b"hello dht";
    let (hash, stored_at) = api.put_immutable(data).await.unwrap();
    let key = Id::from(*hash.as_bytes());
    let expected = expected_ids(&ids, key, K);

    assert!(!stored_at.is_empty());
    for id in &stored_at {
        assert!(
            expected.contains(id),
            "put reported {id} which is not among the k closest"
        );
    }
    assert_eq!(api.get_immutable(hash).await.unwrap(), Some(data.to_vec()));
}

#[tokio::test]
async fn get_immutable_skips_hash_mismatch() {
    let ids = create_node_ids(&create_secrets(0, 1));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (rpc, api) = &nodes[0].1;
    let data = b"real";
    let hash = blake3::hash(data);
    let key = Id::from(*hash.as_bytes());

    // get_immutable asks each peer for a single value, so a hash mismatch is
    // terminal for that peer.
    rpc.set(key, immutable(b"fake")).await.unwrap();
    assert_eq!(api.get_immutable(hash).await.unwrap(), None);
}

#[tokio::test]
async fn get_immutable_returns_matching_blob() {
    let ids = create_node_ids(&create_secrets(0, 1));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (rpc, api) = &nodes[0].1;
    let data = b"real";
    let hash = blake3::hash(data);
    let key = Id::from(*hash.as_bytes());
    rpc.set(key, immutable(data)).await.unwrap();
    assert_eq!(api.get_immutable(hash).await.unwrap(), Some(data.to_vec()));
}

#[tokio::test]
async fn get_all_honors_n_without_seed() {
    let ids = create_node_ids(&create_secrets(0, 1));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (rpc, _) = &nodes[0].1;
    let key = Id::from([7u8; 32]);
    for i in 0..5u8 {
        rpc.set(key, immutable(&[i])).await.unwrap();
    }

    let all = collect_values(rpc, key, Kind::Blake3Immutable, None, None).await;
    assert_eq!(all.len(), 5);

    let limited = collect_values(rpc, key, Kind::Blake3Immutable, None, Some(nz(2))).await;
    assert_eq!(limited.len(), 2);
}

#[tokio::test]
async fn get_all_n_greater_than_len_does_not_panic() {
    let ids = create_node_ids(&create_secrets(0, 1));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (rpc, _) = &nodes[0].1;
    let key = Id::from([8u8; 32]);
    rpc.set(key, immutable(b"only")).await.unwrap();

    let got = collect_values(rpc, key, Kind::Blake3Immutable, Some(nz(1)), Some(nz(100))).await;
    assert_eq!(got.len(), 1);
}

#[tokio::test]
async fn get_all_empty_key_ends_stream() {
    let ids = create_node_ids(&create_secrets(0, 1));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (rpc, _) = &nodes[0].1;
    let key = Id::from([9u8; 32]);
    let got = collect_values(rpc, key, Kind::Blake3Immutable, None, Some(nz(1))).await;
    assert!(got.is_empty());
}

#[tokio::test]
async fn candidate_lookup_without_strategy_does_not_hang() {
    let ids = create_node_ids(&create_secrets(0, 1));
    let nodes = create_nodes(&ids, next_n(0), Config::default()).await;
    let (_, api) = &nodes[0].1;
    tokio::time::timeout(Duration::from_secs(1), api.candidate_lookup())
        .await
        .expect("candidate_lookup hung (oneshot never completed)");
}

#[tokio::test(flavor = "multi_thread")]
async fn iroh_put_get_roundtrip() -> TestResult<()> {
    let secrets = create_secrets(0, 4);
    let iroh_nodes = iroh_create_nodes(&secrets, 3, None).await?;
    let nodes = iroh_nodes
        .iter()
        .map(|(ep, x)| (ep.id(), x.clone()))
        .collect::<Vec<_>>();
    let ids = nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    init_routing_tables(&nodes, &ids, Some(0)).await?;
    let _routers = spawn_routers(&iroh_nodes);
    let (_, api) = &nodes[0].1;
    let (hash, stored_at) = api.put_immutable(b"hello iroh").await?;
    assert!(!stored_at.is_empty());
    assert_eq!(api.get_immutable(hash).await?, Some(b"hello iroh".to_vec()));
    Ok(())
}
