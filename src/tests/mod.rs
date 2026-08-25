//! Test helpers shared by protocol tests and swarm visualizations.
mod protocol;
mod viz;

use std::sync::{Arc, Mutex};

use iroh::{
    Endpoint, SecretKey, address_lookup::memory::MemoryLookup, endpoint::BindError,
    protocol::Router,
};
use iroh_blobs::util::connection_pool::ConnectionPool;
use n0_future::{BufferedStreamExt, stream};
use rand::{Rng, rngs::StdRng, seq::SliceRandom};

use super::*;
use crate::pool::IrohPool;

#[derive(Debug, Clone)]
struct TestPool {
    clients: Arc<Mutex<BTreeMap<NodeId, RpcClient>>>,
    node_id: NodeId,
}

impl ClientPool for TestPool {
    async fn client(&self, id: NodeId) -> Result<RpcClient, String> {
        let client = self
            .clients
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| format!("client not found: {id}"))?;
        Ok(client)
    }

    fn id(&self) -> NodeId {
        self.node_id
    }
}

fn expected_ids(ids: &[NodeId], key: Id, n: usize) -> Vec<NodeId> {
    let mut expected = ids
        .iter()
        .cloned()
        .map(|id| (Distance::between(id.as_bytes(), &key), id))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    expected.dedup();
    expected.truncate(n);
    expected.into_iter().map(|(_, id)| id).collect()
}

type Nodes = Vec<(NodeId, (RpcClient, ApiClient))>;

fn rng(seed: u64) -> StdRng {
    let mut expanded = [0; 32];
    expanded[..8].copy_from_slice(&seed.to_le_bytes());
    StdRng::from_seed(expanded)
}

/// Choose bootstrap nodes.
///
/// Selection indexes wrap. Self and duplicates are skipped.
fn apply_selection(this: usize, ids: &[NodeId], selection: &[usize]) -> Vec<NodeId> {
    let mut res = Vec::new();
    for i in selection {
        if *i == this {
            continue;
        }
        let offset = i % ids.len();
        let id = ids[offset];
        if !res.contains(&id) {
            res.push(id);
        }
    }
    res
}

type Clients = Arc<Mutex<BTreeMap<NodeId, RpcClient>>>;

async fn create_nodes(
    ids: &[NodeId],
    select_bootstrap: impl Fn(usize) -> Vec<usize>,
    config: Config,
) -> Nodes {
    create_nodes_and_clients(ids, select_bootstrap, config)
        .await
        .0
}

async fn create_nodes_and_clients(
    ids: &[NodeId],
    select_bootstrap: impl Fn(usize) -> Vec<usize>,
    config: Config,
) -> (Nodes, Clients) {
    let clients = Arc::new(Mutex::new(BTreeMap::new()));
    let nodes = ids
        .iter()
        .enumerate()
        .map(|(offset, id)| {
            let pool = TestPool {
                clients: clients.clone(),
                node_id: *id,
            };
            let bootstrap = apply_selection(offset, ids, &select_bootstrap(offset));
            (
                *id,
                create_node_impl(*id, pool, bootstrap, None, config.clone()),
            )
        })
        .collect::<Vec<_>>();
    clients
        .lock()
        .unwrap()
        .extend(nodes.iter().map(|(id, (rpc, _))| (*id, rpc.clone())));
    (nodes, clients)
}

/// Insert `ids` into every node's routing table. Full buckets still drop extras.
async fn init_routing_tables(nodes: &Nodes, ids: &[NodeId], seed: Option<u64>) -> irpc::Result<()> {
    let mut rng = seed.map(rng);
    let ids = ids.to_vec();
    stream::iter(nodes.iter().enumerate())
        .for_each_concurrent(4096, |(index, (_, (_, api)))| {
            if ids.len() > 10000 {
                println!("{index}");
            }
            let mut ids = ids.clone();
            if let Some(rng) = &mut rng {
                ids.shuffle(rng);
            }
            async move {
                api.nodes_seen(&ids).await.ok();
            }
        })
        .await;
    Ok(())
}

fn next_n(n: usize) -> impl Fn(usize) -> Vec<usize> {
    move |offset| (1..=n).map(|i| offset + i).collect::<Vec<_>>()
}

const DHT_TEST_ALPN: &[u8] = b"iroh/dht/test-0";

type IrohNodes = Vec<(Endpoint, (RpcClient, ApiClient))>;

async fn iroh_create_nodes(
    secrets: &[SecretKey],
    mut n_bootstrap: usize,
    buckets: Option<Buckets>,
) -> std::result::Result<IrohNodes, BindError> {
    let n = secrets.len();
    let node_ids = secrets.iter().map(|s| s.public()).collect::<Vec<_>>();
    let node_ids = Arc::new(node_ids);
    let buckets = Arc::new(buckets);
    let discovery = MemoryLookup::new();
    n_bootstrap = n_bootstrap.min(n - 1);
    stream::iter(secrets.iter().zip(node_ids.iter()).enumerate())
        .map(|(offset, (secret, node_id))| {
            let buckets = buckets.clone();
            let node_ids = node_ids.clone();
            let discovery = discovery.clone();
            async move {
                let endpoint = Endpoint::builder(iroh::endpoint::presets::Minimal)
                    .secret_key(secret.clone())
                    .relay_mode(iroh::RelayMode::Disabled)
                    .address_lookup(discovery.clone())
                    .bind()
                    .await?;
                let addr = endpoint.addr();
                discovery.add_endpoint_info(addr.clone());
                let pool = ConnectionPool::new(
                    endpoint.clone(),
                    DHT_TEST_ALPN,
                    iroh_blobs::util::connection_pool::Options {
                        max_connections: 32,
                        idle_timeout: Duration::from_secs(1),
                        connect_timeout: Duration::from_secs(1),
                        on_connected: None,
                    },
                );
                let pool = IrohPool::new(endpoint.clone(), pool, discovery.clone());
                let bootstrap = (0..n_bootstrap)
                    .map(|i| node_ids[(offset + i + 1) % n])
                    .collect::<Vec<_>>();
                let (rpc, api) = create_node_impl(
                    *node_id,
                    pool.clone(),
                    bootstrap,
                    (*buckets).clone(),
                    Default::default(),
                );
                pool.set_self_client(Some(rpc.downgrade()));
                Ok((endpoint, (rpc, api)))
            }
        })
        .buffered_unordered(32)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect()
}

fn create_secrets(seed: u64, n: usize) -> Vec<SecretKey> {
    let mut rng = rng(seed);
    (0..n)
        .map(|_| SecretKey::from_bytes(&rng.r#gen::<[u8; 32]>()))
        .collect()
}

fn create_node_ids(secrets: &[SecretKey]) -> Vec<NodeId> {
    secrets.iter().map(|s| s.public()).collect()
}

// todo: we need a special protocol handler that validates the requester id of
// incoming FindNode messages to be the remote node id. This is pretty
// straightforward, but I can't write it right now because of some
// dependency weirdness due to all the patching.
fn spawn_routers(iroh_nodes: &IrohNodes) -> Vec<Router> {
    iroh_nodes
        .iter()
        .map(|(endpoint, (rpc, _))| {
            let sender = rpc.0.as_local().unwrap();
            Router::builder(endpoint.clone())
                .accept(DHT_TEST_ALPN, irpc_iroh::IrohProtocol::with_sender(sender))
                .spawn()
        })
        .collect()
}
