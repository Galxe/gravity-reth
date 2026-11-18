#![allow(missing_docs)]

use alloy_primitives::{
    map::{FbBuildHasher, HashMap},
    B256, U256,
};
use criterion::{measurement::WallTime, *};
use rand::SeedableRng;
use reth_eth_wire::EthVersion;
use reth_eth_wire_types::EthNetworkPrimitives;
use reth_network::{
    test_utils::{
        transactions::{buffer_hash_to_tx_fetcher, new_mock_session},
        Testnet,
    },
    transactions::{
        fetcher::TransactionFetcher, TransactionFetcherConfig, TransactionPropagationMode::Max,
        TransactionsManagerConfig,
    },
};
use reth_network_peers::PeerId;
use reth_primitives_traits::SignedTransaction;
use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
use reth_transaction_pool::{
    test_utils::TransactionGenerator, EthPooledTransaction, PoolTransaction, TransactionPool,
};
use tokio::runtime::Runtime as TokioRuntime;

criterion_group!(
    name = tx_fetch_benches;
    config = Criterion::default();
    targets = tx_fetch_bench, fetch_pending_hashes,
);

pub fn benchmark_fetch_pending_hashes(group: &mut BenchmarkGroup<'_, WallTime>, peers_num: usize) {
    let group_id = format!("fetch pending hashes, peers num: {peers_num}");
    group.bench_function(group_id, |b| {
        b.iter_batched(
            || {
                let mut fetcher = TransactionFetcher::<EthNetworkPrimitives>::default();
                let mut peers = HashMap::<_, _, FbBuildHasher<64>>::default();
                let mut sessions = Vec::with_capacity(peers_num);
                for _ in 0..peers_num {
                    let peer = PeerId::random();
                    let hash = B256::random();
                    let (mut metadata, session) = new_mock_session(peer, EthVersion::Eth66);
                    metadata.seen_transactions_mut().insert(hash);
                    peers.insert(peer, metadata);
                    sessions.push(session);
                    buffer_hash_to_tx_fetcher(&mut fetcher, hash, peer, None);
                }
                (fetcher, peers, sessions)
            },
            |(mut fetcher, peers, _sessions)| {
                std::hint::black_box(fetcher.dispatch(&peers, 256));
            },
            BatchSize::SmallInput,
        );
    });
}

pub fn fetch_pending_hashes(c: &mut Criterion) {
    let mut group = c.benchmark_group("Fetch Pending Hashes");

    for peers in [5, 10, 20, 100, 1000, 10000, 100000] {
        benchmark_fetch_pending_hashes(&mut group, peers);
    }

    group.finish();
}

pub fn tx_fetch_bench(c: &mut Criterion) {
    let rt = TokioRuntime::new().unwrap();

    let mut group = c.benchmark_group("Transaction Fetch");
    group.sample_size(30);

    group.bench_function("fetch_transactions", |b| {
        b.to_async(&rt).iter_with_setup(
            || {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        let tx_manager_config = TransactionsManagerConfig {
                            propagation_mode: Max(0),
                            transaction_fetcher_config: TransactionFetcherConfig {
                                max_inflight_requests: 1,
                                ..Default::default()
                            },
                            ..Default::default()
                        };

                        let mut genesis = reth_chainspec::MAINNET.genesis.clone();
                        genesis.gas_limit = 30_000_000;
                        let chain_spec =
                            reth_chainspec::ChainSpecBuilder::mainnet().genesis(genesis).build();
                        let provider = MockEthProvider::default().with_chain_spec(chain_spec);
                        provider.add_header(
                            provider.chain_spec.genesis_hash(),
                            provider.chain_spec.genesis_header().clone(),
                        );
                        let num_peers = 10;
                        let net = Testnet::create_with(num_peers, provider.clone()).await;

                        // install request handlers
                        let net = net.with_eth_pool_config(tx_manager_config);
                        let handle = net.spawn();

                        // connect all the peers first
                        handle.connect_peers().await;

                        let listening_peer = &handle.peers()[num_peers - 1];
                        let listening_peer_tx_listener =
                            listening_peer.pool().unwrap().pending_transactions_listener();

                        let num_tx_per_peer = 10;

                        let mut tx_gen = TransactionGenerator::with_num_signers(
                            rand::rngs::StdRng::seed_from_u64(0),
                            num_tx_per_peer * (num_peers - 1),
                        );
                        let signers = tx_gen.signer_keys.clone();
                        for i in 0..num_peers - 1 {
                            let peer_pool = handle.peers()[i].pool().unwrap();
                            for j in 0..num_tx_per_peer {
                                // Unique senders with nonce zero keep every generated tx pending.
                                let signer = signers[i * num_tx_per_peer + j];
                                let mut tx = tx_gen.transaction();
                                tx.signer = signer;
                                let tx = EthPooledTransaction::try_from_consensus(
                                    tx.into_eip1559().try_into_recovered().unwrap(),
                                )
                                .unwrap();
                                provider.add_account(
                                    tx.sender(),
                                    ExtendedAccount::new(0, U256::from(10u128.pow(18))),
                                );
                                peer_pool.add_external_transaction(tx).await.unwrap();
                            }
                        }

                        // Total expected transactions
                        let total_expected_tx = num_tx_per_peer * (num_peers - 1);

                        (listening_peer_tx_listener, total_expected_tx)
                    })
                })
            },
            |(mut listening_peer_tx_listener, total_expected_tx)| async move {
                let mut received_tx = 0;
                while listening_peer_tx_listener.recv().await.is_some() {
                    received_tx += 1;
                    if received_tx >= total_expected_tx {
                        break;
                    }
                }
            },
        )
    });

    group.finish();
}

criterion_main!(tx_fetch_benches);
