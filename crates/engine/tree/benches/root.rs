#![allow(missing_docs)]

use std::{collections::HashMap, sync::Arc};

use alloy_primitives::{keccak256, Address, B256, U256};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::Rng;
use reth_engine_tree::tree::{multiproof::SparseTrieUpdate, sparse_trie::update_sparse_trie};
use reth_primitives_traits::Account;
use reth_provider::{
    providers::ConsistentDbView, test_utils::create_test_provider_factory, PersistBlockCache, StateWriter, TrieWriter
};
use reth_trie::{
    prefix_set::TriePrefixSetsMut, proof::ProofTrieNodeProviderFactory, updates::TrieUpdatesSorted,
    HashedPostState, HashedPostStateSorted, HashedStorage, LeafNode, LeafNodeRef, Nibbles,
    StateRoot,
};
use reth_trie_db::DatabaseStateRoot;
use reth_trie_parallel::{
    nested_hash::NestedStateRoot, proof::ParallelProof, proof_task::{ProofTaskCtx, ProofTaskManager, ProofTaskTx}
};
use reth_trie_sparse::{SerialSparseTrie, SparseStateTrie};
use reth_trie_sparse_parallel::ParallelSparseTrie;
use tokio::runtime::Runtime;

fn random_state(
    account_size: usize,
    storage_trie_size: usize,
) -> (HashedPostState, HashedPostState) {
    let mut storage_trie_sum = 0;
    let mut rng = rand::rng();
    let db_state = (0..account_size)
        .map(|_| {
            let address = B256::random();
            let account =
                Account { balance: U256::from(rng.random::<u64>()), ..Default::default() };
            let mut storage = HashMap::<B256, U256>::default();
            if storage_trie_sum < storage_trie_size {
                for _ in 0..account_size {
                    let key = B256::random();
                    let value = U256::random();
                    // println!("random_state {} {}", key, value);
                    storage.insert(key, value);
                }
                storage_trie_sum += 1;
            }
            (address, (account, storage))
        })
        .collect::<HashMap<_, _>>();

    let mut updated_state = HashMap::new();
    let modify_slot = 1500;
    db_state.iter().for_each(|(address, (account, storage))| {
        if !storage.is_empty() {
            let mut new_storage = HashMap::<B256, U256>::default();
            let mut i = 0;
            for (key, _) in storage {
                new_storage.insert(*key, U256::random());
                i += 1;
                if i == modify_slot {
                    break;
                }
            }
            let mut new_account = account.clone();
            new_account.nonce += 1;
            updated_state.insert(address.clone(), (new_account, new_storage));
        }
    });

    (
        HashedPostState::default()
            .with_accounts(
                db_state.iter().map(|(address, (account, _))| (address.clone(), Some(*account))),
            )
            .with_storages(db_state.into_iter().map(|(address, (_, storage))| {
                (address, HashedStorage::from_iter(false, storage))
            })),
        HashedPostState::default()
            .with_accounts(
                updated_state
                    .iter()
                    .map(|(address, (account, _))| (address.clone(), Some(*account))),
            )
            .with_storages(updated_state.into_iter().map(|(address, (_, storage))| {
                (address, HashedStorage::from_iter(false, storage))
            })),
    )
}

fn calculate_state_root(c: &mut Criterion) {
    let mut group = c.benchmark_group("calculate root from leaves");
    group.sample_size(20);

    for size in [1_000, 5_000, 10_000, 100_000] {
        let (init_state, update_state) = random_state(size, 10);
        let provider_factory = create_test_provider_factory();
        {
            let provider_rw = provider_factory.provider_rw().unwrap();
            provider_rw.write_hashed_state(&init_state.into_sorted()).unwrap();
            let (_, updates) =
                StateRoot::from_tx(provider_rw.tx_ref()).root_with_updates().unwrap();
            provider_rw.write_trie_updates(&updates).unwrap();
            provider_rw.commit().unwrap();
        }

        let view = ConsistentDbView::new(provider_factory.clone(), None);

        let targets = update_state.multi_proof_targets();
        // println!("target {:?}", targets);

        let rt = Runtime::new().unwrap();
        let task_ctx =
            ProofTaskCtx::new(Default::default(), Default::default(), Default::default());
        let provider_ro = view.provider_ro().unwrap();
        let tx = provider_ro.into_tx();
        let task_tx = ProofTaskTx::new(tx, task_ctx.clone());
        let proof_task =
            ProofTaskManager::new(rt.handle().clone(), view.clone(), task_ctx.clone(), 1);

        let (trie_cursor_factory, hashed_cursor_factory) = task_tx.create_factories();

        let blinded_provider_factory = ProofTrieNodeProviderFactory::new(
            trie_cursor_factory,
            hashed_cursor_factory,
            task_ctx.prefix_sets(),
        );

        let proof_task_handle = proof_task.handle();

        // keep the join handle around to make sure it does not return any errors
        // after we compute the state root
        let _ = rt.spawn_blocking(move || proof_task.run());
        let cache = Some(PersistBlockCache::default());

        // encode_account_value(&new_db_state);
        // println!("targets {:?}\n update_state {:?}\nmultiproof {:?}", targets, update_state,
        // multi_proof); let mut trie = SparseStateTrie::<SerialSparseTrie,
        // SerialSparseTrie>::default().with_updates(true); let trie_update =
        // SparseTrieUpdate { state: update_state.clone(), multiproof: multi_proof.clone() };
        // let _ = update_sparse_trie(&mut trie, trie_update, &blinded_provider_factory);
        // println!("{:?}\n{:?}\n{:?}", db_state, new_db_state, trie);
        // let (state_root, updates) = trie.root_with_updates(&blinded_provider_factory).unwrap();
        // assert_eq!(true_root, state_root);

        // println!("{}\n{}\n{}", state_root, test_utils::state_root(db_state),
        // test_utils::state_root(new_db_state));
        group.bench_function(BenchmarkId::new("sparse trie", size), |b| {
            b.iter_with_setup(
                || {
                    let multi_proof = ParallelProof::new(
                        view.clone(),
                        Arc::new(TrieUpdatesSorted::default()),
                        Arc::new(HashedPostStateSorted::default()),
                        Arc::new(TriePrefixSetsMut::default()),
                        proof_task_handle.clone(),
                    ).decoded_multiproof(targets.clone())
                    .unwrap();      
                    let trie_update = SparseTrieUpdate {
                        state: update_state.clone(),
                        multiproof: multi_proof.clone(),
                    };
                    trie_update
                },
                |trie_update| {
                    let mut trie = SparseStateTrie::<ParallelSparseTrie, SerialSparseTrie>::default();
                    let _ = update_sparse_trie(&mut trie, trie_update, &blinded_provider_factory);
                    let (state_root, _) =
                        trie.root_with_updates(&blinded_provider_factory).unwrap();
                },
            )
        });

        let provider_ro = || view.provider_ro().map(|db| db.into_tx());
        group.bench_function(BenchmarkId::new("nested trie", size), |b| {
            b.iter_with_setup(
                || (),
                |()| {
                    // 1s
                    let nested_hash = NestedStateRoot::new(provider_ro, cache.clone());
                    let (state_root, _, _) = nested_hash.calculate(&update_state, false).unwrap();
                },
            )
        });

        // drop the handle to terminate the task and then block on the proof task handle to make
        // sure it does not return any errors
        drop(proof_task_handle);
    }
}

criterion_group!(sparse_parallel, calculate_state_root);
criterion_main!(sparse_parallel);
