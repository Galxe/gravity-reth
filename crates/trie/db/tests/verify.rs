//! Verifies legacy trie views using one captured native database snapshot.
#![cfg(not(target_family = "wasm"))]

use alloy_primitives::{keccak256, B256, U256};
use reth_db::{test_utils::create_test_rw_db, DatabaseEnv};
use reth_db_api::{
    database::Database,
    tables,
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use reth_primitives_traits::{Account, StorageEntry};
use reth_trie::{
    parallel_verify::Output, trie_cursor::noop::NoopTrieCursorFactory, updates::TrieUpdates,
    BranchNodeCompact, Nibbles, StateRoot, StorageTrieEntry, StoredNibbles, StoredNibblesSubKey,
};
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseStorageRoot, LegacyDatabaseVerifier};

fn write_hashed_state(db: &DatabaseEnv, version: u64) {
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        for i in 1u64..=32 {
            let address = keccak256(i.to_be_bytes());
            tx.put::<tables::HashedAccounts>(
                address,
                Account { nonce: i, balance: U256::from(i + version), bytecode_hash: None },
            )
            .unwrap();
            if i <= 2 {
                for slot in 1u64..=64 {
                    tx.put::<tables::HashedStorages>(
                        address,
                        StorageEntry {
                            key: keccak256(slot.to_be_bytes()),
                            value: U256::from(slot + version),
                        },
                    )
                    .unwrap();
                }
            }
        }
        tx.commit().unwrap();
    });
}

fn write_legacy_cache(db: &DatabaseEnv, updates: TrieUpdates) {
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        tx.clear::<tables::AccountsTrie>().unwrap();
        tx.clear::<tables::StoragesTrie>().unwrap();
        for (path, node) in updates.account_nodes {
            tx.put::<tables::AccountsTrie>(StoredNibbles(path), node).unwrap();
        }
        for (address, storage) in updates.storage_tries {
            for (path, node) in storage.storage_nodes {
                tx.put::<tables::StoragesTrie>(
                    address,
                    StorageTrieEntry { nibbles: StoredNibblesSubKey(path), node },
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
    });
}

fn all_findings(
    iter: impl Iterator<Item = Result<Output, reth_execution_errors::StateRootError>>,
) -> Vec<Output> {
    iter.map(Result::unwrap).filter(|output| !matches!(output, Output::Progress(_))).collect()
}

#[test]
fn workers_opened_after_commit_reuse_the_original_captured_snapshot() {
    let temp = create_test_rw_db();
    let db = temp.db();
    write_hashed_state(db, 0);
    let state_snapshot = db.snapshot(true).unwrap().unwrap();
    let tx = db.tx_with_snapshot(Some(state_snapshot)).unwrap();
    let (_, updates) =
        StateRoot::new(NoopTrieCursorFactory::default(), DatabaseHashedCursorFactory::new(&tx))
            .root_with_updates()
            .unwrap();
    assert!(!updates.account_nodes.is_empty());
    assert!(updates.storage_tries.values().any(|storage| !storage.storage_nodes.is_empty()));
    write_legacy_cache(db, updates);
    let captured = db.snapshot(true).unwrap().unwrap();

    // These real writes participate in the same coordinator used by snapshot(true).
    // Deliberately leave the old legacy caches so a fresh verifier must report inconsistencies.
    write_hashed_state(db, 1000);
    let address = keccak256(1u64.to_be_bytes());
    assert_eq!(
        db.tx().unwrap().get::<tables::HashedAccounts>(address).unwrap().unwrap().balance,
        U256::from(1001)
    );

    // No verifier transaction or worker existed until AFTER the writer committed.
    for workers in [1, 3] {
        let old = LegacyDatabaseVerifier::from_snapshot(db, captured.clone(), workers, 1).unwrap();
        assert!(all_findings(old).is_empty());
    }
    let current = all_findings(LegacyDatabaseVerifier::new(db, 3, 2).unwrap());
    assert!(current.iter().any(|output| matches!(output, Output::AccountWrong { .. })));
    assert!(current.iter().any(|output| matches!(output, Output::StorageWrong { .. })));
}

#[test]
fn snapshot_from_another_database_preserves_the_backend_error() {
    let first = create_test_rw_db();
    let second = create_test_rw_db();
    let snapshot = first.db().snapshot(true).unwrap().unwrap();
    let error = LegacyDatabaseVerifier::from_snapshot(second.db(), snapshot, 2, 1).unwrap_err();
    assert!(
        matches!(error, DatabaseError::Other(message) if message == "snapshot belongs to another database")
    );
}

#[test]
fn address_scan_covers_hashed_only_and_legacy_trie_only_orphans() {
    let temp = create_test_rw_db();
    let db = temp.db();
    let hashed_only = B256::repeat_byte(0x11);
    let trie_only = B256::repeat_byte(0x22);
    let path = Nibbles::from_nibbles([1]);
    let stale = BranchNodeCompact::default();
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        for slot in 1u64..=64 {
            tx.put::<tables::HashedStorages>(
                hashed_only,
                StorageEntry { key: keccak256(slot.to_be_bytes()), value: U256::from(slot) },
            )
            .unwrap();
        }
        tx.put::<tables::StoragesTrie>(
            trie_only,
            StorageTrieEntry { nibbles: StoredNibblesSubKey(path), node: stale.clone() },
        )
        .unwrap();
        tx.commit().unwrap();
    });
    let snapshot = db.snapshot(true).unwrap().unwrap();
    let tx = db.tx_with_snapshot(Some(snapshot)).unwrap();
    let (_, _, updates) =
        reth_trie::StorageRoot::from_tx_hashed(&tx, hashed_only).root_with_updates().unwrap();
    assert!(!updates.storage_nodes.is_empty());
    db.with_write_lock(|| {
        let tx = db.tx_mut().unwrap();
        for (path, node) in updates.storage_nodes {
            tx.put::<tables::StoragesTrie>(
                hashed_only,
                StorageTrieEntry { nibbles: StoredNibblesSubKey(path), node },
            )
            .unwrap();
        }
        tx.commit().unwrap();
    });
    for lookahead in [1, 3] {
        let verifier = LegacyDatabaseVerifier::new(db, 2, lookahead).unwrap();
        assert_eq!(
            all_findings(verifier),
            vec![Output::StorageExtra(trie_only, path, stale.clone())]
        );
    }
}
