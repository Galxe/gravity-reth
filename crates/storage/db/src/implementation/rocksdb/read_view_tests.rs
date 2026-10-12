use super::*;
use crate::implementation::rocksdb::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind};
use alloy_primitives::{b256, Bytes};
use reth_db_api::{
    cursor::DbDupCursorRO,
    database::Database,
    models::ClientVersion,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_trie_common::{nested_trie::StorageNodeEntry, StoredNibbles, StoredNibblesSubKey};
use std::{sync::mpsc, thread, time::Duration};

fn open() -> (tempfile::TempDir, Arc<DatabaseEnv>) {
    let dir = tempfile::tempdir().unwrap();
    let env = Arc::new(
        DatabaseEnv::open(
            dir.path(),
            DatabaseEnvKind::RW,
            DatabaseArguments::new(ClientVersion::default()),
        )
        .unwrap(),
    );
    (dir, env)
}

fn write_three_stores(env: &DatabaseEnv, value: u8) {
    let tx = env.tx_mut().unwrap();
    tx.put::<tables::Metadata>("read-view-test".into(), vec![value]).unwrap();
    tx.put::<tables::AccountsTrieV2>(StoredNibbles::from(vec![1]), Bytes::from(vec![value]))
        .unwrap();
    tx.put::<tables::StoragesTrieV2>(
        b256!("0000000000000000000000000000000000000000000000000000000000000001"),
        StorageNodeEntry {
            path: StoredNibblesSubKey::from(vec![2]),
            node: Bytes::from(vec![value]),
        },
    )
    .unwrap();
    tx.commit().unwrap();
}

fn assert_three_stores(tx: &impl DbTx, value: u8) {
    assert_eq!(tx.get::<tables::Metadata>("read-view-test".into()).unwrap(), Some(vec![value]));
    assert_eq!(
        tx.get::<tables::AccountsTrieV2>(StoredNibbles::from(vec![1])).unwrap(),
        Some(Bytes::from(vec![value]))
    );
    assert_eq!(
        tx.cursor_dup_read::<tables::StoragesTrieV2>()
            .unwrap()
            .get_by_key_subkey(
                b256!("0000000000000000000000000000000000000000000000000000000000000001"),
                StoredNibblesSubKey::from(vec![2]),
            )
            .unwrap()
            .unwrap()
            .node,
        Bytes::from(vec![value])
    );
}

fn capture(env: &DatabaseEnv) -> Option<Arc<dyn DatabaseReadView>> {
    let mut write = env.consistent_write();
    let view = env.read_view().unwrap();
    write.complete();
    view
}

#[test]
fn shared_read_view_stays_fixed_without_waiting_for_append() {
    let (_dir, env) = open();
    write_three_stores(&env, 1);
    let view = capture(&env);
    let reader_view = view.clone();
    let mut write = env.consistent_write();
    write_three_stores(&env, 2);
    let (send, recv) = mpsc::channel();
    let reader_env = env.clone();
    let reader = thread::spawn(move || {
        let tx = reader_env.tx_with_read_view(reader_view).unwrap();
        assert_three_stores(&tx, 1);
        send.send(()).unwrap();
    });
    let result = recv.recv_timeout(Duration::from_secs(5));
    write.complete();
    drop(write);
    reader.join().unwrap();
    result.unwrap();
    assert_three_stores(&env.tx_with_read_view(view.clone()).unwrap(), 1);
    assert_three_stores(&env.tx_with_read_view(capture(&env)).unwrap(), 2);

    drop(env.consistent_write());
    assert!(env.tx().is_err());
    assert_three_stores(&env.tx_with_read_view(view).unwrap(), 1);
    let mut recovery = env.consistent_write();
    recovery.recovered();
    drop(recovery);
    assert!(env.tx().is_ok());
}
