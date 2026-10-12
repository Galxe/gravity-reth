use crate::{
    test_utils::create_test_provider_factory, AccountReader, BlockHashReader, BlockNumReader,
    DatabaseProviderFactory, HeaderProvider, StateWriter, StaticFileProviderFactory,
    StaticFileWriter, TrieWriterV2,
};
use alloy_consensus::Header;
use alloy_primitives::{keccak256, Address, B256, U256};
use reth_db_api::{models::StoredBlockBodyIndices, tables, transaction::DbTxMut};
use reth_primitives_traits::{Account, SealedHeader};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use reth_trie::{HashedPostState, TrieInput};
use reth_trie_db::nested_hash::NestedStateRoot;
use std::{sync::mpsc, thread, time::Duration};

#[test]
fn published_rpc_views_pin_all_stores_without_blocking_append() {
    let factory = create_test_provider_factory();
    let address = Address::ZERO;
    let slot = B256::ZERO;
    let write_block = |number: u64, parent_hash| {
        let account = Account { balance: U256::from(number + 1), ..Default::default() };
        let mut state = HashedPostState::default();
        state.accounts.insert(keccak256(address), Some(account));
        state
            .storages
            .entry(keccak256(address))
            .or_default()
            .storage
            .insert(keccak256(slot), U256::from(number + 1));
        let provider = factory.provider_rw().unwrap();
        let (state_root, updates) =
            NestedStateRoot::new(provider.tx_ref(), None).calculate(&state).unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.write_hashed_state(&state.into_sorted()).unwrap();
        let header = Header { number, parent_hash, state_root, ..Default::default() };
        let hash = header.hash_slow();
        let static_files = factory.static_file_provider();
        let mut headers = static_files.latest_writer(StaticFileSegment::Headers).unwrap();
        headers.append_header(&header, U256::ZERO, &hash).unwrap();
        headers.commit().unwrap();
        provider.tx_ref().put::<tables::PlainAccountState>(address, account).unwrap();
        provider.tx_ref().put::<tables::CanonicalHeaders>(number, hash).unwrap();
        provider.tx_ref().put::<tables::HeaderNumbers>(hash, number).unwrap();
        provider
            .tx_ref()
            .put::<tables::BlockBodyIndices>(
                number,
                StoredBlockBodyIndices { first_tx_num: 0, tx_count: 0 },
            )
            .unwrap();
        for stage in [
            StageId::Execution,
            StageId::AccountHashing,
            StageId::IndexAccountHistory,
            StageId::MerkleExecute,
        ] {
            provider
                .tx_ref()
                .put::<tables::StageCheckpoints>(stage.to_string(), StageCheckpoint::new(number))
                .unwrap();
        }
        provider.commit().unwrap();
        SealedHeader::new(header, hash)
    };

    let mut write = factory.consistent_write();
    let genesis = write_block(0, B256::ZERO);
    factory.publish_rpc_read_view(genesis.num_hash()).unwrap();
    write.complete();
    drop(write);
    let rpc = factory.rpc_provider();
    let old = rpc.provider().unwrap();

    let mut write = factory.consistent_write();
    let next = write_block(1, genesis.hash());
    let reader_factory = rpc.clone();
    let (send, receive) = mpsc::channel();
    let reader = thread::spawn(move || {
        let provider = reader_factory.provider().unwrap();
        send.send((
            provider.best_block_number().unwrap(),
            provider.basic_account(&address).unwrap().unwrap().balance,
        ))
        .unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(3));
    // Release the write range before asserting so a blocked reader cannot hang the test.
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
    drop(write);
    reader.join().unwrap();
    assert_eq!(result.unwrap(), (0, U256::from(1)));

    let current = rpc.provider().unwrap();
    for (provider, header, value) in [(&old, &genesis, 1), (&current, &next, 2)] {
        assert_eq!(provider.best_block_number().unwrap(), header.number);
        assert_eq!(provider.basic_account(&address).unwrap().unwrap().balance, U256::from(value));
        assert_eq!(provider.block_hash(header.number).unwrap(), Some(header.hash()));
        let proof = provider.latest().proof(TrieInput::default(), address, &[slot]).unwrap();
        assert_eq!(proof.info.unwrap().balance, U256::from(value));
        assert_eq!(proof.storage_proofs[0].value, U256::from(value));
        proof.verify(header.state_root).unwrap();
    }
    assert!(old.header_by_number(1).unwrap().is_none());
    assert!(old.block_hash(1).unwrap().is_none());
    assert!(factory.rpc_maintenance(false).is_none());
    drop(current);
    drop(old);

    let mut maintenance = factory.rpc_maintenance(false).unwrap();
    assert!(rpc.provider().is_err());
    let mut write = factory.consistent_write();
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
    drop(write);
    maintenance.complete();
    assert!(rpc.provider().is_err());
    drop(maintenance);
    assert_eq!(rpc.provider().unwrap().best_block_number().unwrap(), 1);
}
