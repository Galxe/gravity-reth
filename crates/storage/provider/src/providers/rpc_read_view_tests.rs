use super::{BlockchainProvider, ProviderFactory, StaticFileProvider};
use crate::{
    test_utils::MockNodeTypesWithDB, AccountReader, BlockHashReader, BlockIdReader, BlockNumReader,
    BlockReader, BlockReaderIdExt, BlockSource, ChangeSetReader, ChangesetRangeReader,
    DatabaseProviderFactory, HeaderProvider, ReceiptProvider, StateProviderFactory,
    StaticFileProviderFactory, StaticFileWriter, StorageChangeSetReader, StorageSettingsCache,
    TransactionsProvider,
};
use alloy_consensus::{Header, SignableTransaction, TxLegacy};
use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{Address, Signature, B256, U256};
use reth_chain_state::{ExecutedBlockWithTrieUpdates, ExecutedTrieUpdates, NewCanonicalChain};
use reth_chainspec::MAINNET;
use reth_db::{init_db, DatabaseArguments, DatabaseEnv};
use reth_db_api::{
    database::Database,
    models::{
        AccountBeforeTx, GravityStorageSettings, ShardedKey, StorageBeforeTx,
        StoredBlockBodyIndices,
    },
    tables,
    transaction::{DbTx, DbTxMut},
    BlockNumberList,
};
use reth_ethereum_primitives::{Block, Receipt, TransactionSigned};
use reth_execution_types::ExecutionOutcome;
use reth_primitives_traits::{Account, RecoveredBlock, SealedHeader};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use revm_database::BundleState;
use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};
use tempfile::TempDir;

type TestFactory = ProviderFactory<MockNodeTypesWithDB<DatabaseEnv>>;

struct TestStorage {
    factory: TestFactory,
    _db_dir: TempDir,
    _static_dir: TempDir,
}

impl TestStorage {
    fn new() -> Self {
        let db_dir = tempfile::tempdir().unwrap();
        let static_dir = tempfile::tempdir().unwrap();
        let db = init_db(db_dir.path(), DatabaseArguments::new(Default::default())).unwrap();
        let factory = ProviderFactory::new(
            Arc::new(db),
            MAINNET.clone(),
            StaticFileProvider::read_write(static_dir.path()).unwrap(),
        );
        Self { factory, _db_dir: db_dir, _static_dir: static_dir }
    }
}

fn append_static_block(
    factory: &TestFactory,
    number: u64,
    parent_hash: B256,
) -> SealedHeader<Header> {
    append_static_block_with_root(factory, number, parent_hash, B256::ZERO)
}

fn append_static_block_with_root(
    factory: &TestFactory,
    number: u64,
    parent_hash: B256,
    state_root: B256,
) -> SealedHeader<Header> {
    let header = Header { number, parent_hash, state_root, ..Default::default() };
    let hash = header.hash_slow();
    let manager = factory.static_file_provider();
    let mut writer = manager.latest_writer(StaticFileSegment::Headers).unwrap();
    writer.append_header(&header, U256::ZERO, &hash).unwrap();
    writer.commit().unwrap();
    drop(writer);

    let tx: TransactionSigned = TxLegacy { nonce: number, ..Default::default() }
        .into_signed(Signature::test_signature())
        .into();
    let mut writer = manager.latest_writer(StaticFileSegment::Transactions).unwrap();
    writer.increment_block(number).unwrap();
    writer.append_transaction(number, &tx).unwrap();
    writer.commit().unwrap();
    drop(writer);

    let mut writer = manager.latest_writer(StaticFileSegment::Receipts).unwrap();
    writer.increment_block(number).unwrap();
    writer
        .append_receipt(number, &Receipt { cumulative_gas_used: number + 1, ..Default::default() })
        .unwrap();
    writer.commit().unwrap();
    SealedHeader::new(header, hash)
}

fn commit_block_metadata(factory: &TestFactory, header: &SealedHeader<Header>) {
    let provider = factory.provider_rw().unwrap();
    let tx = provider.tx_ref();
    tx.put::<tables::HeaderNumbers>(header.hash(), header.number).unwrap();
    tx.put::<tables::CanonicalHeaders>(header.number, header.hash()).unwrap();
    tx.put::<tables::BlockBodyIndices>(
        header.number,
        StoredBlockBodyIndices { first_tx_num: header.number, tx_count: 1 },
    )
    .unwrap();
    tx.put::<tables::PlainAccountState>(
        Address::ZERO,
        Account { balance: U256::from(header.number + 1), ..Default::default() },
    )
    .unwrap();
    tx.put::<tables::AccountsHistory>(
        ShardedKey::new(Address::ZERO, u64::MAX),
        BlockNumberList::new(0..=header.number).unwrap(),
    )
    .unwrap();
    for stage in [
        StageId::Execution,
        StageId::AccountHashing,
        StageId::IndexAccountHistory,
        StageId::MerkleExecute,
        StageId::Finish,
    ] {
        tx.put::<tables::StageCheckpoints>(stage.to_string(), StageCheckpoint::new(header.number))
            .unwrap();
    }
    provider.commit().unwrap();
}

fn publish_genesis(factory: &TestFactory) -> SealedHeader<Header> {
    let mut guard = factory.consistent_write();
    let genesis = append_static_block(factory, 0, B256::ZERO);
    commit_block_metadata(factory, &genesis);
    factory.publish_rpc_read_view(genesis.num_hash()).unwrap();
    guard.complete();
    genesis
}

#[test]
fn failed_rpc_publication_does_not_fail_a_committed_write_and_can_retry() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let mut write = factory.consistent_write();
    let genesis = append_static_block_with_root(factory, 0, B256::ZERO, reth_trie::EMPTY_ROOT_HASH);
    commit_block_metadata(factory, &genesis);
    factory.publish_rpc_read_view(genesis.num_hash()).unwrap();
    write.complete();
    drop(write);
    let rpc_factory = factory.rpc_provider();
    let old = rpc_factory.provider().unwrap();

    let mut write = factory.consistent_write();
    // A publication preflight error must close the old RPC view and leave the committed state
    // usable by the original execution providers. It must not poison the storage write guard.
    factory
        .publish_rpc_read_view(alloy_eips::BlockNumHash { number: 0, hash: B256::repeat_byte(9) })
        .unwrap();
    write.complete();
    drop(write);
    assert!(rpc_factory.provider().is_err());
    assert_eq!(factory.block_hash(0).unwrap(), Some(genesis.hash()));
    assert_eq!(
        factory.provider().unwrap().basic_account(&Address::ZERO).unwrap().unwrap().balance,
        U256::from(1)
    );
    assert_eq!(
        factory.database_provider_live_ro().unwrap().basic_account(&Address::ZERO).unwrap(),
        old.basic_account(&Address::ZERO).unwrap()
    );

    // A later complete boundary validates storage again and reopens RPC admission.
    let mut write = factory.consistent_write();
    factory.publish_rpc_read_view(genesis.num_hash()).unwrap();
    write.complete();
    drop(write);
    assert_eq!(rpc_factory.provider().unwrap().block_hash(0).unwrap(), Some(genesis.hash()));
}

#[test]
fn published_rpc_reads_do_not_wait_for_ordinary_state_write() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let rpc_factory = factory.rpc_provider();
    let old_provider = rpc_factory.provider().unwrap();

    let mut write = factory.consistent_write();
    let next = append_static_block(factory, 1, genesis.hash());
    commit_block_metadata(factory, &next);
    assert_eq!(
        factory
            .database_provider_live_ro()
            .unwrap()
            .basic_account(&Address::ZERO)
            .unwrap()
            .unwrap()
            .balance,
        U256::from(2)
    );
    let (done, receive) = mpsc::channel();
    let reader = thread::spawn(move || {
        let provider = rpc_factory.provider().unwrap();
        done.send((
            provider.best_block_number().unwrap(),
            provider.basic_account(&Address::ZERO).unwrap().unwrap().balance,
        ))
        .unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(3));
    // Always release the barrier before checking the timeout so a regression cannot hang the test.
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
    drop(write);
    reader.join().unwrap();
    assert_eq!(result.unwrap(), (0, U256::from(1)));

    assert_eq!(old_provider.basic_account(&Address::ZERO).unwrap().unwrap().balance, U256::from(1));
    assert_eq!(rpc_factory_read_balance(factory), U256::from(2));
    assert_eq!(factory.provider().unwrap().best_block_number().unwrap(), 1);
}

fn rpc_factory_read_balance(factory: &TestFactory) -> U256 {
    factory
        .rpc_provider()
        .provider()
        .unwrap()
        .basic_account(&Address::ZERO)
        .unwrap()
        .unwrap()
        .balance
}

#[test]
fn published_rpc_static_file_reads_ignore_unpublished_append() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let old_provider = factory.rpc_provider().provider().unwrap();
    let mut write = factory.consistent_write();
    let next = append_static_block(factory, 1, genesis.hash());
    commit_block_metadata(factory, &next);

    let provider = factory.rpc_provider().provider().unwrap();
    for provider in [&old_provider, &provider] {
        assert_eq!(provider.last_block_number().unwrap(), 0);
        assert!(provider.header_by_number(1).unwrap().is_none());
        assert!(provider.sealed_header(1).unwrap().is_none());
        assert!(provider.block_hash(1).unwrap().is_none());
        assert_eq!(provider.headers_range(0..=1).unwrap().len(), 1);
        assert!(provider.headers_range(1..=2).unwrap().is_empty());
        assert_eq!(provider.headers_range(0..=u64::MAX).unwrap().len(), 1);
        assert!(provider.headers_range(2..=u64::MAX).unwrap().is_empty());
        assert_eq!(provider.canonical_hashes_range(0, 2).unwrap(), vec![genesis.hash()]);
        assert!(provider.transaction_by_id(1).unwrap().is_none());
        assert_eq!(provider.transactions_by_tx_range(0..=1).unwrap().len(), 1);
        assert!(provider.transactions_by_tx_range(1..=2).unwrap().is_empty());
        assert_eq!(provider.transactions_by_tx_range(0..=u64::MAX).unwrap().len(), 1);
        assert!(provider.transactions_by_tx_range(2..=u64::MAX).unwrap().is_empty());
        assert!(provider.receipt(1).unwrap().is_none());
        assert_eq!(provider.receipts_by_tx_range(0..=1).unwrap().len(), 1);
        assert!(provider.receipts_by_tx_range(1..=2).unwrap().is_empty());
        assert_eq!(provider.receipts_by_tx_range(0..=u64::MAX).unwrap().len(), 1);
        assert!(provider.receipts_by_tx_range(2..=u64::MAX).unwrap().is_empty());
    }
    assert!(factory.static_file_provider().header_by_number(1).unwrap().is_some());
    assert!(factory.static_file_provider().transaction_by_id(1).unwrap().is_some());
    assert!(factory.static_file_provider().receipt(1).unwrap().is_some());
    assert_eq!(factory.rpc_provider().headers_range(0..=1).unwrap().len(), 1);
    assert_eq!(factory.rpc_provider().transactions_by_tx_range(0..=1).unwrap().len(), 1);
    assert_eq!(factory.rpc_provider().receipts_by_tx_range(0..=1).unwrap().len(), 1);
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
}

fn add_memory_head(
    provider: &BlockchainProvider<MockNodeTypesWithDB<DatabaseEnv>>,
    number: u64,
    parent_hash: B256,
) {
    add_memory_head_with_outcome(provider, number, parent_hash, Default::default());
}

fn add_memory_head_with_outcome(
    provider: &BlockchainProvider<MockNodeTypesWithDB<DatabaseEnv>>,
    number: u64,
    parent_hash: B256,
    outcome: ExecutionOutcome<Receipt>,
) {
    let block = RecoveredBlock::new_unhashed(
        Block {
            header: Header { number, parent_hash, ..Default::default() },
            body: Default::default(),
        },
        Vec::new(),
    );
    provider.canonical_in_memory_state().update_chain(NewCanonicalChain::Commit {
        new: vec![ExecutedBlockWithTrieUpdates::new(
            Arc::new(block),
            Arc::new(outcome),
            Default::default(),
            ExecutedTrieUpdates::empty(),
            Default::default(),
        )],
    });
}

#[test]
fn published_rpc_changeset_reads_clip_static_file_append() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    factory.set_storage_settings_cache(GravityStorageSettings { changesets_in_static_files: true });
    let genesis = publish_genesis(factory);
    let mut write = factory.consistent_write();
    let next = append_static_block(factory, 1, genesis.hash());
    let manager = factory.static_file_provider();
    let mut accounts = manager.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();
    let mut storages = manager.latest_writer(StaticFileSegment::StorageChangeSets).unwrap();
    for block in 0..=1 {
        accounts
            .append_account_changeset(
                vec![AccountBeforeTx { address: Address::ZERO, info: None }],
                block,
            )
            .unwrap();
        storages
            .append_storage_changeset(
                vec![StorageBeforeTx {
                    address: Address::ZERO,
                    key: B256::ZERO,
                    value: U256::from(block),
                }],
                block,
            )
            .unwrap();
    }
    accounts.commit().unwrap();
    storages.commit().unwrap();
    drop(accounts);
    drop(storages);
    commit_block_metadata(factory, &next);
    let provider = factory.rpc_provider().provider().unwrap();
    assert!(provider.account_block_changeset(1).unwrap().is_empty());
    assert!(provider.storage_changeset(1).unwrap().is_empty());
    assert_eq!(provider.account_changesets_range(0..=1).unwrap().len(), 1);
    assert_eq!(provider.storage_changesets_range(0..=1).unwrap().len(), 1);
    assert!(provider.account_changesets_range(1..=2).unwrap().is_empty());
    assert!(provider.storage_changesets_range(1..=2).unwrap().is_empty());
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
}

#[test]
fn published_rpc_view_checks_in_memory_anchor_height_and_hash() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
    let original = Account { balance: U256::from(1), ..Default::default() };
    let changed = Account { balance: U256::from(2), ..Default::default() };
    let outcome = ExecutionOutcome {
        bundle: BundleState::new(
            [(Address::ZERO, Some(original.into()), Some(changed.into()), Default::default())],
            [vec![(Address::ZERO, Some(Some(original.into())), [])]],
            [],
        ),
        first_block: 1,
        ..Default::default()
    };
    add_memory_head_with_outcome(&provider, 1, genesis.hash(), outcome);
    let rpc_provider = provider.rpc_provider();
    assert_eq!(rpc_provider.consistent_provider().unwrap().best_block_number().unwrap(), 1);
    assert_eq!(
        rpc_provider.latest().unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(2))
    );
    assert_eq!(provider.database_provider_live_ro().unwrap().rpc_read_view_bounds(), None);
    assert!(rpc_provider.database_provider_live_ro().unwrap().rpc_read_view_bounds().is_some());

    for (number, parent_hash) in [(1, B256::repeat_byte(1)), (2, genesis.hash())] {
        let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
        add_memory_head(&provider, number, parent_hash);
        assert!(provider.consistent_provider().is_ok());
        assert!(provider.rpc_provider().consistent_provider().is_err());
    }
}

#[test]
fn published_rpc_captured_memory_survives_persistence_and_eviction() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
    let original = Account { balance: U256::from(1), ..Default::default() };
    let changed = Account { balance: U256::from(2), ..Default::default() };
    let outcome = ExecutionOutcome {
        bundle: BundleState::new(
            [(Address::ZERO, Some(original.into()), Some(changed.into()), Default::default())],
            [vec![(Address::ZERO, Some(Some(original.into())), [])]],
            [],
        ),
        first_block: 1,
        ..Default::default()
    };
    add_memory_head_with_outcome(&provider, 1, genesis.hash(), outcome);
    let captured = provider.rpc_provider().consistent_provider().unwrap();
    let memory_hash = captured.block_hash(1).unwrap().unwrap();
    let memory_header = captured.sealed_header(1).unwrap().unwrap();

    let mut write = factory.consistent_write();
    let next = append_static_block_with_root(factory, 1, genesis.hash(), memory_header.state_root);
    assert_eq!(next.hash(), memory_hash);
    commit_block_metadata(factory, &next);
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
    drop(write);
    provider.canonical_in_memory_state().remove_persisted_blocks(next.num_hash());
    assert!(provider.canonical_in_memory_state().head_state().is_none());

    assert_eq!(captured.block_hash(1).unwrap(), Some(memory_hash));
    let captured_state = captured.into_state_provider_at_block_hash(memory_hash).unwrap();
    assert_eq!(captured_state.account_balance(&Address::ZERO).unwrap(), Some(U256::from(2)));
    assert_eq!(
        provider.rpc_provider().latest().unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(2))
    );
}

fn set_pending_with_balance(
    provider: &BlockchainProvider<MockNodeTypesWithDB<DatabaseEnv>>,
    number: u64,
    parent_hash: B256,
) -> B256 {
    set_pending_state(provider, number, parent_hash, 9, 0)
}

fn set_pending_state(
    provider: &BlockchainProvider<MockNodeTypesWithDB<DatabaseEnv>>,
    number: u64,
    parent_hash: B256,
    balance: u64,
    timestamp: u64,
) -> B256 {
    let original = Account { balance: U256::from(1), ..Default::default() };
    let changed = Account { balance: U256::from(balance), ..Default::default() };
    let outcome = ExecutionOutcome {
        bundle: BundleState::new(
            [(Address::ZERO, Some(original.into()), Some(changed.into()), Default::default())],
            [vec![(Address::ZERO, Some(Some(original.into())), [])]],
            [],
        ),
        first_block: number,
        ..Default::default()
    };
    let block = RecoveredBlock::new_unhashed(
        Block {
            header: Header { number, parent_hash, timestamp, ..Default::default() },
            body: Default::default(),
        },
        Vec::new(),
    );
    let hash = block.hash();
    provider.canonical_in_memory_state().set_pending_block(ExecutedBlockWithTrieUpdates::new(
        Arc::new(block),
        Arc::new(outcome),
        Default::default(),
        ExecutedTrieUpdates::empty(),
        Default::default(),
    ));
    hash
}

fn assert_rpc_pending_filtered(
    provider: &BlockchainProvider<MockNodeTypesWithDB<DatabaseEnv>>,
    pending_hash: B256,
) {
    let rpc = provider.rpc_provider();
    let captured = rpc.consistent_provider().unwrap();
    assert!(!captured.has_rpc_pending_state());
    assert!(captured.pending_block_num_hash().unwrap().is_none());
    assert!(captured.pending_block().unwrap().is_none());
    assert!(captured.pending_block_and_receipts().unwrap().is_none());
    assert!(captured.header_by_number_or_tag(BlockNumberOrTag::Pending).unwrap().is_none());
    assert!(captured.sealed_header_by_number_or_tag(BlockNumberOrTag::Pending).unwrap().is_none());
    assert!(captured.find_block_by_hash(pending_hash, BlockSource::Pending).unwrap().is_none());
    assert!(captured.into_rpc_pending_state_provider(Some(pending_hash)).unwrap().is_none());
    assert!(rpc.pending_block_num_hash().unwrap().is_none());
    assert!(rpc.pending_block().unwrap().is_none());
    assert!(rpc.pending_block_and_receipts().unwrap().is_none());
    assert!(rpc.header_by_number_or_tag(BlockNumberOrTag::Pending).unwrap().is_none());
    assert!(rpc.find_block_by_hash(pending_hash, BlockSource::Pending).unwrap().is_none());
    assert!(rpc.pending_state_by_hash(pending_hash).unwrap().is_none());
    assert!(rpc.maybe_pending().unwrap().is_none());
    assert_eq!(
        rpc.pending().unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(1))
    );
    let fallback =
        rpc.consistent_provider().unwrap().into_rpc_pending_state_provider(None).unwrap();
    assert_eq!(fallback.unwrap().account_balance(&Address::ZERO).unwrap(), Some(U256::from(1)));
}

#[test]
fn published_rpc_pending_without_memory_head_must_extend_published_tip() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
    assert!(provider.canonical_in_memory_state().head_state().is_none());

    for (number, parent_hash) in [(1, B256::repeat_byte(1)), (2, genesis.hash())] {
        let pending_hash = set_pending_with_balance(&provider, number, parent_hash);
        assert!(provider.pending_block().unwrap().is_some());
        assert_rpc_pending_filtered(&provider, pending_hash);
    }

    let pending_hash = set_pending_with_balance(&provider, 1, genesis.hash());
    let rpc = provider.rpc_provider();
    let captured = rpc.consistent_provider().unwrap();
    assert!(captured.has_rpc_pending_state());
    assert_eq!(captured.pending_block_num_hash().unwrap().unwrap().hash, pending_hash);
    assert_eq!(rpc.pending_block_num_hash().unwrap().unwrap().hash, pending_hash);
    assert_eq!(rpc.pending_block().unwrap().unwrap().hash(), pending_hash);
    assert_eq!(rpc.pending_block_and_receipts().unwrap().unwrap().0.hash(), pending_hash);
    assert_eq!(rpc.header_by_number_or_tag(BlockNumberOrTag::Pending).unwrap().unwrap().number, 1);
    assert!(rpc.find_block_by_hash(pending_hash, BlockSource::Pending).unwrap().is_some());
    assert_eq!(
        rpc.pending().unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(9))
    );
    assert_eq!(
        rpc.pending_state_by_hash(pending_hash)
            .unwrap()
            .unwrap()
            .account_balance(&Address::ZERO)
            .unwrap(),
        Some(U256::from(9))
    );
    assert_eq!(
        rpc.maybe_pending().unwrap().unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(9))
    );
    let state = captured.into_rpc_pending_state_provider(Some(pending_hash)).unwrap().unwrap();
    assert_eq!(state.account_balance(&Address::ZERO).unwrap(), Some(U256::from(9)));
}

#[test]
fn published_rpc_pending_capture_keeps_its_head_and_filters_stale_parent() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
    add_memory_head(&provider, 1, genesis.hash());
    let parent_hash = provider.block_hash(1).unwrap().unwrap();
    let pending_hash = set_pending_with_balance(&provider, 2, parent_hash);
    let captured = provider.rpc_provider().consistent_provider().unwrap();

    add_memory_head(&provider, 2, parent_hash);
    set_pending_with_balance(&provider, 2, parent_hash);
    assert_rpc_pending_filtered(&provider, pending_hash);

    assert_eq!(captured.best_block_number().unwrap(), 1);
    assert_eq!(captured.pending_block_num_hash().unwrap().unwrap().hash, pending_hash);
    let pending = captured.into_rpc_pending_state_provider(Some(pending_hash)).unwrap().unwrap();
    assert_eq!(pending.account_balance(&Address::ZERO).unwrap(), Some(U256::from(9)));
}

#[test]
fn published_rpc_pending_state_hash_never_switches_to_replacement() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
    let first = set_pending_state(&provider, 1, genesis.hash(), 9, 0);
    let rpc = provider.rpc_provider();
    let captured = rpc.state_by_block_hash(first).unwrap();
    assert_eq!(captured.account_balance(&Address::ZERO).unwrap(), Some(U256::from(9)));
    assert_eq!(
        rpc.state_by_block_id(BlockId::hash(first))
            .unwrap()
            .account_balance(&Address::ZERO)
            .unwrap(),
        Some(U256::from(9))
    );
    assert!(matches!(
        rpc.state_by_block_id(BlockId::hash_canonical(first)),
        Err(crate::ProviderError::BlockHashNotFound(hash)) if hash == first
    ));
    // The generic RPC helper's hash lookup also retains legacy pending-provider support.
    assert_eq!(
        provider.state_by_block_hash(first).unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(9))
    );

    let replacement = set_pending_state(&provider, 1, genesis.hash(), 13, 1);
    assert_ne!(replacement, first);
    for state in [rpc.state_by_block_hash(first), rpc.state_by_block_id(BlockId::hash(first))] {
        assert!(matches!(state,
            Err(crate::ProviderError::BlockHashNotFound(hash)) if hash == first
        ));
    }
    assert_eq!(captured.account_balance(&Address::ZERO).unwrap(), Some(U256::from(9)));
    assert_eq!(
        rpc.state_by_block_hash(replacement).unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(13))
    );
    assert!(matches!(
        rpc.state_by_block_id(BlockId::hash_canonical(replacement)),
        Err(crate::ProviderError::BlockHashNotFound(hash)) if hash == replacement
    ));
}

#[test]
fn published_rpc_pending_state_hash_remains_valid_after_becoming_canonical() {
    let storage = TestStorage::new();
    let factory = &storage.factory;
    let genesis = publish_genesis(factory);
    let provider = BlockchainProvider::with_latest(factory.clone(), genesis.clone()).unwrap();
    let hash = set_pending_state(&provider, 1, genesis.hash(), 9, 0);
    let pending = provider.canonical_in_memory_state().pending_state().unwrap();
    let rpc = provider.rpc_provider();
    provider
        .canonical_in_memory_state()
        .update_chain(NewCanonicalChain::Commit { new: vec![pending.block_ref().clone()] });

    assert!(rpc.pending_block_num_hash().unwrap().is_none());
    for block_id in [BlockId::hash(hash), BlockId::hash_canonical(hash)] {
        assert_eq!(
            rpc.state_by_block_id(block_id).unwrap().account_balance(&Address::ZERO).unwrap(),
            Some(U256::from(9))
        );
    }
    assert_eq!(
        rpc.state_by_block_hash(hash).unwrap().account_balance(&Address::ZERO).unwrap(),
        Some(U256::from(9))
    );
}

#[test]
fn published_rpc_reopen_rejects_legacy_storage_only_merged_tail_until_replay() {
    use crate::TrieWriterV2;
    use alloy_primitives::keccak256;
    use reth_db::DatabaseEnvKind;
    use reth_trie::{
        nested_trie::{Node, NodeFlag, StorageNodeEntry},
        HashedPostState, Nibbles, StoredNibblesSubKey,
    };

    let write_account = |factory: &TestFactory, balance| {
        let provider = factory.provider_rw().unwrap();
        let mut hashed = HashedPostState::default();
        hashed.accounts.insert(
            keccak256(Address::ZERO),
            Some(Account { balance: U256::from(balance), ..Default::default() }),
        );
        let (root, updates) =
            reth_trie_db::nested_hash::NestedStateRoot::new(provider.tx_ref(), None)
                .calculate(&hashed)
                .unwrap();
        provider.write_trie_updatesv2(&updates).unwrap();
        provider.commit().unwrap();
        root
    };

    let TestStorage { factory, _db_dir, _static_dir } = TestStorage::new();
    let genesis_root = write_account(&factory, 1u64);
    let genesis = append_static_block_with_root(&factory, 0, B256::ZERO, genesis_root);
    commit_block_metadata(&factory, &genesis);
    factory.publish_rpc_read_view(genesis.num_hash()).unwrap();

    // The legacy first merged flush could leave only the storage shard ahead, with no
    // state checkpoint or body metadata recording the interrupted group's actual tail.
    let orphan = B256::repeat_byte(1);
    let tx = factory.db_ref().tx_mut().unwrap();
    tx.put::<tables::StoragesTrieV2>(
        orphan,
        StorageNodeEntry::new(
            StoredNibblesSubKey(Nibbles::new()),
            Node::ShortNode {
                key: Nibbles::unpack(B256::ZERO.as_slice()),
                value: Box::new(Node::ValueNode(vec![1])),
                flags: NodeFlag::default(),
            },
        ),
    )
    .unwrap();
    tx.commit().unwrap();
    let static_files = factory.static_file_provider();
    drop(Arc::try_unwrap(factory.into_db()).unwrap());

    let factory: TestFactory = ProviderFactory::new(
        Arc::new(
            DatabaseEnv::open(
                _db_dir.path(),
                DatabaseEnvKind::RW,
                DatabaseArguments::new(Default::default()),
            )
            .unwrap(),
        ),
        MAINNET.clone(),
        static_files,
    );
    let mut maintenance = factory.rpc_maintenance(true).unwrap();
    factory.publish_rpc_read_view(genesis.num_hash()).unwrap();
    maintenance.complete();
    drop(maintenance);

    let mut write = factory.consistent_write();
    let root = write_account(&factory, 2u64);
    let first = append_static_block_with_root(&factory, 1, genesis.hash(), root);
    commit_block_metadata(&factory, &first);
    factory.publish_rpc_read_view(first.num_hash()).unwrap();
    write.complete();
    drop(write);
    assert!(factory.rpc_provider().provider().is_err());
    assert_eq!(factory.provider().unwrap().best_block_number().unwrap(), 1);

    let mut write = factory.consistent_write();
    let tx = factory.db_ref().tx_mut().unwrap();
    tx.delete::<tables::StoragesTrieV2>(orphan, None).unwrap();
    tx.commit().unwrap();
    let root = write_account(&factory, 3u64);
    let next = append_static_block_with_root(&factory, 2, first.hash(), root);
    commit_block_metadata(&factory, &next);
    factory.publish_rpc_read_view(next.num_hash()).unwrap();
    write.complete();
    drop(write);
    assert_eq!(rpc_factory_read_balance(&factory), U256::from(3));
    assert!(!factory.rpc_read_view_requires_validation(2));
}
