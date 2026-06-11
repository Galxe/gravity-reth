//! Pre-execution transaction filtering.
//!
//! Mirrors the cheap pool-side guards that `OrderedBlock` txs skipped (nonce, balance,
//! cumulative gas, EIP-7702 intrinsic gas floor) so non-pool-injected txs cannot reach grevm with
//! a state that would make the executor panic.

use alloy_consensus::Transaction;
use alloy_primitives::{
    map::{HashMap, HashSet},
    Address, U256,
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use reth_chainspec::ChainSpec;
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::ParallelDatabase;
use reth_evm_ethereum::revm_spec_by_timestamp_and_block_number;
use revm::state::AccountInfo;
use revm_primitives::hardfork::SpecId;
use tracing::warn;

/// Return the invalid transaction indexes.
///
/// `chain_spec`, `block_timestamp`, `block_number` are taken instead of a
/// pre-computed `SpecId` because the only consumer of `spec_id` here is the
/// intrinsic-gas / 7702 fork-gate logic, and the caller (`lib.rs`) does not
/// know which hardforks the filter *cares about* — historically it built a
/// truncated MERGE/SHANGHAI/PRAGUE ladder that mislabelled CANCUN-active
/// blocks as SHANGHAI. The canonical
/// `revm_spec_by_timestamp_and_block_number` helper is used so the same
/// (correct) mapping that the executor sees is the one the filter is gating
/// against — no risk of the two drifting apart on a future hardfork.
pub(crate) fn filter_invalid_txs<DB: ParallelDatabase>(
    db: DB,
    txs: &[TransactionSigned],
    senders: &[Address],
    base_fee_per_gas: u64,
    gas_limit: u64,
    chain_spec: &ChainSpec,
    block_timestamp: u64,
    block_number: u64,
) -> HashSet<usize> {
    let spec_id =
        revm_spec_by_timestamp_and_block_number(chain_spec, block_timestamp, block_number);
    let mut gas_limit_exceeded_tx_idx = txs.len();
    let mut tx_gas_limit_sum: u64 = 0;
    for (idx, tx) in txs.iter().enumerate() {
        let tx_gas_limit = tx.gas_limit();
        match tx_gas_limit_sum.checked_add(tx_gas_limit) {
            Some(new_sum) if new_sum <= gas_limit => {
                tx_gas_limit_sum = new_sum;
            }
            _ => {
                warn!(target: "filter_invalid_txs",
                    tx_hash=?txs[idx].hash(),
                    sender=?senders[idx],
                    block_gas_limit=?gas_limit,
                    "gas limit exceeded, truncated to {}",
                    idx,
                );
                gas_limit_exceeded_tx_idx = idx;
                break;
            }
        }
    }

    let mut sender_idx: HashMap<&Address, Vec<usize>> = HashMap::default();
    for (i, sender) in senders[..gas_limit_exceeded_tx_idx].iter().enumerate() {
        sender_idx.entry(sender).or_default().push(i);
    }

    let is_tx_valid = |tx: &TransactionSigned, sender: &Address, account: &mut AccountInfo| {
        if account.nonce != tx.nonce() {
            warn!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                nonce=?tx.nonce(),
                account_nonce=?account.nonce,
                "nonce mismatch"
            );
            return false;
        }
        // Pre-Prague safety. revm rejects any TxEip7702 with `Eip7702NotSupported`
        // when `spec_id < PRAGUE` (revm-handler validation.rs:181-182), but
        // `calculate_initial_tx_gas` below only adds the auth-list cost when
        // `spec_id >= PRAGUE` (revm-interpreter gas/calc.rs:417). Without this
        // guard a TxEip7702 with `gas_limit == 21000` in a pre-Prague OrderedBlock
        // would pass the intrinsic check, reach the executor, and panic
        // `lib.rs:1067-1073`. Closes the boundary called out in the acceptance
        // design as P-2.
        if tx.is_eip7702() && !spec_id.is_enabled_in(SpecId::PRAGUE) {
            warn!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                spec_id=?spec_id,
                "EIP-7702 tx in pre-Prague block"
            );
            return false;
        }
        // Mirror reth pool's `ensure_intrinsic_gas` so non-pool-injected txs (e.g. consensus-side
        // mempool) cannot reach grevm with `gas_limit < initial_gas` and panic the executor.
        let access_list = tx.access_list();
        let intrinsic = revm_interpreter::gas::calculate_initial_tx_gas(
            spec_id,
            tx.input(),
            tx.is_create(),
            access_list.map(|l| l.len()).unwrap_or_default() as u64,
            access_list
                .map(|l| l.iter().map(|i| i.storage_keys.len()).sum::<usize>())
                .unwrap_or_default() as u64,
            tx.authorization_list().map(|l| l.len()).unwrap_or_default() as u64,
        );
        if tx.gas_limit() < intrinsic.initial_gas || tx.gas_limit() < intrinsic.floor_gas {
            warn!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                gas_limit=?tx.gas_limit(),
                initial_gas=?intrinsic.initial_gas,
                floor_gas=?intrinsic.floor_gas,
                "intrinsic gas too low"
            );
            return false;
        }
        let gas_spent = U256::from(tx.effective_gas_price(Some(base_fee_per_gas)))
            .saturating_mul(U256::from(tx.gas_limit()));
        let total_spent = gas_spent.saturating_add(tx.value());
        if account.balance < total_spent {
            warn!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                balance=?account.balance,
                gas_spent=?gas_spent,
                transfer_value=?tx.value(),
                "insufficient balance"
            );
            return false;
        }
        account.balance -= total_spent;
        account.nonce += 1;
        true
    };

    let mut invalid_tx_idxs = sender_idx
        .into_par_iter()
        .flat_map(|(sender, idxs)| {
            if let Some(mut account) = db.basic_ref(*sender).unwrap() {
                idxs.into_iter()
                    .filter(|&idx| !is_tx_valid(&txs[idx], sender, &mut account))
                    .collect()
            } else {
                // Sender does not exist in the state trie, balance is 0
                warn!(target: "filter_invalid_txs",
                    tx_hash=?txs[idxs[0]].hash(),
                    sender=?sender,
                    "insufficient balance"
                );
                idxs
            }
        })
        .collect::<HashSet<_>>();
    invalid_tx_idxs.extend(gas_limit_exceeded_tx_idx..txs.len());
    invalid_tx_idxs
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{TxEip1559, TxEip7702, TxLegacy};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::{Address, Signature, TxKind, B256};
    use reth_chainspec::{ChainSpec, ChainSpecBuilder, MAINNET};
    use reth_ethereum_primitives::{Transaction, TransactionSigned};
    use reth_revm::state::{AccountInfo, Bytecode};
    use revm::{
        primitives::{StorageKey, StorageValue},
        DatabaseRef,
    };
    use std::{collections::HashMap as StdHashMap, sync::Arc};

    /// Chainspec with Prague active from genesis — the canonical test fixture for
    /// every case that wants the filter to apply 7702 intrinsic-gas rules.
    fn prague_chain_spec() -> Arc<ChainSpec> {
        Arc::new(ChainSpecBuilder::from(&*MAINNET).prague_activated().build())
    }

    /// Chainspec with Shanghai active from genesis but Prague unset — the
    /// `pre-Prague` test fixture. Used to pin the boundary where a TxEip7702
    /// must be discarded by the filter (revm would otherwise reject it with
    /// `Eip7702NotSupported` and panic the executor).
    fn shanghai_chain_spec() -> Arc<ChainSpec> {
        Arc::new(ChainSpecBuilder::from(&*MAINNET).shanghai_activated().build())
    }

    // Mock database for testing
    #[derive(Debug, Default)]
    struct MockDatabase {
        accounts: StdHashMap<Address, AccountInfo>,
    }

    impl MockDatabase {
        fn new() -> Self {
            Self::default()
        }

        fn insert_account(&mut self, address: Address, account: AccountInfo) {
            self.accounts.insert(address, account);
        }
    }

    impl DatabaseRef for MockDatabase {
        type Error = std::convert::Infallible;

        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(self.accounts.get(&address).cloned())
        }

        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            unreachable!()
        }

        fn storage_ref(
            &self,
            _address: Address,
            _index: StorageKey,
        ) -> Result<StorageValue, Self::Error> {
            unreachable!()
        }

        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            unreachable!()
        }
    }

    // Legacy `Default` is a contract-create with `to: TxKind::Create`, which under any spec has
    // initial intrinsic gas of 53000 — too high for the 21000-gas_limit fixtures these tests use.
    // Pin `to` to a Call so the legacy intrinsic is the flat 21000.
    fn create_test_transaction(nonce: u64, gas_limit: u64, gas_price: u128) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy {
                nonce,
                gas_price,
                gas_limit,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    fn create_test_transaction_with_value(
        nonce: u64,
        gas_limit: u64,
        gas_price: u128,
        value: U256,
    ) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy {
                nonce,
                gas_price,
                gas_limit,
                value,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    /// EIP-7702 type-0x04 tx with N authorizations; `gas_limit` caller controls.
    fn create_test_7702_transaction(
        nonce: u64,
        gas_limit: u64,
        authorizations: usize,
    ) -> TransactionSigned {
        let authorization_list = (0..authorizations)
            .map(|_| {
                SignedAuthorization::new_unchecked(
                    Authorization { chain_id: U256::ZERO, address: Address::ZERO, nonce: 0 },
                    0,
                    U256::ZERO,
                    U256::ZERO,
                )
            })
            .collect();
        TransactionSigned::new_unhashed(
            Transaction::Eip7702(TxEip7702 {
                nonce,
                gas_limit,
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 0,
                authorization_list,
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    #[test]
    fn test_filter_invalid_txs_empty_input() {
        let db = MockDatabase::new();
        let txs = vec![];
        let senders = vec![];
        let base_fee_per_gas = 20_000_000_000u64; // 20 gwei
        let gas_limit = 30_000_000u64; // 30M gas

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(invalid_idxs.is_empty());
    }

    #[test]
    fn test_filter_invalid_txs_account_not_exists() {
        let db = MockDatabase::new();
        let sender = Address::random();

        // create a transaction, but the account does not exist
        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(invalid_idxs.len(), 1);
        assert!(invalid_idxs.contains(&0));
    }

    #[test]
    fn test_filter_invalid_txs_nonce_mismatch() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // the account exists, but the nonce does not match
        let account = AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            nonce: 5,                                          // 账户 nonce 是 5
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender, account);

        // the transaction nonce is 0, but the account nonce is 5
        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(invalid_idxs.len(), 1);
        assert!(invalid_idxs.contains(&0));
    }

    #[test]
    fn test_filter_invalid_txs_insufficient_balance() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // the account has insufficient balance
        let account = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 0,
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender, account);

        // fee = gas_price * gas_limit + value = 25_000_000_000 * 21_000 + 0 =
        // 525_000_000_000_000
        let tx1 = create_test_transaction(0, 21_000, 25_000_000_000);
        let tx2 = create_test_transaction_with_value(0, 21_000, 1_000, U256::from(500_000_000u64));
        let tx3 = create_test_transaction_with_value(0, 21_000, 1_000, U256::from(500_000_000u64));
        let txs = vec![tx1, tx2, tx3];
        let senders = vec![sender, sender, sender];
        let base_fee_per_gas = 1_000;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(invalid_idxs.len(), 2);
        assert!(invalid_idxs.contains(&0));
        assert!(invalid_idxs.contains(&2));
    }

    #[test]
    fn test_filter_invalid_txs_gas_limit_exceeded() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // the account has enough balance
        let account = AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            nonce: 0,
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender, account);

        // create multiple transactions, the cumulative gas limit exceeds the block limit
        let tx1 = create_test_transaction(0, 20_000_000, 25_000_000_000); // 20M gas
        let tx2 = create_test_transaction(1, 20_000_000, 25_000_000_000); // 20M gas
        let txs = vec![tx1, tx2];
        let senders = vec![sender, sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64; // 30M gas limit

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(invalid_idxs.len(), 1);
        assert!(invalid_idxs.contains(&1));
    }

    #[test]
    fn test_filter_invalid_txs_valid_transactions() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // 账户有足够余额
        let account = AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            nonce: 0,
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender, account);

        // create valid transactions
        let tx1 = create_test_transaction(0, 21_000, 25_000_000_000);
        let tx2 = create_test_transaction(1, 21_000, 25_000_000_000);
        let txs = vec![tx1, tx2];
        let senders = vec![sender, sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(invalid_idxs.is_empty());
    }

    #[test]
    fn test_filter_invalid_txs_mixed_scenarios() {
        let mut db = MockDatabase::new();
        let sender1 = Address::random();
        let sender2 = Address::random();
        let sender3 = Address::random();

        let account1 = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 0,
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender1, account1);

        let account2 = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 5,
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender2, account2);

        let account3 = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 0,
            code_hash: B256::default(),
            code: None,
        };
        db.insert_account(sender3, account3);

        // create mixed scenarios transactions
        let tx1 = create_test_transaction(0, 21_000, 25); // sender1: valid
        let tx2 = create_test_transaction(0, 21_000, 25); // sender1: nonce does not match
        let tx3 = create_test_transaction(1, 21_000, 25_000_000); // sender1: insufficient balance
        let tx4 = create_test_transaction(5, 21_000, 25); // sender2: valid
        let tx5 = create_test_transaction(2, 21_000, 25); // sender1: nonce does not match
        let tx6 = create_test_transaction(6, 30_000_000, 25); // sender2: gas limit exceeds
        let tx7 = create_test_transaction(0, 21000, 25); // sender3: truncated
        let txs = vec![tx1, tx2, tx3, tx4, tx5, tx6, tx7];
        let senders = vec![sender1, sender1, sender1, sender2, sender2, sender2, sender3];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(invalid_idxs.len(), 5, "invalid_idxs: {invalid_idxs:?}");
        assert!(invalid_idxs.contains(&1));
        assert!(invalid_idxs.contains(&2));
        assert!(invalid_idxs.contains(&4));
        assert!(invalid_idxs.contains(&5));
        assert!(invalid_idxs.contains(&6));
    }

    /// Regression: a type-0x04 tx whose `gas_limit` is below `21000 + PER_EMPTY_ACCOUNT_COST * N`
    /// must be discarded at the filter stage. Before this fix it would reach grevm and panic the
    /// executor with `IntrinsicGasTooLow` (see gravity-audit issue #668).
    #[test]
    fn test_filter_invalid_txs_eip7702_intrinsic_gas_too_low_under_prague() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH — balance is fine
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        // 21000 (base) + 25000 (PER_EMPTY_ACCOUNT_COST) = 46000 is the Prague intrinsic floor for
        // a 7702 tx with one authorization and no calldata; gas_limit 30_000 is below it.
        let tx = create_test_7702_transaction(0, 30_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 0;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(invalid_idxs.len(), 1, "intrinsic-gas-too-low 7702 tx should be discarded");
        assert!(invalid_idxs.contains(&0));
    }

    /// Sanity check: same 7702 tx with a `gas_limit` at or above the floor passes the filter.
    #[test]
    fn test_filter_invalid_txs_eip7702_intrinsic_gas_just_enough_under_prague() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        // exactly 21000 + 25000 = 46000 — at the floor, should pass
        let tx = create_test_7702_transaction(0, 46_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 0;
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(invalid_idxs.is_empty(), "got: {invalid_idxs:?}");
    }

    /// U-1 (acceptance design §3.1): a 7702 tx with `authorization_list.len() == 2` and
    /// `gas_limit = 21000 + 25000 * 2 + 1000 = 72000` passes the filter under Prague.
    #[test]
    fn test_filter_invalid_txs_eip7702_two_auths_gas_sufficient() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let tx = create_test_7702_transaction(0, 72_000, 2);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "two-auth 7702 tx with 72k gas must pass: {invalid_idxs:?}"
        );
    }

    /// U-2 (acceptance design §3.1): a 7702 tx with three authorizations and
    /// `gas_limit = 21_000` is discarded by the filter rather than reaching the executor.
    #[test]
    fn test_filter_invalid_txs_eip7702_three_auths_gas_too_low() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let tx = create_test_7702_transaction(0, 21_000, 3);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(invalid_idxs.len(), 1, "three-auth 7702 tx at 21k gas must be discarded");
        assert!(invalid_idxs.contains(&0));
    }

    /// Pre-Prague boundary (acceptance design P-2): a TxEip7702 with otherwise-fine intrinsic
    /// gas (`gas_limit > 21_000` so the SHANGHAI calculator that ignores `auth_list_num` would
    /// accept it) must still be discarded when `spec_id < PRAGUE`, because the executor would
    /// otherwise reject the tx with `Eip7702NotSupported` and panic `lib.rs:1067-1073`.
    #[test]
    fn test_filter_invalid_txs_eip7702_rejected_pre_prague() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        // 100k gas is well above the pre-Prague intrinsic (21k flat, since the
        // auth-list cost is gated behind PRAGUE). Without the pre-Prague guard
        // this tx would pass the filter and panic the executor.
        let tx = create_test_7702_transaction(0, 100_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &shanghai_chain_spec(), 0, 0);
        assert_eq!(invalid_idxs.len(), 1, "7702 tx must be discarded when spec_id < PRAGUE");
        assert!(invalid_idxs.contains(&0));
    }

    /// U-3 (acceptance design §3.1): the #668 fix must not regress legacy/1559 filtering.
    /// A non-7702 tx with `authorization_list == None` and a 21k gas limit must still
    /// pass the filter under Prague (the auth-list count contribution to intrinsic is 0).
    #[test]
    fn test_filter_invalid_txs_non_eip7702_under_prague_still_passes() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "legacy 21k-gas tx must not be regressed by the 7702 intrinsic fix: {invalid_idxs:?}"
        );
    }

    /// EIP-1559 helper with explicit fee caps so the sub-base-fee and priority>max
    /// cases can be exercised independently of the legacy `gas_price` path.
    /// Shared by the `feecap-below-floor` and `missing-basefee-and-priorityfee` repros.
    fn create_test_1559_transaction(
        nonce: u64,
        gas_limit: u64,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    ) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                nonce,
                gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    // ----------------------------------------------------------------------
    // REPRO: feecap-below-floor-grevm-panic-halt (severity: high)
    //
    // BUG: When the Gravity base-fee floor is active, `next_block_base_fee`
    // (crates/chainspec/src/api.rs:95-106) clamps the block base fee UP to
    // `gravity_min_base_fee`. The executor then runs grevm with revm's base-fee
    // check live (`disable_base_fee` is never set in `next_evm_env`), so any tx
    // with `max_fee_per_gas < base_fee_per_gas` is rejected by revm with
    // `GasPriceLessThanBasefee`. grevm maps that to a hard `Err`, and the block
    // executor calls `executor.execute(&block).unwrap_or_else(|err| panic!(...))`
    // at lib.rs:1060-1066 -> deterministic chain halt on every validator.
    //
    // The ONLY guard that strips such txs out of an OrderedBlock before grevm is
    // `filter_invalid_txs` (this file). It checks nonce, EIP-7702/pre-Prague,
    // intrinsic gas, and balance -- but it NEVER compares
    // `max_fee_per_gas >= base_fee_per_gas`. The balance check uses
    // `tx.effective_gas_price(Some(base_fee))`, which for an underpaying 1559 tx
    // returns the smaller `max_fee_per_gas`, so a well-funded sender's underpaying
    // tx passes the balance check too.
    //
    // This test ASSERTS the correct behavior and FAILS on current HEAD: the filter
    // returns an empty set, so the underpaying tx reaches grevm -> panic.
    // ----------------------------------------------------------------------
    #[test]
    fn repro_feecap_below_floor_grevm_panic_halt() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // Plenty of balance: 100 ETH. The bug is NOT a balance problem; balance
        // must be generously sufficient even at the (higher) floor base fee.
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(100u128) * U256::from(1_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        // Active floor / block base fee = 50 Gwei (the released-genesis floor).
        let base_fee_per_gas: u64 = 50_000_000_000;

        // Underpaying EIP-1559 tx: feeCap = 10 Gwei < 50 Gwei floor.
        let underpaying = create_test_1559_transaction(
            0,
            21_000,
            10_000_000_000, // max_fee_per_gas = 10 Gwei (BELOW floor)
            1_000_000_000,  // max_priority_fee_per_gas = 1 Gwei
        );
        let txs = vec![underpaying];
        let senders = vec![sender];
        let gas_limit = 30_000_000u64;

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );

        // Sanity-check that the balance guard genuinely does NOT catch the
        // underpayment: effective_gas_price for this 1559 tx collapses to the
        // (smaller) max_fee_per_gas, not the base fee.
        let effective =
            alloy_consensus::Transaction::effective_gas_price(&txs[0], Some(base_fee_per_gas));
        assert_eq!(
            effective, 10_000_000_000u128,
            "effective_gas_price must collapse to the (sub-floor) max_fee_per_gas, \
             demonstrating the balance check cannot detect the underpayment"
        );

        // CORRECT behavior: the underpaying tx is invalid and must be stripped
        // before grevm. FAILS on buggy HEAD because there is no
        // `max_fee_per_gas >= base_fee` guard.
        assert!(
            invalid_idxs.contains(&0),
            "BUG (feecap-below-floor-grevm-panic-halt): a tx with max_fee_per_gas (10 Gwei) \
             below the active base-fee floor (50 Gwei) must be filtered out before grevm, \
             but filter_invalid_txs returned {invalid_idxs:?} (empty). On HEAD this tx reaches \
             grevm, revm rejects it with GasPriceLessThanBasefee, and lib.rs:1060-1066 panics \
             the executor -> deterministic chain halt across all validators."
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: filter-missing-basefee-and-priorityfee-halt (severity: high)
    //
    // `is_tx_valid` validates nonce, EIP-7702 pre-Prague gating, intrinsic/floor
    // gas, and balance -- but NEVER checks effective gas price >= base_fee, nor
    // `max_priority_fee_per_gas <= max_fee_per_gas`. revm rejects such txs
    // (GasPriceLessThanBasefee / PriorityFeeGreaterThanMaxFee); when one survives
    // this filter and reaches grevm, the whole block aborts and lib.rs:1060-1067
    // panics every validator -> chain halt. The base-fee floor (api.rs:96-106)
    // makes this reachable: a floor-activation jump can lift the base fee above a
    // tx's fee cap that was valid when admitted.
    // ----------------------------------------------------------------------

    /// REPRO 1 (legacy / floor-jump path): legacy `gas_price` (1 gwei) below the
    /// block base fee (100-gwei floor). Ample balance + matching nonce, so every
    /// existing check passes. The filter MUST flag it invalid; buggy HEAD does not.
    #[test]
    fn repro_filter_legacy_gas_price_below_base_fee_is_not_rejected() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(10_000_000_000_000_000_000u128), // 10 ETH — balance is fine
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let base_fee_per_gas = 100_000_000_000u64; // 100 gwei (gravity_min_base_fee floor)
        let tx = create_test_transaction(0, 21_000, 1_000_000_000); // 1 gwei < 100 gwei base fee
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );

        assert_eq!(
            invalid_idxs.len(),
            1,
            "legacy tx with gas_price below base fee must be filtered out (revm rejects it with \
             GasPriceLessThanBasefee, which aborts the whole block and panics the executor); \
             got {invalid_idxs:?}"
        );
        assert!(invalid_idxs.contains(&0));
    }

    /// REPRO 2 (EIP-1559 sub-base-fee): `max_fee_per_gas` (50 gwei) below the block
    /// base fee (100-gwei floor). revm rejects with GasPriceLessThanBasefee.
    #[test]
    fn repro_filter_1559_max_fee_below_base_fee_is_not_rejected() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(10_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let base_fee_per_gas = 100_000_000_000u64; // 100 gwei floor
        let tx = create_test_1559_transaction(0, 21_000, 50_000_000_000, 0);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );

        assert_eq!(
            invalid_idxs.len(),
            1,
            "1559 tx with max_fee_per_gas below base fee must be filtered out; got {invalid_idxs:?}"
        );
        assert!(invalid_idxs.contains(&0));
    }

    /// REPRO 3 (priority > max): `max_priority_fee_per_gas` (200 gwei) >
    /// `max_fee_per_gas` (150 gwei), with max_fee above the base fee so the
    /// sub-base-fee guard alone would not catch it. revm rejects with
    /// PriorityFeeGreaterThanMaxFee.
    #[test]
    fn repro_filter_1559_priority_fee_greater_than_max_fee_is_not_rejected() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(10_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let base_fee_per_gas = 100_000_000_000u64; // 100 gwei floor; max_fee is above it
        let tx = create_test_1559_transaction(0, 21_000, 150_000_000_000, 200_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );

        assert_eq!(
            invalid_idxs.len(),
            1,
            "1559 tx with max_priority_fee_per_gas > max_fee_per_gas must be filtered out (revm \
             rejects it with PriorityFeeGreaterThanMaxFee, aborting the block); got {invalid_idxs:?}"
        );
        assert!(invalid_idxs.contains(&0));
    }

    /// Control: a well-priced tx (gas_price == base_fee) MUST still pass. Guards
    /// against an over-broad fix and confirms the repro isolates the fee check.
    #[test]
    fn repro_filter_well_priced_tx_still_passes() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(10_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let base_fee_per_gas = 100_000_000_000u64;
        let tx = create_test_transaction(0, 21_000, 100_000_000_000); // gas_price == base fee
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(invalid_idxs.is_empty(), "tx priced at the base fee must pass: {invalid_idxs:?}");
    }

    // ======================================================================
    // TRACK A1 — DIFFERENTIAL ORACLE: filter_invalid_txs vs revm pre-execution
    // validation (Galxe/revm @ bfa048c, crates/handler/src/validation.rs +
    // pre_execution.rs).
    //
    // The cluster builds the executor's CfgEnv via `EthEvmConfig::next_evm_env`
    // (crates/ethereum/evm/src/lib.rs:196-220) with a plain `CfgEnv::new()` —
    // NONE of revm's `disable_*` cfg flags are set (disable_base_fee=false,
    // disable_nonce_check=false, disable_balance_check=false,
    // disable_block_gas_limit=false, is_eip3607_disabled=false,
    // is_priority_fee_check_disabled=false). So EVERY rejection branch in
    // `validate_tx_env` / `validate_against_state_and_deduct_caller` is LIVE in
    // the executor. grevm maps any such rejection to a hard `Err`, and
    // lib.rs:1060 `executor.execute(&block).unwrap_or_else(|err| panic!(...))`
    // halts every validator.
    //
    // `filter_invalid_txs` is the ONLY pre-grevm guard. Each helper below
    // constructs a tx that revm REJECTS; the assertion that the filter ALSO
    // rejects it is the oracle. A FAILING assertion = a confirmed divergence
    // where the filter is more permissive than revm = a chain-halt bug.
    //
    // Run:
    //   cd /mnt/data2/kenji/galxe/gravity-sdk-dev/external/gravity-reth && \
    //   cargo test -p reth-pipe-exec-layer-ext-v2 --lib \
    //     tx_filter::tests::oracle_ -- --nocapture
    // ======================================================================

    /// A funded EOA fixture: 100 ETH, nonce 0, no code.
    fn oracle_funded_sender(db: &mut MockDatabase) -> Address {
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(100u128) * U256::from(1_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );
        sender
    }

    /// CREATE legacy tx carrying `init_code_len` bytes of init-code, priced at
    /// `gas_price`, with a generous gas_limit so the *only* differentiator is the
    /// EIP-3860 init-code-size limit.
    fn oracle_create_tx(nonce: u64, init_code_len: usize, gas_price: u128) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy {
                nonce,
                gas_price,
                // Huge gas_limit so intrinsic-gas never fires; isolate the 3860 check.
                gas_limit: 200_000_000,
                to: TxKind::Create,
                input: alloy_primitives::Bytes::from(vec![0u8; init_code_len]),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    // ----------------------------------------------------------------------
    // ORACLE 1: EIP-3860 init-code size limit (revm: CreateInitCodeSizeLimit,
    // validation.rs:210-215). Active whenever spec >= SHANGHAI for CREATE txs
    // whose init-code exceeds `max_initcode_size()` (= 49152 by default).
    //
    // The filter NEVER inspects `tx.input().len()` against any init-code limit.
    // A CREATE tx with >49152 bytes of init-code, well-funded and correct nonce,
    // passes every filter guard -> reaches grevm -> revm rejects with
    // CreateInitCodeSizeLimit -> block aborts -> lib.rs:1060 panic -> halt.
    // ----------------------------------------------------------------------
    #[test]
    fn oracle_filter_vs_revm_initcode_size_over_eip3860_limit() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);

        // 49153 = eip3860::MAX_INITCODE_SIZE + 1. revm rejects this; the floor
        // intrinsic gas for this calldata is well under the 200M gas_limit.
        let tx = oracle_create_tx(0, 49_153, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 200_000_000, &prague_chain_spec(), 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "DIVERGENCE (initcode-size > EIP-3860 limit): revm rejects this CREATE tx with \
             CreateInitCodeSizeLimit (validation.rs:210-215, max_initcode_size=49152), but \
             filter_invalid_txs returned {invalid_idxs:?}. The tx reaches grevm and panics the \
             executor at lib.rs:1060 -> deterministic chain halt."
        );
    }

    /// Control for ORACLE 1: a CREATE tx exactly at the 49152-byte limit must
    /// pass (revm accepts it) — isolates the divergence to the over-limit case.
    #[test]
    fn oracle_filter_vs_revm_initcode_size_at_eip3860_limit_passes() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);

        let tx = oracle_create_tx(0, 49_152, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 200_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "CREATE tx at the 49152-byte init-code limit must pass: {invalid_idxs:?}"
        );
    }

    // ----------------------------------------------------------------------
    // ORACLE 2: EIP-3607 — sender has deployed (non-delegated) code
    // (revm: RejectCallerWithCode, pre_execution.rs:80-90). `is_eip3607_disabled`
    // is false in the executor cfg, so revm rejects any tx whose sender account
    // carries non-empty, non-7702 bytecode.
    //
    // The filter only reads `account.nonce` and `account.balance`; it never looks
    // at `account.code`/`code_hash`. A tx from a contract-coded address with the
    // right nonce and balance passes the filter -> grevm -> revm rejects with
    // RejectCallerWithCode -> panic -> halt. Reachable because consensus-ordered
    // txs are NOT pool-validated and a sender address can become contract-coded
    // (e.g. CREATE2 collision target, or a forged OrderedBlock entry).
    // ----------------------------------------------------------------------
    #[test]
    fn oracle_filter_vs_revm_sender_with_code_eip3607() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        // Sender account carries real (non-7702) bytecode.
        let code = Bytecode::new_raw(alloy_primitives::Bytes::from(vec![0x60, 0x00, 0x60, 0x00]));
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(100u128) * U256::from(1_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: code.hash_slow(),
                code: Some(code),
            },
        );

        let tx = create_test_transaction(0, 21_000, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "DIVERGENCE (EIP-3607 sender-has-code): revm rejects a tx whose sender carries \
             non-delegated bytecode with RejectCallerWithCode (pre_execution.rs:80-90), but \
             filter_invalid_txs ignores account.code entirely and returned {invalid_idxs:?}. \
             The tx reaches grevm and panics the executor at lib.rs:1060 -> chain halt."
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: txfilter-missing-eip3607 (severity: medium)
    //
    // BUG: revm enforces EIP-3607 in pre-execution validation: a tx whose
    // sender (caller) account carries non-empty code that is NOT an EIP-7702
    // delegation designator is rejected with
    // `InvalidTransaction::RejectCallerWithCode` (Galxe/revm @ bfa048c,
    // crates/handler/src/pre_execution.rs:80-89, gated only by
    // `is_eip3607_disabled`). The executor's CfgEnv leaves
    // `disable_eip3607 = false` -- it is set to true ONLY on RPC call/estimate
    // paths (crates/rpc/rpc-eth-api/src/helpers/call.rs, estimate.rs), never on
    // the grevm/block path built by `next_evm_env`
    // (crates/ethereum/evm/src/lib.rs) -- so EIP-3607 is LIVE during execution.
    //
    // `filter_invalid_txs` (this file, l.71-140) reads only `account.nonce` and
    // `account.balance`; it NEVER inspects `account.code` / `code_hash`. A
    // funded, correctly-nonced sender that happens to carry real (non-7702)
    // bytecode therefore passes every filter guard, reaches grevm, is rejected
    // by revm with `RejectCallerWithCode`, the block aborts, and lib.rs:1060
    // `executor.execute(&block).unwrap_or_else(|err| panic!(...))` halts every
    // validator -> deterministic chain halt.
    //
    // Distinct from the other tx_filter findings (fee floor, priority>max,
    // empty-7702-authlist, LackOfFundForMaxFee, init-code size, chain-id): the
    // ONLY divergence here is the missing sender-code (EIP-3607) check.
    //
    // Severity medium because reachability requires a sender that is both
    // code-bearing AND the recovered signer of an admitted OrderedBlock tx
    // (e.g. a genesis-allocated account given code without clearing its key, a
    // CREATE2-collision target, or a forged/byzantine-proposer OrderedBlock
    // sender) -- consensus-ordered txs bypass the pool's EIP-3607 guard, which
    // is exactly the gap this filter is meant to mirror.
    //
    // This test ASSERTS the correct (revm-equivalent) behavior -- the filter
    // must reject index 0 -- and therefore FAILS on current HEAD, where the
    // filter ignores account.code and lets the code-bearing sender's tx through.
    // ----------------------------------------------------------------------
    #[test]
    fn repro_txfilter_missing_eip3607() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // Sender account carries real, NON-delegated bytecode (PUSH1 0 PUSH1 0).
        // This is genuine contract code, NOT an EIP-7702 `0xef0100 || address`
        // delegation designator, so revm's RejectCallerWithCode branch
        // (pre_execution.rs:87 `!bytecode.is_empty() && !bytecode.is_eip7702()`)
        // fires.
        let code = Bytecode::new_raw(alloy_primitives::Bytes::from(vec![0x60, 0x00, 0x60, 0x00]));
        // Fixture sanity: the code is non-empty and is NOT a 7702 delegation, so
        // revm WILL reject this caller (the divergence is real, not a 7702 EOA).
        assert!(!code.is_empty(), "fixture sanity: sender code must be non-empty");
        assert!(
            !code.is_eip7702(),
            "fixture sanity: sender code must NOT be a 7702 delegation designator (else revm \
             would allow it and there would be no divergence)"
        );
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(100u128) * U256::from(1_000_000_000_000_000_000u128),
                nonce: 0, // matches the tx nonce -> nonce guard passes
                code_hash: code.hash_slow(),
                code: Some(code),
            },
        );

        // Otherwise-perfectly-valid legacy tx: correct nonce 0, 21k intrinsic
        // floor, base_fee 0 so neither the intrinsic-gas nor the balance guard
        // can fire. The sender's code is the ONLY thing revm would reject on.
        let tx = create_test_transaction(0, 21_000, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "BUG (txfilter-missing-eip3607): revm rejects a tx whose sender carries non-empty, \
             non-delegated bytecode with RejectCallerWithCode (pre_execution.rs:80-89, \
             is_eip3607_disabled=false on the block path), but filter_invalid_txs reads only \
             account.nonce/account.balance and never account.code, so it returned \
             {invalid_idxs:?} (admits the tx). The code-bearing sender's tx reaches grevm, the \
             block aborts, and lib.rs:1060 panics the executor -> deterministic chain halt \
             across all validators."
        );
    }

    /// Control for the repro: a sender whose code IS a valid EIP-7702 delegation
    /// designator (`0xef0100 || address`) must still be allowed to originate a tx
    /// -- revm explicitly exempts delegated EOAs (pre_execution.rs:85-88,
    /// `!bytecode.is_eip7702()`), so the filter must NOT reject it. Isolates the
    /// divergence to the real-code case and guards against an over-broad fix that
    /// would break 7702-delegated senders.
    #[test]
    fn repro_txfilter_eip7702_delegated_sender_passes() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // 0xef0100 || 20-byte address = a valid 7702 delegation designator.
        let mut raw = vec![0xef, 0x01, 0x00];
        raw.extend_from_slice(Address::random().as_slice());
        let code = Bytecode::new_raw(alloy_primitives::Bytes::from(raw));
        assert!(
            code.is_eip7702(),
            "fixture sanity: control code must be a valid 7702 delegation designator"
        );
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(100u128) * U256::from(1_000_000_000_000_000_000u128),
                nonce: 0,
                code_hash: code.hash_slow(),
                code: Some(code),
            },
        );

        let tx = create_test_transaction(0, 21_000, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "a sender with a valid EIP-7702 delegation designator must be allowed to originate a \
             tx (revm exempts delegated EOAs from RejectCallerWithCode): {invalid_idxs:?}"
        );
    }

    // ----------------------------------------------------------------------
    // ORACLE 3: LackOfFundForMaxFee — balance covers EFFECTIVE fee but not
    // MAX fee (revm: pre_execution.rs:135-145).
    //
    // revm's balance gate uses `max_balance_spending = gas_limit * max_fee_per_gas
    // + value` (transaction.rs:165-180). The filter instead uses
    // `effective_gas_price(base_fee) * gas_limit + value` (this file, l.123-125),
    // which for an EIP-1559 tx collapses to `min(max_fee, base_fee + max_priority)`
    // — STRICTLY SMALLER than max_fee whenever max_fee > base_fee + priority.
    //
    // Construct a 1559 tx with a high max_fee_per_gas but tiny priority fee, and
    // fund the sender for the effective cost but NOT the max cost. The filter
    // passes it; revm rejects with LackOfFundForMaxFee -> grevm abort -> panic.
    // ----------------------------------------------------------------------
    #[test]
    fn oracle_filter_vs_revm_balance_covers_effective_not_max_fee() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        let gas_limit = 21_000u64;
        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let max_priority: u128 = 0;
        let max_fee: u128 = 1_000_000_000_000; // 1000 gwei — far above base fee

        // effective_gas_price = min(max_fee, base_fee + priority) = base_fee = 1 gwei.
        // effective cost = 21000 * 1 gwei = 21_000 gwei.
        // max cost (revm)  = 21000 * 1000 gwei = 21_000_000 gwei.
        // Fund the sender for the effective cost + a little slack, but well below max.
        let effective_cost = U256::from(gas_limit as u128 * base_fee as u128);
        db.insert_account(
            sender,
            AccountInfo {
                balance: effective_cost + U256::from(1_000u128), // covers effective, NOT max
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let tx = create_test_1559_transaction(0, gas_limit, max_fee, max_priority);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, base_fee, 30_000_000, &prague_chain_spec(), 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "DIVERGENCE (LackOfFundForMaxFee): revm requires balance >= gas_limit*max_fee_per_gas \
             + value (pre_execution.rs:135-145, max_balance_spending), but filter_invalid_txs only \
             charges effective_gas_price(base_fee)*gas_limit+value (l.123-125), which is strictly \
             smaller for this 1559 tx (1 gwei vs 1000 gwei). The under-funded tx passes the filter \
             ({invalid_idxs:?}), reaches grevm, and panics the executor at lib.rs:1060 -> chain halt."
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: txfilter-lack-of-fund-for-max-fee (severity: high)
    //
    // BUG: The filter's caller-balance gate (this file, l.123-125) computes
    //   gas_spent = effective_gas_price(Some(base_fee)) * gas_limit
    //             = min(max_fee, base_fee + max_priority) * gas_limit
    // and rejects the tx only if `balance < gas_spent + value`. revm's
    // pre-execution gate (Galxe/revm @ bfa048c, crates/handler/src/pre_execution.rs
    // :135-145) instead requires
    //   balance >= max_balance_spending = gas_limit * max_fee_per_gas + value
    // (crates/context/interface/src/transaction.rs:165-179), returning
    // `LackOfFundForMaxFee` otherwise. effective_gas_price for an EIP-1559 tx is
    // `min(max_fee, base_fee + priority)` (alloy-consensus eip1559.rs), which is
    // STRICTLY SMALLER than `max_fee` whenever `max_fee > base_fee + priority`.
    //
    // So a sender funded for the EFFECTIVE cost but NOT the MAX cost passes the
    // filter, reaches grevm, is rejected by revm with `LackOfFundForMaxFee`, and
    // panics the executor at lib.rs:1060 `executor.execute(&block).unwrap_or_else(
    // |err| panic!(...))` -> deterministic, consensus-ordered chain halt on cheap
    // attacker input.
    //
    // This is distinct from the round-1 fee-floor items (FeeCapTooLow /
    // GasPriceLessThanBasefee) and the priority>max-fee item: here max_fee is FAR
    // ABOVE the base fee and priority is well-formed, so every fee-validity guard
    // is satisfied — the ONLY divergence is the balance gate charging effective
    // rather than max fee.
    //
    // The test ASSERTS the correct (revm-equivalent) behavior — the filter must
    // reject index 0 — and therefore FAILS on current HEAD, where the filter only
    // charges the effective fee and lets the under-funded tx through.
    // ----------------------------------------------------------------------
    #[test]
    fn repro_txfilter_lack_of_fund_for_max_fee() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        let gas_limit = 21_000u64;
        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let max_priority: u128 = 0;
        let max_fee: u128 = 1_000_000_000_000; // 1000 gwei — far above the base fee

        // effective_gas_price = min(max_fee, base_fee + priority) = base_fee = 1 gwei
        //   effective cost = 21000 * 1 gwei              = 21_000 gwei
        //   max cost (revm) = 21000 * 1000 gwei          = 21_000_000 gwei
        // Fund the sender for the effective cost plus a sliver of slack, but far
        // below the max cost revm actually requires.
        let effective_cost = U256::from(gas_limit as u128 * base_fee as u128);
        let max_cost = U256::from(gas_limit as u128 * max_fee);
        let balance = effective_cost + U256::from(1_000u128);
        assert!(
            balance < max_cost,
            "fixture sanity: balance must be below revm's max_balance_spending"
        );
        db.insert_account(
            sender,
            AccountInfo {
                balance, // covers the effective fee, NOT the max fee
                nonce: 0,
                code_hash: B256::default(),
                code: None,
            },
        );

        let tx = create_test_1559_transaction(0, gas_limit, max_fee, max_priority);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );

        assert!(
            invalid_idxs.contains(&0),
            "DIVERGENCE (LackOfFundForMaxFee): revm requires balance >= \
             gas_limit*max_fee_per_gas + value (pre_execution.rs:135-145, \
             max_balance_spending), but filter_invalid_txs (tx_filter.rs:123-125) only \
             charges effective_gas_price(base_fee)*gas_limit + value, which for this 1559 \
             tx is 1 gwei*21000 vs revm's 1000 gwei*21000. The under-funded tx passes the \
             filter ({invalid_idxs:?}), reaches grevm, and panics the executor at \
             lib.rs:1060 -> deterministic chain halt."
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: txfilter-effective-vs-maxfee-balance-underdeduct (severity: high)
    //
    // BUG (multi-tx-per-sender running-balance under-deduction): the filter's
    // per-sender loop (this file, l.142-148) processes a sender's txs in order and,
    // for each one that passes, debits the cached balance by
    //   total_spent = effective_gas_price(Some(base_fee)) * gas_limit + value
    //               = min(max_fee, base_fee + max_priority) * gas_limit + value
    // (l.123-125, l.137 `account.balance -= total_spent`). revm instead reserves
    // `max_balance_spending = gas_limit * max_fee_per_gas + value` UPFRONT for EACH
    // tx (Galxe/revm @ bfa048c, transaction.rs:165-179, pre_execution.rs:135-145),
    // rejecting with `LackOfFundForMaxFee` when the running balance is short.
    //
    // Because effective price (here = base_fee) is STRICTLY SMALLER than max_fee,
    // the filter's running balance is debited far too little per tx, so it stays
    // artificially high and ADMITS a trailing tx that revm's per-tx max-fee gate
    // rejects. The trailing tx reaches grevm, the block aborts, and lib.rs:1060
    // `executor.execute(&block).unwrap_or_else(|err| panic!(...))` halts every
    // validator -> deterministic, consensus-ordered chain halt.
    //
    // This is the distinguishing trigger (b) of the finding: it does NOT depend on
    // a balance drop between pool admission and execution (trigger (a), covered by
    // the single-tx oracle/repro above). It fires purely from the filter charging
    // effective*gas instead of max_fee*gas as the per-tx running debit, even when
    // tx0 ALONE comfortably passes both the filter and revm. Each tx is well-priced
    // (max_fee >> base_fee, priority well-formed), so every fee-validity guard is
    // satisfied — the ONLY divergence is the running balance reservation.
    //
    // FIXTURE (base_fee = 1 gwei, priority = 0, max_fee = 1000 gwei, gas_limit =
    // 21000, value = 0):
    //   effective per tx = 21000 * 1 gwei     = 21_000 gwei
    //   max per tx (revm) = 21000 * 1000 gwei = 21_000_000 gwei
    //   balance = max_per_tx + (effective_per_tx + slack)
    // -> revm: tx0 reserves max_per_tx (fits, balance left ~= effective+slack <
    //    max_per_tx), tx1 needs another max_per_tx and is REJECTED (LackOfFundForMaxFee).
    // -> filter: tx0 debits only effective_per_tx, leaving ~= max_per_tx + slack,
    //    so tx1's effective_per_tx debit still fits and tx1 PASSES the filter.
    //
    // The test ASSERTS the correct (revm-equivalent) behavior — the filter MUST
    // reject the trailing tx (index 1) — and therefore FAILS on current HEAD, where
    // the filter's effective-fee running debit lets the under-funded tx1 through.
    // ----------------------------------------------------------------------
    #[test]
    fn repro_txfilter_effective_vs_maxfee_balance_underdeduct() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        let gas_limit = 21_000u64;
        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let max_priority: u128 = 0;
        let max_fee: u128 = 1_000_000_000_000; // 1000 gwei — far above the base fee

        // effective_gas_price = min(max_fee, base_fee + priority) = base_fee = 1 gwei
        let effective_per_tx = U256::from(gas_limit as u128 * base_fee as u128);
        let max_per_tx = U256::from(gas_limit as u128 * max_fee);

        // Fund the sender for ONE tx at the revm max cost plus a second tx at only
        // the (much smaller) effective cost. revm reserves max_per_tx for EACH tx,
        // so after tx0 the remaining balance (~= effective_per_tx + slack) is far
        // below the max_per_tx it needs for tx1 -> LackOfFundForMaxFee on tx1. The
        // filter, debiting only effective_per_tx for tx0, keeps a running balance
        // ~= max_per_tx + slack and so admits tx1.
        let slack = U256::from(1_000u128);
        let balance = max_per_tx + effective_per_tx + slack;

        // Fixture sanity: tx0 is affordable under BOTH the filter and revm; the
        // bug is isolated to the trailing tx1.
        assert!(
            balance >= max_per_tx,
            "fixture sanity: tx0 must be affordable under revm's per-tx max reservation"
        );
        // Fixture sanity: after revm reserves max_per_tx for tx0, the remainder is
        // below the max_per_tx required for tx1 (so revm rejects tx1)...
        assert!(
            balance - max_per_tx < max_per_tx,
            "fixture sanity: tx1 must be UNaffordable under revm's per-tx max reservation"
        );
        // ...but it is still >= the effective cost the filter charges for tx1 (so
        // the buggy filter admits tx1).
        assert!(
            balance - effective_per_tx >= effective_per_tx,
            "fixture sanity: tx1 must be affordable under the filter's effective-fee debit"
        );

        db.insert_account(
            sender,
            AccountInfo { balance, nonce: 0, code_hash: B256::default(), code: None },
        );

        // Two well-priced 1559 txs from the same sender, consecutive nonces.
        let tx0 = create_test_1559_transaction(0, gas_limit, max_fee, max_priority);
        let tx1 = create_test_1559_transaction(1, gas_limit, max_fee, max_priority);
        let txs = vec![tx0, tx1];
        let senders = vec![sender, sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );

        // tx0 must pass (it is affordable under both gates) — guards against an
        // over-broad assertion that would also flag the legitimately-affordable tx.
        assert!(
            !invalid_idxs.contains(&0),
            "fixture sanity: tx0 is affordable under both the filter and revm and must pass; \
             got {invalid_idxs:?}"
        );

        // tx1 MUST be rejected: revm reserves gas_limit*max_fee_per_gas per tx and
        // rejects tx1 with LackOfFundForMaxFee, but the filter only deducts the
        // effective fee for tx0 and so lets tx1 through. This assertion FAILS on
        // HEAD, proving the under-deduction lets the executor-rejected tx into the
        // block -> grevm abort -> lib.rs:1060 panic -> deterministic chain halt.
        assert!(
            invalid_idxs.contains(&1),
            "BUG (txfilter-effective-vs-maxfee-balance-underdeduct): with two same-sender 1559 \
             txs, revm reserves gas_limit*max_fee_per_gas ({max_per_tx}) per tx upfront and \
             rejects the trailing tx with LackOfFundForMaxFee (pre_execution.rs:135-145), but \
             filter_invalid_txs debits only effective_gas_price(base_fee)*gas_limit \
             ({effective_per_tx}) per tx (tx_filter.rs:123-125,137), so its running balance \
             stays artificially high and admits tx1. invalid_idxs={invalid_idxs:?}. The \
             under-funded tx1 reaches grevm, the block aborts, and lib.rs:1060 panics the \
             executor -> deterministic chain halt across all validators."
        );
    }

    /// Control for the repro: with the SAME two same-sender txs but a balance that
    /// covers revm's per-tx max reservation for BOTH (2 * max_per_tx), neither tx
    /// is under-funded under revm, so the filter must accept both. Isolates the
    /// divergence to the under-funded-trailing-tx case and guards against an
    /// over-broad fix that would reject legitimately-affordable same-sender txs.
    #[test]
    fn repro_txfilter_effective_vs_maxfee_two_txs_fully_funded_passes() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        let gas_limit = 21_000u64;
        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let max_priority: u128 = 0;
        let max_fee: u128 = 1_000_000_000_000; // 1000 gwei

        let max_per_tx = U256::from(gas_limit as u128 * max_fee);
        // Fund for BOTH txs at the full revm max reservation, plus slack.
        let balance = max_per_tx * U256::from(2u8) + U256::from(1_000u128);

        db.insert_account(
            sender,
            AccountInfo { balance, nonce: 0, code_hash: B256::default(), code: None },
        );

        let tx0 = create_test_1559_transaction(0, gas_limit, max_fee, max_priority);
        let tx1 = create_test_1559_transaction(1, gas_limit, max_fee, max_priority);
        let txs = vec![tx0, tx1];
        let senders = vec![sender, sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );

        assert!(
            invalid_idxs.is_empty(),
            "two same-sender 1559 txs funded for the full per-tx max reservation must both pass: \
             {invalid_idxs:?}"
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: txfilter-missing-initcode-size (severity: high)
    //
    // BUG: For Shanghai+ revm rejects a CREATE tx whose init-code length exceeds
    // `cfg.max_initcode_size()` (default eip3860::MAX_INITCODE_SIZE = 49152) with
    // `InvalidTransaction::CreateInitCodeSizeLimit` (Galxe/revm @ bfa048c,
    // crates/handler/src/validation.rs:209-215). This branch is unconditional in
    // the executor cfg built by `next_evm_env` (no `disable_*` flag gates it), so
    // it is LIVE.
    //
    // `filter_invalid_txs` (this file, l.71-140) validates nonce, the EIP-7702 /
    // pre-Prague gate, intrinsic/floor gas, and balance -- but NEVER inspects
    // `tx.input().len()` against any init-code limit. A well-funded, correct-nonce
    // CREATE tx carrying >49152 bytes of init-code therefore passes every filter
    // guard, reaches grevm, is rejected by revm with `CreateInitCodeSizeLimit`,
    // the block aborts, and lib.rs:1060 `executor.execute(&block).unwrap_or_else(
    // |err| panic!(...))` halts every validator -> deterministic, consensus-ordered
    // chain halt on cheap attacker input.
    //
    // Distinct from the round-1 fee/authlist findings and from the EIP-3607 /
    // LackOfFundForMaxFee / chain-id oracles: the ONLY divergence here is the
    // missing EIP-3860 init-code-size check.
    //
    // This test ASSERTS the correct (revm-equivalent) behavior — the filter must
    // reject index 0 — and therefore FAILS on current HEAD, where the filter never
    // looks at init-code size and lets the oversized CREATE tx through.
    // ----------------------------------------------------------------------
    #[test]
    fn repro_txfilter_missing_initcode_size() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);

        // 49153 = eip3860::MAX_INITCODE_SIZE + 1. revm rejects this with
        // CreateInitCodeSizeLimit. gas_limit is 200M and base_fee 0 so neither the
        // intrinsic-gas nor the balance guard can fire — the init-code-size check
        // is the only thing that distinguishes this tx from a valid one.
        let over_limit = revm_primitives::eip3860::MAX_INITCODE_SIZE + 1;
        let tx = oracle_create_tx(0, over_limit, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 200_000_000, &prague_chain_spec(), 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "BUG (txfilter-missing-initcode-size): revm rejects this Shanghai+ CREATE tx whose \
             init-code is {over_limit} bytes (> max_initcode_size = {}) with \
             CreateInitCodeSizeLimit (validation.rs:209-215), but filter_invalid_txs never \
             inspects tx.input().len() and returned {invalid_idxs:?}. The oversized CREATE tx \
             reaches grevm, the block aborts, and lib.rs:1060 panics the executor -> \
             deterministic chain halt across all validators.",
            revm_primitives::eip3860::MAX_INITCODE_SIZE,
        );
    }

    /// Control for the repro: a CREATE tx exactly at the 49152-byte init-code
    /// limit must pass (revm accepts it) — isolates the divergence to the
    /// strictly-over-limit case and guards against an over-broad fix.
    #[test]
    fn repro_txfilter_initcode_size_at_limit_passes() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);

        let at_limit = revm_primitives::eip3860::MAX_INITCODE_SIZE;
        let tx = oracle_create_tx(0, at_limit, 1_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 200_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "CREATE tx at the {at_limit}-byte init-code limit must pass: {invalid_idxs:?}"
        );
    }

    // ----------------------------------------------------------------------
    // ORACLE 4: chain-id mismatch (revm: InvalidChainId, validation.rs:105-114).
    // `tx_chain_id_check()` defaults to ON. A typed tx (1559/2930/7702/4844) whose
    // chain_id != cfg.chain_id is rejected.
    //
    // The filter never compares tx.chain_id() to the chainspec chain id. A 1559 tx
    // with a wrong chain_id, correctly funded/nonce'd, passes the filter -> grevm
    // -> revm rejects with InvalidChainId -> panic -> halt.
    // ----------------------------------------------------------------------
    #[test]
    fn oracle_filter_vs_revm_chain_id_mismatch() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);

        // MAINNET chain id is 1; set a bogus chain id on the tx.
        let tx = TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                chain_id: 999_999,
                nonce: 0,
                gas_limit: 21_000,
                max_fee_per_gas: 1_000_000_000,
                max_priority_fee_per_gas: 0,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        );
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "DIVERGENCE (InvalidChainId): revm rejects a typed tx whose chain_id != cfg.chain_id \
             (validation.rs:105-114, tx_chain_id_check defaults ON; MAINNET=1, tx=999999), but \
             filter_invalid_txs never checks chain_id and returned {invalid_idxs:?}. The tx reaches \
             grevm and panics the executor at lib.rs:1060 -> chain halt."
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: txfilter-missing-chainid (severity: high)
    //
    // BUG: revm's `tx_chain_id_check()` defaults to ON (Galxe/revm @ bfa048c,
    // crates/context/src/cfg.rs:150 sets `tx_chain_id_check: true`). For any
    // typed tx (EIP-1559/2930/7702/4844) that carries `Some(chain_id)`, revm's
    // pre-execution validation rejects it with
    // `InvalidTransaction::InvalidChainId` when `tx.chain_id() != cfg.chain_id()`
    // (crates/handler/src/validation.rs:104-108). The executor cfg gets the real
    // chain id via `CfgEnv::new().with_chain_id(self.chain_spec().chain().id())`
    // (crates/ethereum/evm/src/lib.rs:210/...), and no `disable_*` flag gates the
    // chain-id branch, so it is LIVE.
    //
    // `filter_invalid_txs` (this file, l.71-140) validates nonce, the EIP-7702 /
    // pre-Prague gate, intrinsic/floor gas, and balance -- but NEVER compares
    // `tx.chain_id()` against the chainspec chain id. A correctly-funded,
    // correct-nonce typed tx carrying a bogus chain_id therefore passes every
    // filter guard, reaches grevm, is rejected by revm with `InvalidChainId`,
    // the block aborts, and lib.rs:1060 `executor.execute(&block).unwrap_or_else(
    // |err| panic!(...))` halts every validator -> deterministic, consensus-
    // ordered chain halt from a single cheap-to-craft tx.
    //
    // Distinct from the other tx_filter findings (fee floor, priority>max,
    // empty-7702-authlist, LackOfFundForMaxFee, init-code size): the ONLY
    // divergence here is the missing chain_id comparison. Legacy txs may omit
    // chain_id and are exempt in revm, so this repro uses an EIP-1559 typed tx.
    //
    // This test ASSERTS the correct (revm-equivalent) behavior -- the filter must
    // reject index 0 -- and therefore FAILS on current HEAD, where the filter
    // never inspects chain_id and lets the wrong-chain-id tx through.
    // ----------------------------------------------------------------------
    #[test]
    fn repro_txfilter_missing_chainid() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);

        // MAINNET (the base for prague_chain_spec) has chain id 1. Pin a bogus
        // chain id on an otherwise-perfectly-valid EIP-1559 tx: correct nonce 0,
        // gas_limit at the 21k intrinsic floor, fee == base_fee (0), funded sender.
        const BOGUS_CHAIN_ID: u64 = 999_999;
        let spec = prague_chain_spec();
        assert_ne!(
            spec.chain().id(),
            BOGUS_CHAIN_ID,
            "fixture sanity: the bogus tx chain id must differ from the chainspec chain id"
        );

        let tx = TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                chain_id: BOGUS_CHAIN_ID,
                nonce: 0,
                gas_limit: 21_000,
                max_fee_per_gas: 1_000_000_000,
                max_priority_fee_per_gas: 0,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        );
        // Confirm the tx really carries a typed chain_id (so revm's check applies
        // and the legacy-omitted-chain_id exemption does NOT).
        assert_eq!(
            alloy_consensus::Transaction::chain_id(&tx),
            Some(BOGUS_CHAIN_ID),
            "fixture sanity: typed 1559 tx must carry Some(chain_id)"
        );

        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &spec, 0, 0);

        assert!(
            invalid_idxs.contains(&0),
            "BUG (txfilter-missing-chainid): revm rejects this typed (EIP-1559) tx whose chain_id \
             ({BOGUS_CHAIN_ID}) != chainspec chain id ({}) with InvalidChainId \
             (validation.rs:104-108, tx_chain_id_check defaults ON at cfg.rs:150), but \
             filter_invalid_txs never compares tx.chain_id() and returned {invalid_idxs:?}. The \
             wrong-chain-id tx reaches grevm, the block aborts, and lib.rs:1060 panics the \
             executor -> deterministic chain halt across all validators.",
            spec.chain().id(),
        );
    }

    /// Control for the repro: the SAME 1559 tx with the CORRECT chain id (matching
    /// the chainspec) must pass the filter (revm accepts it) -- isolates the
    /// divergence to the chain-id-mismatch case and guards against an over-broad
    /// fix that would reject well-formed typed txs.
    #[test]
    fn repro_txfilter_correct_chainid_passes() {
        let mut db = MockDatabase::new();
        let sender = oracle_funded_sender(&mut db);
        let spec = prague_chain_spec();

        let tx = TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                chain_id: spec.chain().id(), // correct chain id
                nonce: 0,
                gas_limit: 21_000,
                max_fee_per_gas: 1_000_000_000,
                max_priority_fee_per_gas: 0,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        );
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &spec, 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "a typed 1559 tx with the correct chain id must pass: {invalid_idxs:?}"
        );
    }

    // ----------------------------------------------------------------------
    // REPRO: txfilter-missing-blob-eip4844-validation (severity: medium)
    //
    // BUG: `filter_invalid_txs` (this file, l.71-140) has ZERO EIP-4844 blob
    // handling. Its caller-balance gate (l.123-125) charges only
    //   gas_spent = effective_gas_price(Some(base_fee)) * gas_limit + value
    // It NEVER adds the blob data fee
    //   data_fee = total_blob_gas * max_fee_per_blob_gas
    //            = (blob_versioned_hashes.len() * DATA_GAS_PER_BLOB) * max_fee_per_blob_gas
    // to the required balance. revm's `max_balance_spending`
    // (Galxe/revm @ bfa048c, crates/context/interface/src/transaction.rs:165-180)
    // unconditionally adds `calc_max_data_fee = max_fee_per_blob_gas * total_blob_gas`
    // for an Eip4844 tx, and pre_execution.rs:135-145 rejects the caller with
    // `LackOfFundForMaxFee` when the balance is short.
    //
    // So a sender funded for the regular (gas+value) cost but NOT the blob data
    // fee passes this filter, reaches grevm, is rejected by revm with
    // `LackOfFundForMaxFee`, the block aborts, and lib.rs:1060
    // `executor.execute(&block).unwrap_or_else(|err| panic!(...))` halts every
    // validator -> deterministic, consensus-ordered chain halt. The attacker
    // controls `max_fee_per_blob_gas`, so the uncharged gap can be made
    // arbitrarily large (here 1000 gwei per blob-gas over a 131072-blob-gas blob).
    //
    // Reachability: cancunTime=0 in the released genesis (Cancun active at
    // genesis) and the pool builder defaults eip4844=true, so blob txs enter the
    // mempool, get gossiped, and land in `OrderedBlock.transactions` as
    // `TransactionSigned` -> tx_filter -> grevm. Consensus-ordered txs are NOT
    // pool-revalidated (the exact gap this filter is meant to mirror); the halt
    // window is the admission->execution balance-change gap (a prior tx in the
    // same block spends the sender's balance, or a peer-injected blob tx the
    // local pool never validated). This fixture models that post-admission state
    // directly via the account balance.
    //
    // Distinct from every round-1 / earlier-round tx_filter finding (fee floor,
    // priority>max, empty-7702-authlist, LackOfFundForMaxFee on the *regular*
    // 1559 max-fee, init-code size, chain-id, EIP-3607): the ONLY divergence here
    // is the missing EIP-4844 blob data-fee in the balance requirement.
    //
    // This test ASSERTS the correct (revm-equivalent) behavior -- the filter must
    // reject index 0 -- and therefore FAILS on current HEAD, where the filter has
    // no blob handling and admits the under-funded blob tx.
    // ----------------------------------------------------------------------

    /// EIP-4844 blob tx with `n_blobs` versioned hashes (each correctly prefixed
    /// with `VERSIONED_HASH_VERSION_KZG = 0x01` so revm's `BlobVersionNotSupported`
    /// branch does NOT fire -- isolating the divergence to the data-fee balance
    /// gap). `max_fee_per_gas` is the regular fee cap, `max_fee_per_blob_gas` the
    /// blob fee cap. `to` is a plain Call so there is no init-code involvement.
    fn create_test_4844_transaction(
        chain_id: u64,
        nonce: u64,
        gas_limit: u64,
        max_fee_per_gas: u128,
        max_fee_per_blob_gas: u128,
        n_blobs: usize,
    ) -> TransactionSigned {
        let blob_versioned_hashes = (0..n_blobs)
            .map(|i| {
                // 0x01 || arbitrary-but-distinct 31 bytes => a well-versioned hash.
                let mut h = [0u8; 32];
                h[0] = alloy_eips::eip4844::VERSIONED_HASH_VERSION_KZG;
                h[31] = i as u8;
                B256::from(h)
            })
            .collect::<Vec<_>>();
        TransactionSigned::new_unhashed(
            Transaction::Eip4844(alloy_consensus::TxEip4844 {
                chain_id,
                nonce,
                gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas: 0,
                to: Address::ZERO,
                value: U256::ZERO,
                access_list: Default::default(),
                blob_versioned_hashes,
                max_fee_per_blob_gas,
                input: Default::default(),
            }),
            Signature::test_signature(),
        )
    }

    #[test]
    fn repro_txfilter_missing_blob_eip4844_validation() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        let spec = prague_chain_spec(); // Prague => Cancun active => blob txs valid form

        let gas_limit = 21_000u64;
        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let max_fee_per_gas: u128 = 1_000_000_000; // 1 gwei == base_fee (regular fee is exactly met)
        // Attacker-controlled blob fee cap, far above the block blob gas price
        // (which is ~1 wei because the builder hardcodes excess_blob_gas=0).
        let max_fee_per_blob_gas: u128 = 1_000_000_000_000; // 1000 gwei

        // One blob: total_blob_gas = 1 * DATA_GAS_PER_BLOB = 131072.
        let n_blobs = 1usize;
        let total_blob_gas =
            U256::from(n_blobs as u64) * U256::from(alloy_eips::eip4844::DATA_GAS_PER_BLOB);

        // Regular cost the filter DOES charge: effective_gas_price*gas_limit + value.
        // effective_gas_price = min(max_fee, base_fee + priority) = base_fee here.
        let regular_cost = U256::from(gas_limit as u128) * U256::from(base_fee as u128);
        // Blob data fee revm ADDITIONALLY requires (the filter charges 0 of this):
        //   data_fee = total_blob_gas * max_fee_per_blob_gas.
        let data_fee = total_blob_gas * U256::from(max_fee_per_blob_gas);

        // Fund the sender for the regular cost (plus a sliver of slack) but NOT
        // the blob data fee. Under the filter's blob-blind balance gate this is
        // sufficient; under revm's max_balance_spending it is short by ~data_fee.
        let balance = regular_cost + U256::from(1_000u128);
        assert!(
            balance >= regular_cost,
            "fixture sanity: balance must cover the regular gas cost the filter charges"
        );
        assert!(
            balance < regular_cost + data_fee,
            "fixture sanity: balance must be BELOW revm's required regular+blob-data fee \
             (data_fee={data_fee})"
        );

        db.insert_account(
            sender,
            AccountInfo { balance, nonce: 0, code_hash: B256::default(), code: None },
        );

        let tx = create_test_4844_transaction(
            spec.chain().id(),
            0,
            gas_limit,
            max_fee_per_gas,
            max_fee_per_blob_gas,
            n_blobs,
        );
        // Fixture sanity: this really is a blob tx carrying one versioned hash.
        assert!(tx.is_eip4844(), "fixture sanity: tx must be EIP-4844");
        assert_eq!(
            alloy_consensus::Transaction::blob_versioned_hashes(&tx).map(|h| h.len()),
            Some(1),
            "fixture sanity: tx must carry exactly one blob versioned hash"
        );

        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee,
            30_000_000,
            &spec,
            0,
            0,
        );

        assert!(
            invalid_idxs.contains(&0),
            "BUG (txfilter-missing-blob-eip4844-validation): revm requires balance >= \
             gas_limit*max_fee_per_gas + value + total_blob_gas*max_fee_per_blob_gas \
             (transaction.rs:165-180 calc_max_data_fee; pre_execution.rs:135-145 \
             LackOfFundForMaxFee), i.e. an extra blob data_fee of {data_fee} wei, but \
             filter_invalid_txs has NO EIP-4844 handling and charges only \
             effective_gas_price(base_fee)*gas_limit + value (tx_filter.rs:123-125). The \
             under-funded blob tx passes the filter ({invalid_idxs:?}), reaches grevm, is \
             rejected by revm with LackOfFundForMaxFee, the block aborts, and lib.rs:1060 \
             panics the executor -> deterministic chain halt across all validators."
        );
    }

    /// Control for the repro: the SAME blob tx with a balance that fully covers
    /// BOTH the regular cost AND the blob data fee must pass the filter (revm
    /// accepts it). Isolates the divergence to the under-funded-blob-fee case and
    /// guards against an over-broad fix that would reject well-funded blob txs.
    #[test]
    fn repro_txfilter_blob_eip4844_fully_funded_passes() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        let spec = prague_chain_spec();

        let gas_limit = 21_000u64;
        let base_fee: u64 = 1_000_000_000; // 1 gwei
        let max_fee_per_gas: u128 = 1_000_000_000;
        let max_fee_per_blob_gas: u128 = 1_000_000_000_000; // 1000 gwei
        let n_blobs = 1usize;

        let total_blob_gas =
            U256::from(n_blobs as u64) * U256::from(alloy_eips::eip4844::DATA_GAS_PER_BLOB);
        let regular_cost = U256::from(gas_limit as u128) * U256::from(base_fee as u128);
        let data_fee = total_blob_gas * U256::from(max_fee_per_blob_gas);
        // Fund for regular + blob data fee, plus slack.
        let balance = regular_cost + data_fee + U256::from(1_000u128);

        db.insert_account(
            sender,
            AccountInfo { balance, nonce: 0, code_hash: B256::default(), code: None },
        );

        let tx = create_test_4844_transaction(
            spec.chain().id(),
            0,
            gas_limit,
            max_fee_per_gas,
            max_fee_per_blob_gas,
            n_blobs,
        );
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee,
            30_000_000,
            &spec,
            0,
            0,
        );
        assert!(
            invalid_idxs.is_empty(),
            "a blob tx funded for both the regular cost and the blob data fee must pass: \
             {invalid_idxs:?}"
        );
    }
}
