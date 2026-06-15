//! Pre-execution transaction filtering.
//!
//! Mirrors the cheap pool-side guards that `OrderedBlock` txs skipped (nonce, balance,
//! cumulative gas, EIP-7702 intrinsic gas floor, EIP-7702 pre-Prague reject, EIP-4844 reject,
//! EIP-3860 init-code size) so non-pool-injected txs cannot reach grevm with a state that
//! would make the executor panic.

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
use revm_primitives::{eip3860::MAX_INITCODE_SIZE, hardfork::SpecId};
use tracing::info;

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
                info!(target: "filter_invalid_txs",
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
            info!(target: "filter_invalid_txs",
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
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                spec_id=?spec_id,
                "EIP-7702 tx in pre-Prague block"
            );
            return false;
        }
        // Gravity does not support EIP-4844. revm tx-level validation can reject a type-3
        // tx with `EmptyBlobs` / `BlobVersionNotSupported` / `TooManyBlobs` /
        // `BlobVersionedHashesNotSupported` / `BlobGasPriceGreaterThanMax` /
        // `MaxFeePerBlobGasNotSupported` / `BlobCreateTransaction`; any of these reaches the
        // executor as `EVMError` and panics. Drop the whole tx type here so a byzantine
        // proposer cannot reach grevm via this surface. Closes gravity-audit#696 trigger 2.
        if tx.is_eip4844() {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                "EIP-4844 blob tx rejected — unsupported on Gravity"
            );
            return false;
        }
        // EIP-3860 init-code cap. A Create tx with `input.len() > MAX_INITCODE_SIZE` is
        // rejected by revm with `CreateInitCodeSizeLimit` at tx-level validation, which the
        // executor cannot recover from. Gate it before grevm sees it. Closes
        // gravity-audit#696 trigger 4.
        if tx.is_create() && tx.input().len() > MAX_INITCODE_SIZE {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                init_code_size=?tx.input().len(),
                "init code exceeds EIP-3860 limit"
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
            info!(target: "filter_invalid_txs",
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
            info!(target: "filter_invalid_txs",
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
                info!(target: "filter_invalid_txs",
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
    use alloy_consensus::{TxEip4844, TxEip7702, TxLegacy};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::{Address, Bytes, Signature, TxKind, B256};
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

    /// Build a type-3 (EIP-4844) tx with the given nonce / gas_limit. Other fields default —
    /// the filter rejects the whole tx type, so the inner shape doesn't matter for the test.
    fn create_test_4844_transaction(nonce: u64, gas_limit: u64) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Eip4844(TxEip4844 {
                nonce,
                gas_limit,
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 0,
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    /// Build a Create tx with `input_size` bytes of init code. Uses a 1559 envelope so the
    /// intrinsic-gas math is the standard `21_000 + 32_000 + 16 * len + EIP-3860 word cost`.
    fn create_test_oversize_initcode_transaction(
        nonce: u64,
        gas_limit: u64,
        input_size: usize,
    ) -> TransactionSigned {
        use alloy_consensus::TxEip1559;
        TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                nonce,
                gas_limit,
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 0,
                to: TxKind::Create,
                input: Bytes::from(vec![0u8; input_size]),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    /// gravity-audit#696 trigger 2: Gravity does not support EIP-4844. Any type-3 tx —
    /// regardless of its blob_versioned_hashes shape — must be dropped by the filter so it
    /// never reaches grevm with a malformed blob payload that would panic the executor.
    #[test]
    fn test_filter_invalid_txs_eip4844_rejected() {
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

        // Default TxEip4844 has empty blob_versioned_hashes — revm would reject it with
        // `EmptyBlobs` at tx-level validation, panicking the executor without this filter.
        let tx = create_test_4844_transaction(0, 100_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(invalid_idxs.len(), 1, "type-3 (blob) tx must be discarded on Gravity");
        assert!(invalid_idxs.contains(&0));
    }

    /// gravity-audit#696 trigger 4: a Create tx with init code larger than
    /// `MAX_INITCODE_SIZE` (49152) is rejected by revm with `CreateInitCodeSizeLimit`,
    /// which the executor cannot recover from. The filter must drop it first.
    #[test]
    fn test_filter_invalid_txs_eip3860_oversized_init_code_rejected() {
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

        // 1 byte over the EIP-3860 cap; gas_limit deliberately high so the intrinsic check
        // wouldn't reject it — the size check is what must fire.
        let tx = create_test_oversize_initcode_transaction(0, 30_000_000, MAX_INITCODE_SIZE + 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(
            invalid_idxs.len(),
            1,
            "Create tx with init_code.len() > MAX_INITCODE_SIZE must be discarded"
        );
        assert!(invalid_idxs.contains(&0));
    }

    /// Boundary: a Create tx with `input.len() == MAX_INITCODE_SIZE` is within EIP-3860
    /// and must NOT be rejected by the size check. (The tx may still be rejected for other
    /// reasons — gas/balance — but not by the init-code-size gate.)
    #[test]
    fn test_filter_invalid_txs_eip3860_init_code_at_limit_not_rejected_by_size() {
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

        // gas_limit large enough to cover 21_000 + 32_000 + word-cost + calldata for 49152 zero
        // bytes (4 gas/byte). Block gas_limit raised in lockstep so the cumulative-gas truncation
        // check doesn't kick in first.
        let tx = create_test_oversize_initcode_transaction(0, 5_000_000, MAX_INITCODE_SIZE);
        let txs = vec![tx];
        let senders = vec![sender];

        let invalid_idxs =
            filter_invalid_txs(&db, &txs, &senders, 0, 10_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            invalid_idxs.is_empty(),
            "Create tx at exactly MAX_INITCODE_SIZE must pass the size gate: {invalid_idxs:?}"
        );
    }
}
