//! Pre-execution transaction filtering — Gravity's canonical tx-admission gate.
//!
//! In Gravity's consensus-orders-before-execution model the executor cannot recover from
//! a per-tx `revm::InvalidTransaction` error (see `lib.rs:1060` panic site), and "drop
//! the tx and continue" is not implementable without protocol-level coordination
//! (would change the state transition function). The only viable defense is to make
//! the executor's `EVMError` path unreachable by gating every `InvalidTransaction`
//! variant here, before grevm sees the tx.
//!
//! This file is therefore intentionally a **superset** of revm's tx-level validation
//! (`revm-handler/src/validation.rs::validate_tx_env` +
//! `pre_execution.rs::validate_account_nonce_and_code` +
//! `validate_against_state_and_deduct_caller`). Each guard below maps to a specific
//! `InvalidTransaction::*` variant and cites the audit issue that motivated it.
//!
//! ## Variants pinned as unreachable on revm 10.0.1
//!
//! The following `InvalidTransaction::*` variants exist in the enum (kept for
//! ABI/back-compat with downstream `match` arms) but have **no `return Err(...)`
//! construction site** anywhere in `revm-handler-10.0.1` /
//! `revm-context-interface-10.2.0` — the legacy validation paths that raised them
//! were removed upstream. revm cannot return them at runtime regardless of input on
//! this version pin, so the filter deliberately does NOT gate them:
//!
//! - `AccessListNotSupported` — legacy `env.rs` validation site removed.
//! - `MaxFeePerBlobGasNotSupported` / `BlobVersionedHashesNotSupported` / `BlobCreateTransaction` —
//!   removed; 4844 type rejected wholesale below regardless.
//! - `AuthorizationListNotSupported` — type-gating now flows through `Eip7702NotSupported`, which
//!   the pre-Prague guard below pre-empts.
//! - `AuthorizationListInvalidFields` — validation moved to 7702 auth-list processing (during
//!   execution, after balance deduct); not raised from `validate_tx_env` /
//!   `validate_against_state_and_deduct_caller`.
//!
//! ## Upgrade-time audit checklist
//!
//! Re-run on every revm bump (`Cargo.toml: revm = ...`). Each forward-watch entry
//! below is currently unreachable but will become reachable on the listed bump and
//! MUST be paired with a new guard at that time:
//!
//! - `NonceOverflowInTransaction` — re-introduced in `revm-handler 18.1.0` (`validation.rs:232`).
//!   Bumping past `10.0.1` requires adding `tx.nonce() != u64::MAX` plus overflow-safe
//!   `account.nonce += 1`.
//! - `Eip7873NotSupported` / `Eip7873MissingTarget` — activates on OSAKA. Adding Osaka requires an
//!   init-code-tx type gate.
//! - `TxGasLimitGreaterThanCap` — per-tx gas cap. `validate_tx_env` rejects `tx.gas_limit() >
//!   cfg.tx_gas_limit_cap()`; that cap is `u64::MAX` pre-OSAKA. Under OSAKA Gravity pins it to a
//!   Monad-style `GRAVITY_TX_GAS_LIMIT_CAP` (30M) rather than EIP-7825's `2^24` — see the
//!   OSAKA-gated guard in `is_tx_valid` below. The executor cfg
//!   (`reth-evm-ethereum::apply_gravity_tx_gas_cap`) and the consensus block check
//!   (`reth-consensus-common`) are pinned to the same 30M value.
//!
//! Procedure on upgrade: `grep 'return Err(InvalidTransaction::'` in
//! `revm-handler/src/` for the new pin, diff against the "unreachable" list above,
//! and either add a guard or move the variant from the unreachable list to the
//! forward-watch list with a citation.
//!
//! Closed audit issues: gravity-audit#668 / #696 / #710.

use alloy_consensus::{constants::KECCAK_EMPTY, Transaction};
use alloy_primitives::{
    map::{HashMap, HashSet},
    Address, U256,
};
use reth_chainspec::{is_eip7702_lockdown_active, ChainSpec, EthChainSpec};
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::ParallelDatabase;
use reth_evm_ethereum::revm_spec_by_timestamp_and_block_number;
use reth_primitives_traits::constants::GRAVITY_TX_GAS_LIMIT_CAP;
use revm::state::AccountInfo;
use revm_primitives::{eip3860::MAX_INITCODE_SIZE, hardfork::SpecId};
use tracing::info;

/// Outcome of pre-execution filtering for one ordered block.
///
/// - [`Self::discard`]: validation failures — remove from the local pool and report `is_discarded`
///   to consensus.
/// - [`Self::defer`]: otherwise-valid txs that do not fit the remaining block gas budget (or depend
///   on a same-sender gas-deferred predecessor). Excluded from this block body but **kept** in the
///   pool for a later block (audit#646).
/// - Neither set: include in the block in original relative order.
#[derive(Debug, Default, Clone)]
pub(crate) struct TxFilterResult {
    /// Indices of txs that failed validation and must be discarded from the pool.
    pub discard: HashSet<usize>,
    /// Indices of valid txs skipped only for block-gas packing.
    pub defer: HashSet<usize>,
}

/// Filter ordered txs before grevm execution.
///
/// Walks the ordered list **serially** (required for EIP-7702 cross-account auth
/// nonce simulation). Per-tx order is: admission guards + in-block state sim first,
/// then block gas budget as the **last** gate. Invalid txs never consume gas budget,
/// so a junk high-`gas_limit` tx cannot prefix-cut later valid txs that still fit
/// (gravity-audit#646). Gas-deferred txs are not discarded from the pool.
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
///
/// EIP-7702 emergency lockdown (audit#838) is computed inside this function from
/// [`is_eip7702_lockdown_active`] (active until [`reth_chainspec::GravityHardfork::Beta`])
/// — not a bool parameter — so every call site agrees without a consensus-critical
/// flag that could drift.
#[allow(clippy::too_many_arguments)]
pub(crate) fn filter_invalid_txs<DB: ParallelDatabase>(
    db: DB,
    txs: &[TransactionSigned],
    senders: &[Address],
    base_fee_per_gas: u64,
    gas_limit: u64,
    chain_spec: &ChainSpec,
    block_timestamp: u64,
    block_number: u64,
) -> TxFilterResult {
    let eip7702_lockdown = is_eip7702_lockdown_active(chain_spec, block_timestamp);
    let spec_id =
        revm_spec_by_timestamp_and_block_number(chain_spec, block_timestamp, block_number);

    let cfg_chain_id = chain_spec.chain_id();
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
        // Chain-id gate. revm rejects with `InvalidChainId` when a typed tx carries the
        // wrong chain_id and with `MissingChainId` when a typed tx carries `None`. Legacy
        // txs may legitimately have `None` (pre-EIP-155 replay-vulnerable encoding), so
        // those are accepted here only if `tx.is_legacy()`. Closes audit#710 gap 5.
        match tx.chain_id() {
            Some(id) if id != cfg_chain_id => {
                info!(target: "filter_invalid_txs",
                    tx_hash=?tx.hash(),
                    sender=?sender,
                    tx_chain_id=?id,
                    cfg_chain_id=?cfg_chain_id,
                    "chain id mismatch"
                );
                return false;
            }
            None if !tx.is_legacy() => {
                info!(target: "filter_invalid_txs",
                    tx_hash=?tx.hash(),
                    sender=?sender,
                    "typed tx missing chain_id"
                );
                return false;
            }
            _ => {}
        }
        // Per-tx gas cap. Gravity uses a Monad-style flat cap (`GRAVITY_TX_GAS_LIMIT_CAP` = 30M,
        // matching Monad's `TFM_MAX_GAS_LIMIT`) in place of Ethereum's EIP-7825 `2^24`. Under
        // OSAKA revm rejects `tx.gas_limit() > cfg.tx_gas_limit_cap()` with
        // `TxGasLimitGreaterThanCap`; the executor cfg is pinned to the same 30M under OSAKA
        // (`reth-evm-ethereum::apply_gravity_tx_gas_cap`) and the consensus block check mirrors it
        // (`reth-consensus-common`). Placed after the chain-id gate and before the fee gates to
        // mirror revm's `validate_tx_env` rule order. revm's Amsterdam `is_amsterdam_eip8037`
        // wrapper is not mirrored — Gravity does not activate Amsterdam, so the omission is
        // over-filter (the safe direction). The boundary is inclusive (`>`): a 30M system tx
        // passes.
        if spec_id.is_enabled_in(SpecId::OSAKA) && tx.gas_limit() > GRAVITY_TX_GAS_LIMIT_CAP {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                gas_limit=?tx.gas_limit(),
                cap=?GRAVITY_TX_GAS_LIMIT_CAP,
                "tx gas limit exceeds Gravity per-tx cap"
            );
            return false;
        }
        // Fee gates. revm rejects with `GasPriceLessThanBasefee` when the tx's max fee
        // cap is below the prevailing base fee (this is the unified `max_fee_per_gas` —
        // legacy `gas_price` collapses into it), and with `PriorityFeeGreaterThanMaxFee`
        // when an EIP-1559+ tx sets `max_priority > max_fee`. Closes audit#710 gaps 1, 2.
        if tx.max_fee_per_gas() < base_fee_per_gas as u128 {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                max_fee_per_gas=?tx.max_fee_per_gas(),
                base_fee_per_gas=?base_fee_per_gas,
                "max fee below base fee"
            );
            return false;
        }
        if let Some(prio) = tx.max_priority_fee_per_gas() &&
            prio > tx.max_fee_per_gas()
        {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                max_priority_fee_per_gas=?prio,
                max_fee_per_gas=?tx.max_fee_per_gas(),
                "priority fee exceeds max fee"
            );
            return false;
        }
        // EIP-7702 gates.
        //
        // Base (always): reject a pre-Prague type-4 tx (revm `Eip7702NotSupported`) and a
        // post-Prague type-4 tx with an empty `authorization_list` (`EmptyAuthorizationList`);
        // both would otherwise reach the executor as InvalidTransaction. Closes audit#696 P-2
        // and audit#710 gap 3.
        //
        // EMERGENCY LOCKDOWN — L1 (audit#838 + #822, gated on `eip7702_lockdown` / pre-Beta):
        // when enabled, reject the ENTIRE type-4 tx type wholesale (a strict superset of the
        // base gate) so no NEW delegations can be created. Together with the from-/to-delegated
        // drops (L2/L3) below this closes the 7702 nonce-bump halt surface.
        if tx.is_eip7702() {
            if eip7702_lockdown {
                info!(target: "filter_invalid_txs",
                    tx_hash=?tx.hash(),
                    sender=?sender,
                    "7702-lockdown: EIP-7702 (SetCode) tx rejected wholesale (audit#838)"
                );
                return false;
            }
            if !spec_id.is_enabled_in(SpecId::PRAGUE) {
                info!(target: "filter_invalid_txs",
                    tx_hash=?tx.hash(),
                    sender=?sender,
                    spec_id=?spec_id,
                    "EIP-7702 tx in pre-Prague block"
                );
                return false;
            }
            let auth_count = tx.authorization_list().map(|l| l.len()).unwrap_or(0);
            if auth_count == 0 {
                info!(target: "filter_invalid_txs",
                    tx_hash=?tx.hash(),
                    sender=?sender,
                    "EIP-7702 tx with empty authorization_list"
                );
                return false;
            }
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
        if tx.gas_limit() < intrinsic.initial_total_gas() || tx.gas_limit() < intrinsic.floor_gas {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                gas_limit=?tx.gas_limit(),
                initial_gas=?intrinsic.initial_total_gas(),
                floor_gas=?intrinsic.floor_gas,
                "intrinsic gas too low"
            );
            return false;
        }
        // Balance gate. revm's `validate_against_state_and_deduct_caller` pre-deducts
        // `max_fee_per_gas * gas_limit + value` (worst case — the unused portion is
        // refunded post-execution). The filter must check against the same worst-case
        // bound, otherwise a tx where `effective < max_fee` and `balance < max * limit`
        // would pass here and panic in revm with `LackOfFundForMaxFee`. Closes audit#710
        // gap 4. The per-sender simulated balance is then reduced by the *effective*
        // cost so subsequent txs from the same sender see what revm sees post-refund.
        //
        // The `saturating_*` arithmetic below is also load-bearing for a second revm variant:
        // `validate_against_state_and_deduct_caller` calls `tx.max_balance_spending()`
        // (`pre_execution.rs:135`), which returns `OverflowPaymentInTransaction` when
        // `gas_limit * max_fee (+ value)` overflows. Saturating to `U256::MAX` and rejecting via
        // the `balance < max_total` comparison is a strict superset of that reject for every
        // physically-possible balance (a real divergence needs balance >= 2^128 wei). Keep these
        // saturating — switching to checked/wrapping would drop the implicit gate.
        let max_charge =
            U256::from(tx.max_fee_per_gas()).saturating_mul(U256::from(tx.gas_limit()));
        let max_total = max_charge.saturating_add(tx.value());
        if account.balance < max_total {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                balance=?account.balance,
                max_charge=?max_charge,
                transfer_value=?tx.value(),
                "insufficient balance for max fee"
            );
            return false;
        }
        let gas_spent = U256::from(tx.effective_gas_price(Some(base_fee_per_gas)))
            .saturating_mul(U256::from(tx.gas_limit()));
        let total_spent = gas_spent.saturating_add(tx.value());
        account.balance -= total_spent;
        account.nonce += 1;
        true
    };

    // `true` if the account's code is empty or a valid EIP-7702 delegation designator —
    // i.e. it may originate a tx (EIP-3607, closes audit#710 gap 6) and, as an
    // authorization authority, revm will apply its delegation.
    let code_permits = |acct: &AccountInfo| -> bool {
        acct.code_hash == KECCAK_EMPTY ||
            acct.code
                .clone()
                .or_else(|| db.code_by_hash_ref(acct.code_hash).ok())
                .map(|b| b.is_eip7702())
                .unwrap_or(false)
    };

    // EMERGENCY EIP-7702 LOCKDOWN — L2/L3 helper. `true` if the account CURRENTLY
    // carries a 7702 delegation designator (`0xef 0x01 0x00 || address`), read from
    // block-start state. Used to drop any tx originated BY (L2) or sent TO (L3) a
    // delegated account. Differs from `code_permits`: an empty-code EOA is NOT delegated.
    // Fail-closed: DB errors on code load panic (same spirit as sender `basic_ref`), so a
    // transient DB fault cannot silently classify a delegated account as non-delegated.
    let is_delegated = |acct: &AccountInfo| -> bool {
        if acct.code_hash == KECCAK_EMPTY {
            return false;
        }
        let code = match &acct.code {
            Some(c) => c.clone(),
            None => db
                .code_by_hash_ref(acct.code_hash)
                .expect("7702-lockdown: db.code_by_hash_ref for delegated check"),
        };
        code.is_eip7702()
    };

    // Block-order sequential simulation. `sim[addr]` is the address's account evolved
    // in-block (nonce + balance), seeded lazily from the certified parent state; `None`
    // marks an address absent from state. It holds BOTH tx senders AND EIP-7702
    // authorities: a self-sponsored OR cross-account authorization bumps ITS authority's
    // nonce during execution (revm `apply_auth_list` -> `delegate()` -> `bump_nonce`), so
    // a *later same-block* tx from that authority must see the bumped nonce, or it hits
    // `NonceTooLow` in revm and panics the executor (gravity-audit#822).
    //
    // This is sequential (not the previous per-sender-parallel pass) because cross-account
    // nonce effects cross sender groups, so the simulation must advance in a single global
    // block order. The per-tx guards are cheap; the extra `recover_authority()` ECDSA
    // recovery runs only for type-4 txs.
    //
    // Block gas is applied as the **last** gate after a tx has passed admission + sim:
    // invalid txs do not consume budget, and a tx that does not fit is deferred (kept in
    // pool) while later smaller txs from other senders may still pack (audit#646).
    let mut sim: HashMap<Address, Option<AccountInfo>> = HashMap::default();
    let mut result = TxFilterResult::default();
    // Senders whose next in-block nonce was gas-deferred: later same-sender txs cannot
    // execute without a nonce gap, so they are deferred too (not discarded).
    let mut gas_blocked_senders: HashSet<Address> = HashSet::default();
    let mut remaining_gas = gas_limit;

    for (idx, (tx, &sender)) in txs.iter().zip(senders.iter()).enumerate() {
        if gas_blocked_senders.contains(&sender) {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                "deferred: same-sender predecessor excluded by block gas budget"
            );
            result.defer.insert(idx);
            continue;
        }

        // EMERGENCY EIP-7702 LOCKDOWN — L3 (pre-Beta): drop any tx whose recipient is a
        // currently-delegated account, so no inbound CALL can trigger the callee's
        // delegated CREATE (audit#838). Read against block-start state (L1 rejects all
        // type-4, so no delegation is created in-block). `to()` is `None` for Create.
        // Fail-closed on DB error (same spirit as the sender `basic_ref` path below).
        if eip7702_lockdown && let Some(to) = tx.to() {
            let to_delegated = db
                .basic_ref(to)
                .expect("7702-lockdown L3: db.basic_ref for recipient")
                .as_ref()
                .map(is_delegated)
                .unwrap_or(false);
            if to_delegated {
                info!(target: "filter_invalid_txs",
                    tx_hash=?tx.hash(),
                    to=?to,
                    "7702-lockdown: tx to delegated account rejected (audit#838)"
                );
                result.discard.insert(idx);
                continue;
            }
        }

        // Validate the tx against the simulated sender account and apply the caller nonce
        // bump + balance deduction. Scoped so the `sim[sender]` borrow is released before
        // the authorization loop re-borrows `sim` (an authority may be any account).
        // On success the sim account is mutated; if we later defer for gas we must roll
        // that mutation back so a same-sender follow-up still sees the pre-tx account.
        let sender_snapshot = sim.get(&sender).cloned();
        let valid = {
            let sender_acct = sim.entry(sender).or_insert_with(|| db.basic_ref(sender).unwrap());
            match sender_acct.as_mut() {
                // Sender absent from state -> cannot pay for / originate the tx.
                None => false,
                Some(account) => {
                    if eip7702_lockdown && is_delegated(account) {
                        // EMERGENCY EIP-7702 LOCKDOWN — L2 (pre-Beta): the delegated
                        // sender's own execution-time CREATE can bump its nonce (not
                        // modelled by this filter's auth-list simulation), making this
                        // current-nonce tx `NonceTooLow` -> executor halt (audit#838).
                        info!(target: "filter_invalid_txs",
                            tx_hash=?tx.hash(),
                            sender=?sender,
                            "7702-lockdown: tx from delegated account rejected (audit#838)"
                        );
                        false
                    } else if !code_permits(account) {
                        info!(target: "filter_invalid_txs",
                            sender=?sender,
                            code_hash=?account.code_hash,
                            "EIP-3607: sender has non-delegation code"
                        );
                        false
                    } else {
                        // Mutates `account` (caller nonce bump + balance deduct) on success.
                        is_tx_valid(tx, &sender, account)
                    }
                }
            }
        };
        if !valid {
            result.discard.insert(idx);
            continue;
        }

        // Last gate: block gas budget (worst-case `tx.gas_limit()`). Does not discard.
        let tx_gas = tx.gas_limit();
        if tx_gas > remaining_gas {
            info!(target: "filter_invalid_txs",
                tx_hash=?tx.hash(),
                sender=?sender,
                tx_gas_limit=?tx_gas,
                remaining_gas=?remaining_gas,
                block_gas_limit=?gas_limit,
                "deferred: tx does not fit remaining block gas budget"
            );
            // Roll back the optimistic sim mutation from `is_tx_valid`.
            match sender_snapshot {
                Some(prev) => {
                    sim.insert(sender, prev);
                }
                None => {
                    sim.remove(&sender);
                }
            }
            result.defer.insert(idx);
            gas_blocked_senders.insert(sender);
            continue;
        }
        remaining_gas -= tx_gas;

        // Mirror revm `apply_auth_list`: for a type-4 tx every valid authorization bumps
        // ITS authority's nonce once — self (authority == sender) AND cross-account. The
        // caller bump above already advanced the sender, so a self-authorization matching
        // `sender_nonce + 1` bumps it a second time; chained authorizations advance off the
        // evolving simulated nonce exactly as revm applies them in order.
        if tx.is_eip7702() {
            if let Some(auth_list) = tx.authorization_list() {
                for auth in auth_list {
                    if !auth.chain_id().is_zero() && *auth.chain_id() != U256::from(cfg_chain_id) {
                        continue;
                    }
                    if auth.nonce() == u64::MAX {
                        continue;
                    }
                    let Ok(authority) = auth.recover_authority() else { continue };
                    let auth_acct =
                        sim.entry(authority).or_insert_with(|| db.basic_ref(authority).unwrap());
                    // revm applies the delegation iff the authority's (block-start) code is
                    // empty/7702 and its *current* nonce equals the authorization nonce; a
                    // nonexistent authority has nonce 0 and is created on delegation.
                    let (authority_nonce, code_ok) = match auth_acct.as_ref() {
                        Some(a) => (a.nonce, code_permits(a)),
                        None => (0, true),
                    };
                    if code_ok && auth.nonce() == authority_nonce {
                        match auth_acct {
                            Some(a) => a.nonce = a.nonce.saturating_add(1),
                            None => {
                                *auth_acct = Some(AccountInfo { nonce: 1, ..Default::default() })
                            }
                        }
                    }
                }
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{TxEip4844, TxEip7702, TxLegacy};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::{Address, Bytes, Signature, TxKind, B256};
    use reth_chainspec::{
        ChainHardforks, ChainSpec, ChainSpecBuilder, ForkCondition, GravityHardfork, MAINNET,
    };
    use reth_ethereum_primitives::{Transaction, TransactionSigned};
    use reth_revm::state::{AccountInfo, Bytecode};
    use revm::{
        primitives::{StorageKey, StorageValue},
        DatabaseRef,
    };
    use std::{collections::HashMap as StdHashMap, sync::Arc};

    /// Mainnet chain id — pinned here because every test chainspec in this module is
    /// built from `MAINNET`, and the filter's chain-id gate rejects any typed tx whose
    /// `chain_id` doesn't match the chainspec.
    const MAINNET_CHAIN_ID: u64 = 1;

    /// Chainspec with Prague active from genesis but **no** Gravity Beta — the default
    /// fixture. EIP-7702 lockdown is ON (fail-closed: missing `betaTime` keeps lockdown
    /// active). Use [`prague_chain_spec_with_beta`] for cases that need type-4 to pass.
    fn prague_chain_spec() -> Arc<ChainSpec> {
        Arc::new(ChainSpecBuilder::from(&*MAINNET).prague_activated().build())
    }

    /// Prague + Gravity Beta active from timestamp 0 — lockdown OFF. Required by every
    /// test that expects a type-4 tx (or a tx from a delegated account) to be admitted.
    fn prague_chain_spec_with_beta() -> Arc<ChainSpec> {
        prague_chain_spec_with_beta_at(0)
    }

    /// Prague + Gravity Beta activating at the given timestamp (fork-boundary tests).
    fn prague_chain_spec_with_beta_at(beta_time: u64) -> Arc<ChainSpec> {
        let mut spec = ChainSpecBuilder::from(&*MAINNET).prague_activated().build();
        spec.gravity_hardforks =
            ChainHardforks::from([(GravityHardfork::Beta, ForkCondition::Timestamp(beta_time))]);
        Arc::new(spec)
    }

    /// Chainspec with Shanghai active from genesis but Prague unset — the
    /// `pre-Prague` test fixture. Used to pin the boundary where a TxEip7702
    /// must be discarded by the filter (revm would otherwise reject it with
    /// `Eip7702NotSupported` and panic the executor).
    fn shanghai_chain_spec() -> Arc<ChainSpec> {
        Arc::new(ChainSpecBuilder::from(&*MAINNET).shanghai_activated().build())
    }

    /// Chainspec with Osaka active from genesis — the fixture for the Gravity per-tx gas cap
    /// gate. The gate is OSAKA-gated, so it only fires under this fixture (not the Prague one).
    fn osaka_chain_spec() -> Arc<ChainSpec> {
        Arc::new(ChainSpecBuilder::from(&*MAINNET).osaka_activated().build())
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

    /// Legacy tx with a caller-chosen recipient — for the 7702-lockdown L3
    /// (tx-to-delegated) tests.
    fn create_test_transaction_to(
        nonce: u64,
        gas_limit: u64,
        gas_price: u128,
        to: Address,
    ) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy {
                nonce,
                gas_price,
                gas_limit,
                to: TxKind::Call(to),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    /// A block-start-state account carrying a 7702 delegation designator to `target`.
    fn delegated_account(target: Address) -> AccountInfo {
        AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64),
            nonce: 0,
            code_hash: B256::repeat_byte(0xcd),
            code: Some(Bytecode::new_eip7702(target)),
            account_id: None,
        }
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

    /// EIP-7702 type-0x04 tx with N authorizations; `gas_limit` caller controls. The
    /// `chain_id` is pinned to `MAINNET_CHAIN_ID` to match the test chainspecs — the
    /// filter rejects mismatches with `InvalidChainId`.
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
                chain_id: MAINNET_CHAIN_ID,
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

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(result.discard.is_empty());
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

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(result.discard.len(), 1);
        assert!(result.discard.contains(&0));
    }

    #[test]
    fn test_filter_invalid_txs_nonce_mismatch() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // the account exists, but the nonce does not match
        let account = AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            nonce: 5,                                          // 账户 nonce 是 5
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
        };
        db.insert_account(sender, account);

        // the transaction nonce is 0, but the account nonce is 5
        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64;

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(result.discard.len(), 1);
        assert!(result.discard.contains(&0));
    }

    #[test]
    fn test_filter_invalid_txs_insufficient_balance() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // the account has insufficient balance
        let account = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 0,
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
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

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(result.discard.len(), 2);
        assert!(result.discard.contains(&0));
        assert!(result.discard.contains(&2));
    }

    /// audit#646: a valid tx that does not fit remaining block gas is deferred (kept in
    /// pool), not discarded. Same-sender follow-ups are deferred too (nonce gap).
    #[test]
    fn test_filter_invalid_txs_gas_limit_exceeded() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // the account has enough balance
        let account = AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            nonce: 0,
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
        };
        db.insert_account(sender, account);

        // create multiple transactions, the cumulative gas limit exceeds the block limit
        let tx1 = create_test_transaction(0, 20_000_000, 25_000_000_000); // 20M gas
        let tx2 = create_test_transaction(1, 20_000_000, 25_000_000_000); // 20M gas
        let txs = vec![tx1, tx2];
        let senders = vec![sender, sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64; // 30M gas limit

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(result.discard.is_empty(), "gas overflow must not discard: {result:?}");
        assert_eq!(result.defer.len(), 1);
        assert!(result.defer.contains(&1));
    }

    // Models the `create_block_for_executor` byzantine path from gravity-audit#621 Fix A:
    // when `sum_system_gas ≥ block.gas_limit`, the call site's `saturating_sub` collapses
    // the user-tx budget to 0, and the filter must exclude *every* user tx — otherwise
    // `header.gas_used > header.gas_limit` once system-txn receipts are appended.
    // Under audit#646 those exclusions are deferred (pool-safe), not discarded.
    #[test]
    fn test_filter_invalid_txs_zero_budget_drops_all() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx1 = create_test_transaction(0, 21_000, 25_000_000_000);
        let tx2 = create_test_transaction(1, 21_000, 25_000_000_000);
        let txs = vec![tx1, tx2];
        let senders = vec![sender, sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            20_000_000_000u64,
            0, // user budget collapsed by saturating_sub
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(result.discard.is_empty(), "zero budget must not wipe the pool: {result:?}");
        assert_eq!(result.defer.len(), 2);
        assert!(result.defer.contains(&0));
        assert!(result.defer.contains(&1));
    }

    #[test]
    fn test_filter_invalid_txs_valid_transactions() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // 账户有足够余额
        let account = AccountInfo {
            balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
            nonce: 0,
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
        };
        db.insert_account(sender, account);

        // create valid transactions
        let tx1 = create_test_transaction(0, 21_000, 25_000_000_000);
        let tx2 = create_test_transaction(1, 21_000, 25_000_000_000);
        let txs = vec![tx1, tx2];
        let senders = vec![sender, sender];
        let base_fee_per_gas = 20_000_000_000u64;
        let gas_limit = 30_000_000u64;

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(result.discard.is_empty());
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
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
        };
        db.insert_account(sender1, account1);

        let account2 = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 5,
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
        };
        db.insert_account(sender2, account2);

        let account3 = AccountInfo {
            balance: U256::from(1_000_000_000u64), // 1 Gwei
            nonce: 0,
            code_hash: KECCAK_EMPTY,
            code: None,
            account_id: None,
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
        // base_fee is 0 here because this test deliberately uses sub-gwei gas_prices to
        // probe nonce / balance / cumulative-gas branches without entangling them with
        // the fee-floor gate (audit#710 gap 1). Other tests cover the fee gate directly.
        let base_fee_per_gas = 0u64;
        let gas_limit = 30_000_000u64;

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        // Validation failures only — gas packing uses defer (audit#646).
        // idx1/2/4: nonce/balance failures; idx5 (30M) deferred for gas; idx6 still packs.
        assert_eq!(result.discard.len(), 3, "discard: {result:?}");
        assert!(result.discard.contains(&1));
        assert!(result.discard.contains(&2));
        assert!(result.discard.contains(&4));
        assert!(result.defer.contains(&5), "30M tx deferred for gas: {result:?}");
        assert!(
            !result.discard.contains(&6) && !result.defer.contains(&6),
            "later small valid tx must still pack: {result:?}"
        );
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // 21000 (base) + 25000 (PER_EMPTY_ACCOUNT_COST) = 46000 is the Prague intrinsic floor for
        // a 7702 tx with one authorization and no calldata; gas_limit 30_000 is below it.
        let tx = create_test_7702_transaction(0, 30_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 0;
        let gas_limit = 30_000_000u64;

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(result.discard.len(), 1, "intrinsic-gas-too-low 7702 tx should be discarded");
        assert!(result.discard.contains(&0));
    }

    /// Sanity check (lockdown OFF / Beta active): same 7702 tx with a `gas_limit` at or
    /// above the floor passes the filter. Uses `prague_chain_spec_with_beta` so L1 does
    /// not wholesale-reject type-4.
    #[test]
    fn test_filter_invalid_txs_eip7702_intrinsic_gas_just_enough_under_prague() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // exactly 21000 + 25000 = 46000 — at the floor, should pass post-Beta
        let tx = create_test_7702_transaction(0, 46_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];
        let base_fee_per_gas = 0;
        let gas_limit = 30_000_000u64;

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            base_fee_per_gas,
            gas_limit,
            &prague_chain_spec_with_beta(),
            0,
            0,
        );
        assert!(result.discard.is_empty(), "got: {result:?}");
    }

    /// U-1 (acceptance design §3.1, lockdown OFF / Beta active): a 7702 tx with
    /// `authorization_list.len() == 2` and `gas_limit = 72000` passes under Prague+Beta.
    #[test]
    fn test_filter_invalid_txs_eip7702_two_auths_gas_sufficient() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_7702_transaction(0, 72_000, 2);
        let txs = vec![tx];
        let senders = vec![sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            0,
            30_000_000,
            &prague_chain_spec_with_beta(),
            0,
            0,
        );
        assert!(
            result.discard.is_empty(),
            "two-auth 7702 tx with 72k gas must pass post-Beta: {result:?}"
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_7702_transaction(0, 21_000, 3);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "three-auth 7702 tx at 21k gas must be discarded");
        assert!(result.discard.contains(&0));
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // 100k gas is well above the pre-Prague intrinsic (21k flat, since the
        // auth-list cost is gated behind PRAGUE). Without the pre-Prague guard
        // this tx would pass the filter and panic the executor.
        let tx = create_test_7702_transaction(0, 100_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &shanghai_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "7702 tx must be discarded when spec_id < PRAGUE");
        assert!(result.discard.contains(&0));
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            result.discard.is_empty(),
            "legacy 21k-gas tx must not be regressed by the 7702 intrinsic fix: {result:?}"
        );
    }

    /// Build a type-3 (EIP-4844) tx with the given nonce / gas_limit. Other fields default —
    /// the filter rejects the whole tx type, so the inner shape doesn't matter for the test.
    fn create_test_4844_transaction(nonce: u64, gas_limit: u64) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Eip4844(TxEip4844 {
                chain_id: MAINNET_CHAIN_ID,
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
                chain_id: MAINNET_CHAIN_ID,
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // Default TxEip4844 has empty blob_versioned_hashes — revm would reject it with
        // `EmptyBlobs` at tx-level validation, panicking the executor without this filter.
        let tx = create_test_4844_transaction(0, 100_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "type-3 (blob) tx must be discarded on Gravity");
        assert!(result.discard.contains(&0));
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // 1 byte over the EIP-3860 cap; gas_limit deliberately high so the intrinsic check
        // wouldn't reject it — the size check is what must fire.
        let tx = create_test_oversize_initcode_transaction(0, 30_000_000, MAX_INITCODE_SIZE + 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(
            result.discard.len(),
            1,
            "Create tx with init_code.len() > MAX_INITCODE_SIZE must be discarded"
        );
        assert!(result.discard.contains(&0));
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
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // gas_limit large enough to cover 21_000 + 32_000 + word-cost + calldata for 49152 zero
        // bytes (4 gas/byte). Block gas_limit raised in lockstep so the cumulative-gas truncation
        // check doesn't kick in first.
        let tx = create_test_oversize_initcode_transaction(0, 5_000_000, MAX_INITCODE_SIZE);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 10_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            result.discard.is_empty(),
            "Create tx at exactly MAX_INITCODE_SIZE must pass the size gate: {result:?}"
        );
    }

    // ===== audit#710 helpers =====================================================

    /// 1559 envelope with caller-controlled fee fields, pinned to MAINNET chain_id.
    fn create_test_1559_transaction(
        nonce: u64,
        gas_limit: u64,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
    ) -> TransactionSigned {
        use alloy_consensus::TxEip1559;
        TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                chain_id: MAINNET_CHAIN_ID,
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

    /// 1559 envelope with caller-controlled `chain_id` for chain-id gate tests.
    fn create_test_1559_transaction_with_chain_id(
        nonce: u64,
        gas_limit: u64,
        chain_id: u64,
    ) -> TransactionSigned {
        use alloy_consensus::TxEip1559;
        TransactionSigned::new_unhashed(
            Transaction::Eip1559(TxEip1559 {
                chain_id,
                nonce,
                gas_limit,
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 0,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    /// Legacy tx with caller-controlled `chain_id` (None for pre-EIP-155).
    fn create_test_legacy_with_chain_id(
        nonce: u64,
        gas_limit: u64,
        gas_price: u128,
        chain_id: Option<u64>,
    ) -> TransactionSigned {
        TransactionSigned::new_unhashed(
            Transaction::Legacy(TxLegacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                to: TxKind::Call(Address::ZERO),
                ..Default::default()
            }),
            Signature::test_signature(),
        )
    }

    // ===== audit#710 gap 1: max_fee < base_fee ==================================

    /// Legacy tx whose `gas_price` is below the prevailing base fee must be discarded —
    /// revm fires `GasPriceLessThanBasefee`.
    #[test]
    fn test_filter_invalid_txs_legacy_gas_price_less_than_basefee() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // gas_price = 10 gwei, base_fee = 20 gwei.
        let tx = create_test_transaction(0, 21_000, 10_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            20_000_000_000,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(
            result.discard.len(),
            1,
            "legacy tx with gas_price < base_fee must be discarded"
        );
        assert!(result.discard.contains(&0));
    }

    /// 1559 tx whose `max_fee_per_gas` is below the prevailing base fee must be
    /// discarded (same revm error class as the legacy case, unified envelope).
    #[test]
    fn test_filter_invalid_txs_1559_max_fee_less_than_basefee() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // max_fee = 10 gwei, base_fee = 20 gwei.
        let tx = create_test_1559_transaction(0, 21_000, 10_000_000_000, 0);
        let txs = vec![tx];
        let senders = vec![sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            20_000_000_000,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(result.discard.len(), 1, "1559 tx with max_fee < base_fee must be discarded");
        assert!(result.discard.contains(&0));
    }

    // ===== audit#710 gap 2: priority > max =====================================

    /// 1559+ tx with `max_priority_fee > max_fee` is rejected by revm with
    /// `PriorityFeeGreaterThanMaxFee`.
    #[test]
    fn test_filter_invalid_txs_priority_fee_greater_than_max_fee() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // max_fee = 10 gwei, priority = 20 gwei → inverted, must reject.
        let tx = create_test_1559_transaction(0, 21_000, 10_000_000_000, 20_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "1559 tx with prio > max must be discarded");
        assert!(result.discard.contains(&0));
    }

    // ===== audit#710 gap 3: 7702 empty authorization_list ======================

    /// Post-Prague TxEip7702 with `authorization_list = []` is rejected by revm with
    /// `EmptyAuthorizationList`.
    #[test]
    fn test_filter_invalid_txs_eip7702_empty_authorization_list() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // 0 authorizations under Prague — well above intrinsic floor (21k) so the
        // intrinsic-gas gate would not fire first.
        let tx = create_test_7702_transaction(0, 100_000, 0);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(
            result.discard.len(),
            1,
            "7702 tx with empty authorization_list must be discarded"
        );
        assert!(result.discard.contains(&0));
    }

    // ===== audit#710 gap 4: balance must use max_fee, not effective ============

    /// In the window where `effective_gas_price < max_fee_per_gas` and
    /// `effective * gas <= balance < max_fee * gas`, the old filter passed the tx
    /// (using effective for the check); revm would then panic the executor with
    /// `LackOfFundForMaxFee`. The new filter must reject.
    #[test]
    fn test_filter_invalid_txs_balance_uses_max_fee_not_effective() {
        let mut db = MockDatabase::new();
        let sender = Address::random();

        // base_fee = 10 gwei, prio = 5 gwei, max_fee = 30 gwei, gas_limit = 21_000.
        //   effective = min(max_fee, base_fee + prio) = min(30G, 15G) = 15G
        //   effective * gas = 15G * 21k = 315e12
        //   max_fee   * gas = 30G * 21k = 630e12
        // Set balance to 400e12 — covers effective but not max. Old filter would
        // have passed; new filter must reject (LackOfFundForMaxFee class).
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(400_000_000_000_000u128),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_1559_transaction(0, 21_000, 30_000_000_000, 5_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            10_000_000_000,
            30_000_000,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(
            result.discard.len(),
            1,
            "balance covers effective but not max — must be discarded by max-fee gate"
        );
        assert!(result.discard.contains(&0));
    }

    // ===== audit#710 gap 5: chain_id =========================================

    /// 1559 tx with `chain_id != cfg.chain_id` is rejected (revm `InvalidChainId`).
    #[test]
    fn test_filter_invalid_txs_invalid_chain_id_typed() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // Chainspec is MAINNET (1), tx claims chain_id = 2.
        let tx = create_test_1559_transaction_with_chain_id(0, 21_000, 2);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "1559 tx with wrong chain_id must be discarded");
        assert!(result.discard.contains(&0));
    }

    /// Legacy tx with explicit `chain_id` that doesn't match config is rejected.
    #[test]
    fn test_filter_invalid_txs_invalid_chain_id_legacy() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_legacy_with_chain_id(0, 21_000, 25_000_000_000, Some(2));
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "legacy tx with chain_id=Some(2) must be discarded");
        assert!(result.discard.contains(&0));
    }

    /// Boundary: legacy pre-EIP-155 tx (`chain_id = None`) is accepted — matches
    /// revm's behaviour and reth-pool's policy.
    #[test]
    fn test_filter_invalid_txs_legacy_pre_eip155_chain_id_none_accepted() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_legacy_with_chain_id(0, 21_000, 25_000_000_000, None);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            result.discard.is_empty(),
            "pre-EIP-155 legacy tx must pass the chain-id gate: {result:?}"
        );
    }

    // ===== audit#710 gap 6: EIP-3607 (sender has code) =========================

    /// EIP-3607: sender with non-empty, non-7702-delegation code cannot originate
    /// transactions. revm fires `RejectCallerWithCode`. Per-sender — all of the
    /// sender's txs in the batch are invalidated.
    #[test]
    fn test_filter_invalid_txs_sender_with_code_eip3607_rejected() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        let code = Bytecode::new_raw(Bytes::from(vec![0x60u8, 0x00, 0x60, 0x00, 0xf3]));
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                // Any non-KECCAK_EMPTY hash triggers the gate; filter resolves the
                // actual bytecode via `account.code` first (no DB fetch needed).
                code_hash: B256::repeat_byte(0xab),
                code: Some(code),
                account_id: None,
            },
        );

        // Two txs from the same sender — both must be invalidated (per-sender check).
        let tx1 = create_test_transaction(0, 21_000, 25_000_000_000);
        let tx2 = create_test_transaction(1, 21_000, 25_000_000_000);
        let txs = vec![tx1, tx2];
        let senders = vec![sender, sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 2, "all txs from coded sender must be discarded");
        assert!(result.discard.contains(&0));
        assert!(result.discard.contains(&1));
    }

    /// EIP-3607 delegation exception (lockdown OFF / Beta active): sender whose code
    /// is an EIP-7702 delegation designator (`0xef 0x01 0x00 + address`) is allowed
    /// to send txs — Pectra relaxed 3607 precisely to permit delegated EOAs.
    #[test]
    fn test_filter_invalid_txs_sender_with_7702_delegation_accepted() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        let target = Address::repeat_byte(0x42);
        db.insert_account(sender, delegated_account(target));

        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            0,
            30_000_000,
            &prague_chain_spec_with_beta(),
            0,
            0,
        );
        assert!(
            result.discard.is_empty(),
            "tx from EIP-7702-delegated sender must pass post-Beta: {result:?}"
        );
    }

    // ===== EIP-7702 lockdown (audit#838) — gated on Gravity Beta ===============

    /// L1: type-4 (SetCode) is rejected wholesale while lockdown is active
    /// (Prague without Beta). Intrinsic gas is sufficient so only L1 fires.
    #[test]
    fn test_filter_invalid_txs_eip7702_rejected_under_lockdown_pre_beta() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_7702_transaction(0, 46_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        // Default prague_chain_spec has no betaTime → lockdown ON.
        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "L1 must wholesale-reject type-4 pre-Beta: {result:?}");
        assert!(result.discard.contains(&0));
    }

    /// L1 post-Beta: same type-4 with sufficient gas is admitted once Beta is active.
    #[test]
    fn test_filter_invalid_txs_eip7702_accepted_post_beta() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_7702_transaction(0, 46_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            0,
            30_000_000,
            &prague_chain_spec_with_beta(),
            0,
            0,
        );
        assert!(result.discard.is_empty(), "type-4 at floor must pass post-Beta: {result:?}");
    }

    /// Fork boundary: betaTime = T; block_timestamp T-1 rejects type-4, T accepts.
    #[test]
    fn test_filter_invalid_txs_eip7702_lockdown_fork_boundary() {
        const BETA_TIME: u64 = 1_700_000_000;
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_7702_transaction(0, 46_000, 1);
        let txs = vec![tx];
        let senders = vec![sender];
        let chain_spec = prague_chain_spec_with_beta_at(BETA_TIME);

        let pre =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &chain_spec, BETA_TIME - 1, 0);
        assert_eq!(pre.discard.len(), 1, "T-1 must still be under lockdown: {pre:?}");
        assert!(pre.discard.contains(&0));

        let at = filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &chain_spec, BETA_TIME, 0);
        assert!(at.discard.is_empty(), "T must release lockdown: {at:?}");
    }

    /// L2: under lockdown a tx FROM a delegated account is dropped.
    #[test]
    fn test_filter_invalid_txs_sender_with_7702_delegation_rejected_under_lockdown() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        let target = Address::repeat_byte(0x42);
        db.insert_account(sender, delegated_account(target));

        let tx = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(
            result.discard.len(),
            1,
            "7702-lockdown L2 must drop a tx from a delegated sender: {result:?}"
        );
        assert!(result.discard.contains(&0));
    }

    /// L3: under lockdown a tx TO a delegated account is dropped.
    #[test]
    fn test_filter_invalid_txs_recipient_delegated_rejected_under_lockdown() {
        let mut db = MockDatabase::new();
        let caller = Address::random();
        let delegated = Address::repeat_byte(0xa1);
        db.insert_account(
            caller,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );
        db.insert_account(delegated, delegated_account(Address::repeat_byte(0x42)));

        let tx = create_test_transaction_to(0, 21_000, 25_000_000_000, delegated);
        let txs = vec![tx];
        let senders = vec![caller];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(
            result.discard.len(),
            1,
            "7702-lockdown L3 must drop a tx to a delegated recipient: {result:?}"
        );
        assert!(result.discard.contains(&0));
    }

    /// audit#838 attack shape: [funder→A, A@nonce] — both dropped under lockdown
    /// (L3 then L2).
    #[test]
    fn test_filter_invalid_txs_audit838_attack_shape_neutralised() {
        let mut db = MockDatabase::new();
        let funder = Address::random();
        let a = Address::repeat_byte(0xaa);
        db.insert_account(
            funder,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );
        db.insert_account(a, delegated_account(Address::repeat_byte(0xcc)));

        let x = create_test_transaction_to(0, 21_000, 25_000_000_000, a);
        let a_at_m = create_test_transaction(0, 21_000, 25_000_000_000);
        let txs = vec![x, a_at_m];
        let senders = vec![funder, a];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert_eq!(
            result.discard.len(),
            2,
            "both legs of the audit#838 attack shape must be dropped: {result:?}"
        );
        assert!(result.discard.contains(&0) && result.discard.contains(&1));
    }

    /// Lockdown precision: non-delegated traffic (ordinary contract recipient) is
    /// unaffected while lockdown is on.
    #[test]
    fn test_filter_invalid_txs_non_delegated_traffic_unaffected_by_lockdown() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        let contract = Address::repeat_byte(0xbe);
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );
        db.insert_account(
            contract,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 1,
                code_hash: B256::repeat_byte(0x11),
                code: Some(Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00]))),
                account_id: None,
            },
        );

        let tx = create_test_transaction_to(0, 21_000, 25_000_000_000, contract);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 30_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            result.discard.is_empty(),
            "non-delegated traffic must be unaffected by the 7702 lockdown: {result:?}"
        );
    }

    // ===== Gravity per-tx gas cap (Monad-style 30M, OSAKA-gated) ================

    /// Pins the cap value and the boundary is inclusive, so a system tx built at exactly this
    /// value (`new_system_call_txn`, 30M) clears the gate. Lowering the cap below 30M — or
    /// raising the system-tx `gas_limit` above it — would start rejecting system transactions.
    #[test]
    fn test_gravity_tx_gas_cap_constant() {
        assert_eq!(GRAVITY_TX_GAS_LIMIT_CAP, 30_000_000);
    }

    /// A tx with `gas_limit == cap` (30M) passes under Osaka — the gate is a strict `>`, so the
    /// boundary is admitted (this is what lets the 30M system transactions through).
    #[test]
    fn test_filter_invalid_txs_osaka_gas_cap_boundary_passes() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64), // 1 ETH
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        // gas_price 1 / base_fee 0 keeps the fee + balance gates out of the way; block budget
        // (60M) is above the tx so the cumulative pre-pass doesn't truncate first.
        let tx = create_test_transaction(0, GRAVITY_TX_GAS_LIMIT_CAP, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 60_000_000, &osaka_chain_spec(), 0, 0);
        assert!(
            result.discard.is_empty(),
            "30M tx at the cap boundary must pass under Osaka: {result:?}"
        );
    }

    /// A tx one gas over the cap (30M + 1) is rejected under Osaka.
    #[test]
    fn test_filter_invalid_txs_osaka_gas_cap_over_rejected() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_transaction(0, GRAVITY_TX_GAS_LIMIT_CAP + 1, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 60_000_000, &osaka_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "tx above the 30M cap must be discarded under Osaka");
        assert!(result.discard.contains(&0));
    }

    /// The gate is OSAKA-gated: the same over-cap tx passes under Prague (no per-tx cap exists
    /// pre-Osaka, matching revm's `cfg.tx_gas_limit_cap()` = `u64::MAX` there).
    #[test]
    fn test_filter_invalid_txs_pre_osaka_no_gas_cap() {
        let mut db = MockDatabase::new();
        let sender = Address::random();
        db.insert_account(
            sender,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000u64),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                account_id: None,
            },
        );

        let tx = create_test_transaction(0, GRAVITY_TX_GAS_LIMIT_CAP + 1, 1);
        let txs = vec![tx];
        let senders = vec![sender];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 60_000_000, &prague_chain_spec(), 0, 0);
        assert!(
            result.discard.is_empty(),
            "pre-Osaka has no per-tx gas cap, over-cap tx must pass: {result:?}"
        );
    }

    /// The cap is per-tx and does not pollute across senders: an over-cap tx from sender A is
    /// dropped while a normal tx from sender B in the same batch passes, under Osaka.
    #[test]
    fn test_filter_invalid_txs_osaka_gas_cap_per_sender() {
        let mut db = MockDatabase::new();
        let sender_a = Address::random();
        let sender_b = Address::random();
        for s in [sender_a, sender_b] {
            db.insert_account(
                s,
                AccountInfo {
                    balance: U256::from(1_000_000_000_000_000_000u64),
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                    account_id: None,
                },
            );
        }

        let tx_a = create_test_transaction(0, GRAVITY_TX_GAS_LIMIT_CAP + 1, 1); // over cap
        let tx_b = create_test_transaction(0, 21_000, 1); // fine
        let txs = vec![tx_a, tx_b];
        let senders = vec![sender_a, sender_b];

        let result =
            filter_invalid_txs(&db, &txs, &senders, 0, 60_000_000, &osaka_chain_spec(), 0, 0);
        assert_eq!(result.discard.len(), 1, "only sender A's over-cap tx must be dropped");
        assert!(result.discard.contains(&0));
    }

    /// audit#646: gas is the last gate. An invalid early tx must not consume block gas
    /// budget, so a later valid tx that still fits is included (not prefix-cut / discarded).
    #[test]
    fn test_filter_invalid_txs_invalid_does_not_steal_gas_budget() {
        let mut db = MockDatabase::new();
        let sender_a = Address::random();
        let sender_b = Address::random();
        for s in [sender_a, sender_b] {
            db.insert_account(
                s,
                AccountInfo {
                    balance: U256::from(1_000_000_000_000_000_000u64),
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                    account_id: None,
                },
            );
        }

        // Order: valid 10M, invalid (wrong nonce) claiming 25M, valid 5M from another sender.
        // Budget 30M. Old prefix-cut would stop at idx1 (10+25>30) and discard idx1+idx2.
        // New: idx1 discarded for nonce, does not consume gas; idx2 packs (10+5 <= 30).
        let tx0 = create_test_transaction(0, 10_000_000, 25_000_000_000);
        let tx1 = create_test_transaction(9, 25_000_000, 25_000_000_000); // bad nonce
        let tx2 = create_test_transaction(0, 5_000_000, 25_000_000_000);
        let txs = vec![tx0, tx1, tx2];
        let senders = vec![sender_a, sender_a, sender_b];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            20_000_000_000u64,
            30_000_000u64,
            &prague_chain_spec(),
            0,
            0,
        );
        assert_eq!(result.discard.len(), 1);
        assert!(result.discard.contains(&1));
        assert!(result.defer.is_empty(), "later valid tx must pack, not defer: {result:?}");
    }

    /// audit#646: continue packing smaller txs after a large one does not fit remaining gas.
    #[test]
    fn test_filter_invalid_txs_continues_packing_after_gas_skip() {
        let mut db = MockDatabase::new();
        let sender_big = Address::random();
        let sender_small = Address::random();
        for s in [sender_big, sender_small] {
            db.insert_account(
                s,
                AccountInfo {
                    balance: U256::from(1_000_000_000_000_000_000u64),
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                    account_id: None,
                },
            );
        }

        // Budget 30M: pack 20M, skip next same-sender 20M (defer), still pack 5M from
        // another sender under the remaining 10M.
        let tx0 = create_test_transaction(0, 20_000_000, 25_000_000_000);
        let tx1 = create_test_transaction(1, 20_000_000, 25_000_000_000);
        let tx2 = create_test_transaction(0, 5_000_000, 25_000_000_000);
        let txs = vec![tx0, tx1, tx2];
        let senders = vec![sender_big, sender_big, sender_small];

        let result = filter_invalid_txs(
            &db,
            &txs,
            &senders,
            20_000_000_000u64,
            30_000_000u64,
            &prague_chain_spec(),
            0,
            0,
        );
        assert!(result.discard.is_empty(), "{result:?}");
        assert_eq!(result.defer.len(), 1);
        assert!(result.defer.contains(&1));
        assert!(!result.defer.contains(&2), "small later tx must pack: {result:?}");
    }
}
