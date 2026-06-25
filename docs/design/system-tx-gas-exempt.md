# System Transactions: gas-exempt + zero-balance SYSTEM_CALLER

> Implementation tracking: [gravity-reth#364](https://github.com/Galxe/gravity-reth/issues/364) ·
> analysis: [gravity-audit#720](https://github.com/Galxe/gravity-audit/issues/720).
> Gated behind the **Epsilon** Gravity hardfork — inert until a chainspec/genesis sets its
> activation time.

## Problem

The system-transaction sender `SYSTEM_CALLER` (`0x00000000000000000000000000000001625f0000`,
defined in `crates/pipe-exec-layer-ext-v2/execute/src/onchain_config/mod.rs`) is pre-funded in
genesis with a sentinel "infinite" balance (≈ 1.16e58 G) so it can pay the base fee of the system
transactions the protocol injects every block. On-chain measurement: each system tx has
`gas_price = baseFee` with no tip, so its base fee is burned from `SYSTEM_CALLER`'s balance
(~0.006 G/block; the balance only decreases).

The fake supply has been verified **not** to circulate and the footgun has **not** fired, but the
design carries four issues:

1. **Supply-accounting pollution** — every total/circulating-supply computation must special-case
   the 1.16e58 sentinel.
2. **Unbounded-mint footgun** — any bug that lets value leave this account mints real G without
   bound.
3. **Conceptual muddle** — base fee is burned from a fake account for the protocol's own txs.
4. **Soft invariant** — relies on "1e58 never runs out" instead of a hard "system txs are gas-free"
   rule.

## Production precedents

| | EIP-4788/2935 | Arbitrum `0x6a` ArbInternalTx | Gravity (today) | This change |
| --- | --- | --- | --- | --- |
| Receipt / visible in block | none (phantom) | yes (`status=0x1`) | yes | kept |
| Sender nonce increments | no | no (stays 0, `0xA4B05`) | yes (≈ height) | yes (kept; static optional) |
| Gas / fee | free, metered | fully free (`gasUsed=0`) | charges base fee | free, metered |
| Sender balance | 0 | 0 | 1.16e58 sentinel | zeroed at fork |

The fee semantics match EIP-4788/2935's `SYSTEM_ADDRESS` pattern; closest to Arbitrum (real tx +
receipt + fully free + zero balance).

## Plan (hardfork — changes the state root, must switch atomically across all nodes)

### 1. Execution layer: gas-exempt system txs (gas still metered)

When the Epsilon fork is active at the block timestamp, set on the system-tx EVM env:

- `cfg_env.disable_base_fee = true`
- `cfg_env.disable_balance_check = true`
- (`gas_price = 0` optional; the tip is already 0)
- keep `disable_nonce_check = false` (the nonce sequence keeps incrementing)

Execution / calldata / state / receipts / `gas_used` are unchanged — only fee accounting is
skipped. Reuses the revm cfg flags already used in `rpc-eth-api` (eth_call / estimate / prewarm).

**Status: implemented** in both backends (must stay in lockstep):
- serial: `crates/ethereum/evm/src/lib.rs` → `EthEvmConfig::transact_system_txn`
- grevm: `crates/ethereum/evm/src/parallel_execute.rs` → `GrevmExecutor::transact_system_txn`

### 2. State migration: zero `SYSTEM_CALLER`'s balance at the fork block

At the Epsilon activation block, perform a deterministic state write `SYSTEM_CALLER.balance = 0`.
The nonce is preserved (> 0 → not empty → not pruned by EIP-161 state-clear).

**Status: implemented** — `crates/pipe-exec-layer-ext-v2/execute/src/epsilon.rs`, applied via the
backend-agnostic `ParallelExecutor::apply_state_change` in `execute_ordered_block` (right after the
EIP-2935 boundary change it mirrors). One call site → serial (`WrapExecutor`) and grevm get the
identical change, no per-backend duplication. The activation time is parsed from genesis
`epsilonTime` → `GravityHardfork::Epsilon` (`crates/chainspec/src/spec.rs`).

## Correctness checklist

- [x] serial + grevm `transact_system_txn` gas-exempt gate (Epsilon) — kept in lockstep (cf. #363)
- [x] zero `SYSTEM_CALLER.balance` at the Epsilon transition block (single backend-agnostic call)
- [x] `epsilonTime` genesis wiring → `GravityHardfork::Epsilon` (unit-tested) — else fork is inert
- [x] nonce preserved (account stays non-empty / EIP-161), no code/storage on the account
- [ ] confirm no contract/logic depends on `SYSTEM_CALLER`'s balance (it is an identity /
      `msg.sender`, not funds)
- [ ] coinbase unaffected (already no tip) — confirm in e2e
- [ ] gas limit still enforced (free != unbounded) — confirm in e2e
- [x] fork-gated; pre-fork behavior unchanged (gates default false with no `epsilonTime`)

## Test & rollout

- [x] unit: `epsilonTime` parsing + activation/transition semantics (`spec.rs` tests)
- [ ] e2e (testnet first, mirror `gravity_eip2935_test`): after fork — balance stays 0, system txs
      execute with correct receipts, no base fee burned, **serial == grevm state root**, nonce
      continuity, deterministic balance zeroing at the transition block, total supply returns to the
      real value
- [ ] staging → mainnet via the hardfork SOP (single PR + atomic image swap + >=24h lead, new fork
      activation time)
