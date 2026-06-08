# Gravity Hardfork Integration Testing

## Overview

`gravity_hardfork_test.rs` is a **single-node integration test** that verifies reth correctly performs system contract bytecode replacements (hardfork) at a specified block number.

## How It Works

```
Genesis (v1.0.0 old contracts) → run N blocks → gammaBlock triggers apply_gamma() → run M blocks → verify
```

1. **Build an old-version Genesis**: Check out a historical tag (e.g. `gravity-testnet-v1.0.0`) from the contracts repo, then run `scripts/generate_genesis_single.sh` to produce a `genesis.json` containing legacy contract bytecodes.
2. **Inject hardfork config**: Add `gravityHardforks.gammaBlock` to the genesis JSON's `config` object.
3. **Boot a reth node**: Start a single-node reth instance using the modified genesis.
4. **Push blocks via MockConsensus**: Use `PipeExecLayerApi` to push empty blocks across the `gammaBlock` boundary.
5. **Verify bytecode replacement**:
   - Before hardfork (`gammaBlock - 1`): contract bytecodes must still be the old version.
   - At hardfork (`gammaBlock`): contract bytecodes must match the new version.
   - After hardfork: the node must continue producing blocks normally.

## File Reference

| File | Description |
|------|-------------|
| `gravity_hardfork_test.rs` | Integration test code |
| `gravity_hardfork.json` | Legacy genesis with `gammaBlock` injected |
| `HARDFORK_TESTING.md` | This document |

Core code paths:

| File | Description |
|------|-------------|
| `crates/chainspec/src/gravity.rs` | `GravityHardfork` enum (Alpha/Beta/Gamma) |
| `crates/chainspec/src/spec.rs` | Parses `gravityHardforks.gammaBlock` from genesis JSON |
| `crates/chainspec/src/api.rs` | `gamma_transitions_at_block()` trait method |
| `crates/ethereum/evm/src/hardfork/gamma.rs` | Gamma bytecode constants and addresses |
| `crates/ethereum/evm/src/parallel_execute.rs` | `apply_gamma()` bytecode replacement logic |

## Generating the Test Genesis

```bash
# 1. Create a worktree at the old tag in the contracts repo
cd gravity_chain_core_contracts
git worktree add /tmp/gcc-v1.0.0 gravity-testnet-v1.0.0

# 2. Install dependencies (forge-std, openzeppelin, etc.)
cd /tmp/gcc-v1.0.0 && npm install

# 3. Generate genesis
bash scripts/generate_genesis_single.sh

# 4. Inject gammaBlock
python3 -c "
import json
with open('genesis.json') as f: g = json.load(f)
g['config']['gravityHardforks'] = {'gammaBlock': 20}
with open('gravity_hardfork.json', 'w') as f: json.dump(g, f, indent=2)
"

# 5. Copy to test directory
cp gravity_hardfork.json <reth>/crates/pipe-exec-layer-ext-v2/execute/

# 6. Clean up worktree
cd gravity_chain_core_contracts && git worktree remove /tmp/gcc-v1.0.0
```

## Running the Test

```bash
cd crates/pipe-exec-layer-ext-v2/execute
rm -rf data/gravity_hardfork_test   # Must clear stale data before each run
RUSTFLAGS="--cfg tokio_unstable" cargo test --test gravity_hardfork_test -- --nocapture
```

## Pitfalls and Lessons Learned

### 1. Genesis build requires node_modules
`generate_genesis_single.sh` depends on `node_modules` (forge-std, openzeppelin). After checking out an old tag, you **must** run `npm install` first — otherwise Forge compilation will fail.

### 2. `gravityHardforks` must be a top-level config key
reth's `alloy_genesis` handles unknown config fields via `#[serde(flatten)]` into `extra_fields`. This means `gravityHardforks` must be a top-level key inside the `config` object — it cannot be nested under any other field.

### 3. Contracts that didn't exist in the old genesis
Contracts added after v1.0.0 (e.g. `OracleRequestQueue` at 0x1625F4002) won't be present in the legacy genesis. `apply_gamma()` already handles this by checking `if let Some(ref info)` and skipping missing accounts. The test must tolerate this as well.

### 4. StakePool addresses are dynamically created
StakePool is **not** a pre-deployed system contract at a fixed address. It is dynamically created via `CREATE` during `Genesis.initialize` in the genesis-tool. The address is deterministic but not in the `0x1625f` system range. To find it, look for accounts in the genesis `alloc` that have `code` but are outside the system address range.

### 5. ReentrancyGuard storage must be initialized
The new StakePool introduces OpenZeppelin v5's `ReentrancyGuardUpgradeable`, which uses ERC-7201 namespaced storage. When replacing the bytecode at hardfork time, the ReentrancyGuard storage slot **must** be initialized to `NOT_ENTERED` (1). Without this, the very first call to any guarded function will revert with `ReentrancyGuardReentrantCall`.

### 6. StateProvider::storage() returns Option
`StateProvider::storage()` returns `Result<Option<U256>>`, not `Result<U256>`. When asserting storage values, wrap the expected value in `Some(...)`.

### 7. Data directory must be cleaned between runs
reth persists its RocksDB state under `data/gravity_hardfork_test/`. Leftover data from a previous run will cause genesis initialization conflicts. Always `rm -rf data/gravity_hardfork_test` before each test run.

## Guide: Adding a New Hardfork Test

Using a hypothetical "Delta" hardfork as an example:

### 1. Code Changes

```
crates/chainspec/src/gravity.rs             → Add Delta variant to GravityHardfork enum
crates/chainspec/src/spec.rs                → Parse gravityHardforks.deltaBlock
crates/chainspec/src/api.rs                 → Add delta_transitions_at_block()
crates/ethereum/evm/src/hardfork/delta.rs   → New file with updated bytecode constants
crates/ethereum/evm/src/parallel_execute.rs → Add apply_delta() call
```

### 2. Test Data

- Pick a tag **after** Gamma as the "old version" and regenerate the genesis.
- Alternatively, reuse `gravity_hardfork.json` and append `"deltaBlock": N` to the config.
- To test both hardforks in sequence, set both `gammaBlock` and `deltaBlock` in the same genesis.

### 3. Test Code

- Either extend `gravity_hardfork_test.rs` with Delta verification logic, or create a separate `gravity_delta_hardfork_test.rs` for isolation.
- The verification pattern is identical: old bytecodes before → new bytecodes at hardfork block → normal block execution after.

### 4. Things to Watch For

- If new contracts are being **deployed** (not just upgraded), `apply_delta()` needs to insert a new account rather than just replacing bytecode on an existing one.
- If storage layout changes are involved (e.g. new storage slots), the initial values must be written in `apply_delta()`.
- StakePool and other dynamically-created contract addresses must be enumerated from on-chain data and hardcoded into constants before deployment.
