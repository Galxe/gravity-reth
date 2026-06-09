
## 11. Storage/EVM transplant — compile-driven findings (2026-06-09)

Drove `cargo check` through the dependency-fork-backed transplant and
mapped each deep subsystem's true depth. The colleague's **merge-v1.11.3**
(PRs #316/#317, gravity's 1.8→1.11 merge, revm-34) is the authoritative
reference for the storage decisions.

**Resolved (compile-verified or workspace-resolving):**
- Full workspace dep graph resolves (4 Galxe forks via [patch], revm 38).
- Owned pipe-exec crates migrated off the removed `reth-primitives` crate.
- **Storage decision 1 — commit-view**: `DbTx::commit -> Result<bool>` +
  `commit_view()` (per merge-v1.11.3). mdbx/mock impls updated.
- **Storage decision 2 — subkey compression**: v2.2.0 externalized the
  `Compress` trait to crates.io `reth-codecs 0.3.1`; vendored reth-codecs
  back + added `subkey_compress_length`, deduped via `[patch.crates-io]`.
- **chainspec min-base-fee + gravity hardforks**: fully merged,
  `cargo check -p reth-chainspec` clean. ✅

**Deep subsystems — each needs its gravity author + Phase-6 state-root replay:**
- **Storage (reth-db / rocksdb)** — gravity's pre-v2.2.0 machinery (custom
  Compress-in-db-api, `SubkeyContainedValue`, local compression macros)
  must map onto v2.2.0's native machinery (externalized Compress,
  `reth_primitives_traits::ValueWithSubKey<SubKey>`, derive-Compact). The
  rocksdb dup-sort composite-key encoding is the consensus-critical part.
  Owner: AshinGau / nekomoto911 (rocksdb #316/#317).
- **EVM (grevm parallel execution)** — v2.2.0 made `EthEvmConfig` generic
  over `EvmFactory` (`EthEvmConfig<ChainSpec, EvmF>`, PR #16758); gravity's
  grevm integration is written for `EthEvmConfig<ChainSpec>`. Plus
  parallel-execution trait extensions (`Executor::{take_bundle,
  transact_system_txn, apply_state_change}`, `ConfigureEvm::parallel_executor`,
  `ParallelDatabase`) in reth-evm + v1.8.3→v2.2.0 API renames
  (`ExecutableTx`→`ExecutableTxParts`). Owner: AshinGau (grevm).
- **Engine (pipe-exec)** — the deep-5 storage/engine merges (database/
  provider state-root, persistence, tree/mod, tree/tests). Storage-dependent
  + pipe-exec event loop.

**Recurring structural cause:** v2.2.0 publishes foundational crates
(reth-codecs, reth-primitives-traits) to crates.io with partial/vestigial
path dirs, so each gravity addition to them must be vendored ([patch]) or
relocated — not applied in place. (e.g. `SubkeyContainedValue` relocated to
reth-trie-common; nested-trie algorithm moved to trie/db to break a
storage-errors dep cycle.)

**Net:** mechanical/localized parts (Cargo wiring, owned crates, chainspec,
the 84 take-HEAD conflicts) are done or doable solo. The 3 deep subsystems
are tightly-coupled, consensus-critical, and diverge enough from v2.2.0's
evolved native machinery that they should be done by the gravity storage/
grevm authors, gated on Phase-6 mainnet block-replay state-root parity.
