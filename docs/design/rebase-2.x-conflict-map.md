
## 12. 1.11-base transplant — compile progress (2026-06-09)

选型 pivot executed: the transplant baseline moved from gravity-1.8.3 to
**gravity-1.11 (merge-v1.11.3, PRs #316/#317)**, measured at **45 conflicts
vs 111** with the deep subsystems already author-merged at revm-34.
Branch: `feat/transplant-reth-2.2.0-1.11`, foundation tag
`gravity-base/v1.11-clean-ancestry`.

Compile status (cargo check, revm-38 forks via [patch]):

| crate | status | notes |
|---|---|---|
| reth-db (RocksDB backend) | ✅ | local-Compress architecture kept (orphan problem dissolves) |
| reth-db-api | ✅ | scale.rs restored; subkey list + PackedStorageTrieEntry(33B) |
| reth-trie-db / trie-common | ✅ | SubkeyContainedValue relocated to trie-common |
| reth-provider | ✅ | NestedStateRoot + TrieWriterV2 state-root path |
| **reth-engine-tree** | ✅ | **pipe-exec engine both-sides merged** (struct carries gravity's use_hashed_state/persistence_waiters + v2.2.0's timing-stats/runtime; spawn_new takes both) |
| reth-evm-ethereum | ✅ | grevm parallel execution on revm-38 forks |
| bin/reth | 🔧 | full-workspace check in flight |

Removed as obsolete (v2.2.0 natively provides): gravity's 1.11-era
sparse-parallel crate (ParallelSparseTrie now in reth-trie-sparse),
configured_sparse_trie.rs / executor.rs orphans, StateCommitment
abstraction.

NEXT after bin/reth greens: cargo +nightly fmt, full-workspace clippy/test
triage, then Phase-6 mainnet block-replay state-root parity (consensus
gate) + pipe-exec author review of the engine merge.

## 13. MILESTONE: full workspace compiles (2026-06-09)

`cargo check --workspace` (excl. reth-bb / bench tools): **0 errors**, on
`feat/transplant-reth-2.2.0-1.11`. The complete gravity-reth — RocksDB
storage, NestedStateRoot/TrieV2, pipe-exec engine, grevm parallel
execution, all precompiles, rpc, node-builder, the `reth` binary, and all
examples — compiles against upstream reth v2.2.0 + the Galxe revm-38 forks.

Final-stretch adaptations: gravity precompiles (bls PoP-verify + G mint)
migrated to revm 38's reworked precompile interface (soft input-validation
failures are now non-fatal `PrecompileHalt`s at the provider level —
`PrecompileError` is fatal-only; success uses `PrecompileOutput::new`);
NextBlockEnvAttributes gains `slot_number: None` (post-Amsterdam, N/A to
Gravity); removed v2.2.0-obsoleted carriers (stateless, ress, mdbx-only
migrate-v2 subcommand); greth root re-exports updated.

NOT yet done (the verification phase):
1. `cargo nextest` triage across the workspace (unit/integration tests).
2. Phase-6 mainnet block-replay state-root parity — the consensus gate.
3. Author review: pipe-exec engine both-sides merge (tree/mod.rs),
   GRETH-001 system-tx receipt fallback relocation, rocksdb dup-sort
   subkey paths (incl. PackedStorageTrieEntry's 33-byte prefix).
4. Pin fork branches to commit SHAs before any rollout.
