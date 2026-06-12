
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
