# Gravity Genesis Fixture Regeneration

This directory is the source of truth for the checked-in
`../gravity_hardfork.json` fixture used by the Gravity pipe, Prague, system
transaction, BLS, and hardfork integration tests.

`test_genesis.toml` pins the contracts repository ref and the reth-side
hardfork knobs. `validator_genesis.json` is the compact `GenesisConfig` input
for the contracts repo `genesis-tool`; it is intentionally committed so schema
changes in `genesis-tool` fail loudly in CI instead of silently changing the
fixture shape.

Regenerate locally with an existing contracts checkout:

```bash
crates/pipe-exec-layer-ext-v2/execute/fixtures/regen.sh \
  --contracts-dir /path/to/gravity_chain_core_contracts
```

The script creates an isolated git worktree at the ref pinned in
`test_genesis.toml`, so it does not modify the supplied contracts checkout.
Without `--contracts-dir`, it clones the pinned contracts ref into a temporary
directory. Full regeneration requires `forge`, `cargo`, `npm`, `git`, and
`python3`.

To verify the committed fixture without rewriting it:

```bash
crates/pipe-exec-layer-ext-v2/execute/fixtures/regen.sh --check
```

CI caches the generated `gravity_hardfork.json` artifact by hashing this
directory. A cache hit restores the artifact and compares it to the committed
fixture; a cache miss regenerates it with Foundry, Node dependencies, and the
standalone contracts `genesis-tool`.

`../gravity.json` remains a separate legacy fixture for tests that do not need
the hardfork schedule. Keep it committed until those tests are migrated to a
second regeneration profile.
