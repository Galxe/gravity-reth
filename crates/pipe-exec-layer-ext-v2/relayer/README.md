# Gravity Oracle Relayer Core

This crate converts finalized external-source observations into the
UnsupportedJWK payloads used by Gravity validator consensus. This core slice
implements source type `0` (`GravityPortal.MessageSent`). Provider-specific
source types are added in separate modules and PRs.

## Task Identity

Tasks use the following URI shape:

```text
gravity://<source_type>/<source_id>/<task_type>?<parameters>
```

The source type and source ID in the URI must match the coordinates under
which `OracleTaskConfig` registered the task. A relayer-backed
`(sourceType, sourceId)` has exactly one task because `NativeOracle` has one
nonce stream for that pair.

Source type `0` example:

```text
gravity://0/1/events?portal=0x0000000000000000000000000000000000000001&fromBlock=19000000
```

RPC URLs are local validator configuration. They are not stored in the task
URI or committed on-chain.

## Canonical Delivery

Each source observation becomes an `OracleData` value:

```text
nonce            strictly sequential source nonce
source_position  source-defined restart position
payload          callback payload
```

The relayer submits the canonical ABI wrapper:

```solidity
abi.encode(uint128 nonce, uint256 sourcePosition, bytes callbackPayload)
```

After quorum, the execution layer decodes the wrapper and calls the unchanged
`NativeOracle.recordBatch` ABI. Its `blockNumbers` argument carries source
positions. NativeOracle invokes the configured callback atomically and stores
only the latest `(nonce, sourcePosition)` progress checkpoint.

The execution adapter rejects:

- non-canonical ABI wrappers;
- mixed JWK variants;
- non-sequential batch nonces;
- source positions outside the NativeOracle `uint128` range;
- source types whose runtime provider is not compiled into the current core.

## Restart And Replay

The relayer persists three independent values per full task URI:

- the last locally returned nonce;
- the source position associated with that nonce;
- the latest scan cursor, including empty finalized scans.

Startup reconciles that local checkpoint with authoritative NativeOracle
progress. Local state ahead of the chain is rolled back. Local state behind a
known on-chain position is fast-forwarded.

Legacy NativeOracle state can contain `latestNonce > 0` with
`latestPosition == 0`. Zero means the old source position is unknown, not that
the source starts at block zero. Recovery uses the following watermarks:

| Local checkpoint | Recovery cursor |
|---|---|
| Same nonce | Persisted scan cursor |
| Behind with a known local event position | That local event position |
| Missing, empty, or ahead | Task `fromBlock` |

In every unknown-position case, the source starts with the authoritative
on-chain nonce and filters historical observations at or below it. The first
successful post-upgrade delivery establishes a known position.

Polls for the same URI are serialized. Different URIs can still poll in
parallel. This prevents concurrent observers from emitting the same local
scan range twice.

## Validation

```bash
cargo test -p reth-pipe-exec-layer-relayer
cargo test -p reth-pipe-exec-layer-ext-v2 onchain_config
cargo clippy -p reth-pipe-exec-layer-relayer -p reth-pipe-exec-layer-ext-v2 --all-targets
```

The ignored blockchain-source test requires an explicitly configured external
RPC and seeded events; it is not part of the offline unit suite.
