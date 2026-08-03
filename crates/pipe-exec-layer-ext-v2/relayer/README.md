# Gravity Oracle Relayer Core

This crate converts finalized external-source observations into the
UnsupportedJWK payloads used by Gravity validator consensus. This core slice
implements source type `0` (`GravityPortal.MessageSent`), source type `3`
(Binance USD-M index-price klines), and source type `6` (finalized Polygon CTF
settlements).

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

Source type `3` example:

```text
gravity://3/2001/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=1710000000000&decimals=8&graceMs=120000
```

Source type `6` example:

```text
gravity://6/9001/polymarket_settlement?ctf=0x4D97DCd97eC945f40cF65F87097ACe5EA0476045&condition=0x...&fromBlock=89000000&chainId=137&maxBlocksPerPoll=1000
```

See [POLYMARKET_SETTLEMENT_SOURCE.md](POLYMARKET_SETTLEMENT_SOURCE.md) for the
market-discovery boundary, finalized scan semantics, cursor/idempotency rules,
and exact resolver ABI.

RPC URLs are local validator configuration. They are not stored in the task
URI or committed on-chain.

For Binance, the local URL is the USD-M Futures base URL. The adapter appends
`/fapi/v1/indexPriceKlines` and the deterministic query parameters. The public
index-kline endpoint does not require API credentials. `baseUrl` and unknown
query parameters are rejected in the on-chain URI.

## Binance Index Price Delivery

For one `(sourceType=3, feedId)` task, delivery nonce `n` maps to exactly one
bucket:

```text
bucketStart(n) = configuredBucketStart + (n - 1) * interval
bucketEnd(n)   = bucketStart(n) + interval - 1
sourcePosition = bucketEnd(n)
roundId        = bucketStart(n) / interval
resolvedAt     = bucketEnd(n)
```

The adapter waits until `bucketEnd + graceMs`, requests exactly that bucket,
and accepts exactly one response row whose open and close timestamps match.
The callback payload is:

```solidity
abi.encode(
    uint256 feedId,
    uint64 roundId,
    uint64 resolvedAt,
    uint8 decimals,
    int256 price
)
```

The decimal parser rejects negative, zero, malformed, overflowing, or
non-representable prices. HTTP connect/request timeouts and a 64 KiB response
limit bound validator resource use. Supported fixed intervals are `1m`, `3m`,
`5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, and `3d`.

Changing the interval or bucket origin for an active feed changes the
nonce-to-position history. Registration and runtime reconciliation reject that
mismatch; deploy the changed task under a new `feedId`.

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
