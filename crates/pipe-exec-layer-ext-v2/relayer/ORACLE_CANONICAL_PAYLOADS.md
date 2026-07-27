# Oracle Relayer Protocol

The relayer converts deterministic external observations into bytes consumed by
the existing UnsupportedJWK consensus path. The production-candidate oracle
adapters are:

| sourceType | Adapter | Upstream |
| --- | --- | --- |
| `3` | `binance_index_kline_v1` | Binance USD-M closed index-price kline |
| `6` | `polymarket_settlement` | Finalized Polygon CTF resolution log |

The existing `sourceType=0` bridge event adapter remains independent from these
oracle products.

## Configuration split

The on-chain `gravity://` URI is the consensus task identity. It contains only
public deterministic parameters. The relayer JSON config maps the exact URI to
a validator-local upstream URL.

```json
{
  "uri_mappings": {
    "gravity://3/1001/price_feed?...": "https://provider.example",
    "gravity://6/7202626/polymarket_settlement?...": "https://polygon.example"
  }
}
```

Do not put API keys, tenant paths, URL userinfo, or provider URLs in the on-chain
URI. The Binance adapter rejects `baseUrl` as a URI parameter.

## Consensus wrapper

Every adapter returns one or more `OracleData` values:

```rust
OracleData {
    nonce: u128,
    payload: abi.encode(nonce, source_block_or_time, resolver_payload),
}
```

The JWK execution path unwraps this tuple and calls:

```solidity
NativeOracle.recordBatch(
    sourceType,
    sourceId,
    nonces,
    blockNumbers,
    resolverPayloads,
    callbackGasLimits
)
```

The execution layer assigns the standard callback budget to Binance records and
a larger budget to bounded Polymarket payout vectors. Failed callbacks do not
discard the raw `NativeOracle` record and can be replayed.

Delivery nonce is sequential for each `(sourceType, sourceId)`. It is not a
Binance round id and it is not a Polygon log index.

## Binance price feed

URI shape:

```text
gravity://3/<feedId>/price_feed
  ?provider=binance_index_kline_v1
  &pair=<PAIR>
  &interval=1m
  &bucketStartMs=<aligned start>
  &decimals=8
  &graceMs=<milliseconds>
```

The `binance_index_kline_v1` adapter is always continuous. Its accepted task
parameters are exactly `provider`, `pair`, `interval`, `bucketStartMs`,
`decimals`, and `graceMs`. Legacy aggregation and fixture parameters are
rejected.

For delivery nonce `n`:

```text
bucketStart(n) = configuredBucketStart + (n - 1) * intervalMs
bucketEnd(n)   = bucketStart(n) + intervalMs - 1
roundId(n)     = bucketStart(n) / intervalMs
resolvedAt(n)  = bucketEnd(n)
```

`bucketStartMs` is the bucket origin for nonce `1` and is immutable for the
lifetime of a `feedId`. Startup reconciliation rejects a confirmed cursor that
does not match this mapping. The interval is also immutable. Use a new
`feedId` when introducing a new origin or interval.

`round`, `resolvedAt`, and `blockNumber` are derived from the delivery bucket and
cannot be overridden in a Binance task URI.

The request is:

```text
GET /fapi/v1/indexPriceKlines
  ?pair=<PAIR>
  &interval=<interval>
  &startTime=<bucketStart>
  &endTime=<bucketEnd>
  &limit=1
```

Canonical acceptance rules:

- pair is 1-32 uppercase ASCII letters or digits
- interval is one of the fixed-duration Binance intervals supported in code
- bucket start is interval-aligned
- local time is at least `bucketEnd + graceMs`
- response has exactly one row
- row `openTime` and `closeTime` exactly match the requested bucket
- close price is a positive decimal string
- response body is streamed into a buffer capped at 64 KiB
- connection timeout is 5 seconds and total request timeout is 15 seconds
- decimals are at most 18

The resolver payload is the ABI encoding of one Binance close:

```solidity
struct PricePayload {
    uint256 feedId;
    uint64 roundId;
    uint64 resolvedAt;
    uint8 decimals;
    int256 price;
}
```

There is no provider weight, source count, threshold, aggregation mode, or
inline fixture in the source-type-3 protocol.

## Polymarket settlement mirror

URI shape:

```text
gravity://6/<mirrorId>/polymarket_settlement
  ?ctf=<CTF address>
  &condition=<conditionId>
  &fromBlock=<exclusive finalized cursor>
  &maxBlocksPerPoll=<bounded range>
```

`condition` is required. One source id represents one reviewed CTF condition;
the adapter does not scan all Polymarket settlements and ask the callback to
filter them later.

On first poll the RPC endpoint must report chain id `137`. Each poll then:

1. reads Polygon's finalized block number;
2. scans after the exclusive `fromBlock` cursor, at most 10,000 finalized
   blocks per poll;
3. filters the configured CTF address, `ConditionResolution` signature, and
   exact condition topic;
4. validates block number, log index, transaction hash, slot count (maximum
   32), and a
   non-zero payout vector;
5. sorts by `(blockNumber, logIndex, txHash)` and deduplicates identical logs;
6. rejects multiple distinct settlements for one condition and assigns one
   sequential Gravity delivery nonce.

Filtered logs with malformed ABI or missing source identity fail the poll and
do not advance the cursor. Once one settlement has been returned, the one-shot
source stops scanning; cached resend and restart reconciliation handle pending
consensus delivery.

The resolver payload contains mirror id, Polygon chain id, CTF and oracle
addresses, condition and question ids, payout vector, transaction hash, log
index, and settlement kind.

## Progress and retries

The source advances its in-memory cursor when it returns data. The SDK wrapper
caches that complete `PollResult`; while the returned nonce is ahead of
`NativeOracle.latestNonce`, it resends the cached bytes instead of polling the
upstream again.

Persisted relayer progress tracks fetched data, not confirmed data. On restart:

- on-chain ahead: fast-forward local state;
- persisted and on-chain equal: restore cursor;
- persisted ahead: roll back to the confirmed on-chain nonce and block;
- no state: start from configured cursor.

This contract between `gravity-sdk` and `gravity-reth` is required for liveness.
Do not change source cursor semantics without updating the cached-resend and
restart-reconciliation tests together.

## Verification

```bash
cargo test -p reth-pipe-exec-layer-relayer
cargo test -p reth-pipe-exec-layer-ext-v2 --lib jwk_oracle
```

Tests requiring public Binance or Polygon traffic are ignored by default. The
normal suite is deterministic and local.

### Optional live Polymarket check

After public-network access is explicitly approved, provide a Polygon RPC URL,
CTF address, exact condition id, and an exclusive cursor shortly before its
`ConditionResolution` event. Keep the scan range small enough for the provider.
Do not put the RPC URL in the `gravity://` URI or commit it.

```bash
POLYGON_RPC_URL='<rpc>' \
POLYMARKET_CTF_ADDRESS='<ctf>' \
POLYMARKET_CONDITION_ID='<condition>' \
POLYMARKET_FROM_BLOCK='<exclusive-cursor>' \
POLYMARKET_MAX_BLOCKS_PER_POLL='100' \
cargo test -p reth-pipe-exec-layer-relayer \
  test_live_poll_polygon_polymarket_settlements --lib -- --ignored --nocapture
```

A successful check proves that the endpoint reports chain id `137`, finalized
block lookup succeeds, and one matching resolution produces a canonical
payload whose source block, transaction hash, log index, and non-zero payout
vector match Polygon.

Wrong-chain endpoints and malformed matching logs fail closed. An empty result
usually means the condition, CTF address, exclusive cursor, or finalized height
does not cover the event; the provider must support the `finalized` block tag.
If a callback fails, the raw record remains available for
`replaySettlement(mirrorId, nonce)`.

The deterministic SDK suite covers the full consensus, execution, resolver,
market-settlement, and claim path:

```bash
./gravity_e2e/run_test.sh polymarket_mock --force-init
```
