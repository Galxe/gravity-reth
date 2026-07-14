# Gravity Relayer

This crate hosts validator-local source adapters that produce canonical bytes
for Gravity's UnsupportedJWK consensus path.

Current runtime sources:

- `sourceType=0`: existing GravityPortal blockchain events
- `sourceType=3`: Binance closed index-price klines
- `sourceType=6`: finalized Polygon Polymarket CTF settlements

See [ORACLE_CANONICAL_PAYLOADS.md](./ORACLE_CANONICAL_PAYLOADS.md) for URI,
payload, nonce, and recovery invariants.

## Binance continuous feed

```text
gravity://3/<feedId>/price_feed?provider=binance_index_kline_v1&pair=TSLAUSDT&interval=1m&bucketStartMs=<alignedMs>&continuous=true&decimals=8&aggregationMode=2&minSourceCount=1&minTotalWeight=1&maxStaleness=180000&graceMs=120000
```

`bucketStartMs` identifies the first delivery bucket. Delivery nonce `n` maps
to that start plus `(n - 1) * intervalMs`. Validators request one exact closed
bucket from `/fapi/v1/indexPriceKlines` and reject mismatched timestamps.

The base URL comes from validator-local relayer JSON. It is not included in the
URI. Public `indexPriceKlines` requests do not use `BINANCE_API_KEY` or
`BINANCE_SECRET_KEY`.

## Polymarket mirror

```text
gravity://6/<mirrorId>/polymarket_settlement?ctf=<address>&condition=<bytes32>&fromBlock=<exclusive-cursor>&maxBlocksPerPoll=1000
```

The source checks RPC chain id `137`, reads only finalized blocks, and filters
one reviewed condition. A mirror task without `condition` is rejected. A
malformed filtered log fails closed without advancing the cursor.

## Local verification

```bash
cargo test -p reth-pipe-exec-layer-relayer
cargo test -p reth-pipe-exec-layer-ext-v2 --lib jwk_oracle
```

Ignored live tests require explicit public network access and are not part of
the normal test gate.
