# Gravity Relayer

This crate hosts validator-local source adapters that produce canonical bytes
for Gravity's UnsupportedJWK consensus path.

Supported runtime sources are:

- `sourceType=0`: existing GravityPortal blockchain events
- `sourceType=3`: Binance closed index-price klines
- `sourceType=6`: finalized Polygon Polymarket CTF settlements

The canonical [Oracle Relayer Protocol](./ORACLE_CANONICAL_PAYLOADS.md)
documents task URIs, payloads, nonce and cursor semantics, recovery behavior,
and the optional live Polymarket check.

## Local verification

```bash
cargo test -p reth-pipe-exec-layer-relayer
cargo test -p reth-pipe-exec-layer-ext-v2 --lib jwk_oracle
```

Public-network tests are ignored by default.
