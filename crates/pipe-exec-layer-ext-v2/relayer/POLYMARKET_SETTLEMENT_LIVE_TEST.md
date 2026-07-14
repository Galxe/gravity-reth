# Polymarket Settlement Live-Test Runbook

The default relayer test suite is local and deterministic. This runbook is for
an explicitly approved Polygon RPC check of one known CTF condition.

## Inputs

Provide:

- Polygon RPC URL
- CTF contract address
- exact condition id
- a recent block before its `ConditionResolution` event
- a small `maxBlocksPerPoll` range appropriate for the RPC provider

Do not put the RPC URL into the `gravity://` URI or commit it to the repository.

## URI

```text
gravity://6/<mirrorId>/polymarket_settlement
  ?ctf=<ctfAddress>
  &condition=<conditionId>
  &fromBlock=<exclusiveCursorBeforeEvent>
  &maxBlocksPerPoll=<range>
```

The condition is mandatory. The adapter verifies `eth_chainId == 137` before
reading finalized logs. Scanning begins at `fromBlock + 1`.

## Focused adapter test

After outbound access is approved:

```bash
POLYGON_RPC_URL='<rpc>' \
POLYMARKET_CTF_ADDRESS='<ctf>' \
POLYMARKET_CONDITION_ID='<condition>' \
POLYMARKET_FROM_BLOCK='<block>' \
POLYMARKET_MAX_BLOCKS_PER_POLL='100' \
cargo test -p reth-pipe-exec-layer-relayer \
  test_live_poll_polygon_polymarket_settlements --lib -- --ignored --nocapture
```

Expected evidence:

- RPC chain id is accepted as `137`
- finalized block lookup succeeds
- one matching resolution produces one canonical payload
- source block, transaction hash, and log index match Polygon
- payout vector length equals `outcomeSlotCount` and is not all zero

## Full local chain test

Use the deterministic SDK suite for the consensus and execution path:

```bash
./gravity_e2e/run_test.sh polymarket_mock --force-init
```

That suite proves:

```text
finalized ConditionResolution fixture
-> gravity-reth canonical payload
-> UnsupportedJWK validator consensus
-> NativeOracle sourceType=6 record
-> PolymarketSettlementResolver
-> Polymarket market settlement and claim
```

## Failure handling

- Wrong chain id: reject the endpoint; do not override the check.
- Missing finalized tag: use a Polygon provider that implements finalized block
  queries.
- Empty result: verify condition topic, CTF address, start block, and finalized
  height.
- Callback failure: inspect the stored raw record and callback event, fix the
  configuration, then call `replaySettlement(mirrorId, nonce)`.
- Persisted progress ahead of chain state: restart reconciliation rolls back to
  `NativeOracle`'s confirmed nonce and source block.
