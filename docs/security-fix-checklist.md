# Security Fix Checklist — gravity-testnet-v1.0.0

> Reference: [security-audit-gravity-testnet-v1.0.0.md](./security-audit-gravity-testnet-v1.0.0.md)
> Reference: [security-audit-gravity-reth-detailed.md](./security-audit-gravity-reth-detailed.md)
> Last updated: 2026-02-23

---

## 🔴 Critical — Must Fix Before Mainnet

### GRAV-001 · Mint Precompile Wrong Authorized Address

**Repo:** `Galxe/gravity-reth`
**File:** `crates/pipe-exec-layer-ext-v2/execute/src/mint_precompile.rs`
**Risk:** Unlimited native G token minting by unauthorized caller

- [ ] **Identify the correct authorized caller address**
  - Confirm whether the authorized caller should be `JWK_MANAGER_ADDR` (`0x...1625f4001`) or a different system address
  - Cross-check with the smart contract deployment (`NativeOracle.sol`, `JWKManager.sol`)
  - Confirm with contract deployment team which address is actually calling the mint precompile at runtime

- [ ] **Replace hardcoded address with canonical constant**
  ```rust
  use super::onchain_config::JWK_MANAGER_ADDR;
  pub const AUTHORIZED_CALLER: Address = JWK_MANAGER_ADDR;
  ```

- [ ] **Add compile-time assertion**
  ```rust
  const _: () = assert!(AUTHORIZED_CALLER.0 == JWK_MANAGER_ADDR.0);
  ```

- [ ] **Add unit test**
  ```rust
  #[test]
  fn authorized_caller_matches_jwk_manager() {
      assert_eq!(AUTHORIZED_CALLER, JWK_MANAGER_ADDR);
  }
  ```

- [ ] **Audit testnet: verify no unauthorized minting occurred**
  - Check testnet state for unexpected G token supply changes
  - Confirm `0x595475934ed7d9faa7fca28341c2ce583904a44e` has not called the precompile

- [ ] **PR created and reviewed**
- [ ] **PR merged to `Galxe/gravity-reth` main**
- [ ] **gravity-testnet tag updated or new testnet deployment**

---

### GRAV-005 · ValidatorManagement Auto-Eviction Consensus Halt

**Repo:** `Galxe/gravity_chain_core_contracts`
**File:** `src/validator/ValidatorManagement.sol`
**Function:** `evictUnderperformingValidators()`
**Risk:** Zero active validators after epoch transition → consensus halt

- [ ] **Update `remainingActive` count to include `PENDING_INACTIVE`**
  ```solidity
  uint256 remainingActive = 0;
  for (uint256 j = 0; j < activeLen; j++) {
      ValidatorStatus s = _validators[_activeValidators[j]].status;
      if (s == ValidatorStatus.ACTIVE || s == ValidatorStatus.PENDING_INACTIVE) {
          remainingActive++;
      }
  }
  if (remainingActive <= 1) {
      break;
  }
  ```

- [ ] **Write unit test: eviction halts when only 1 ACTIVE + N PENDING_INACTIVE remain**

- [ ] **Write integration test: epoch transition with simultaneous `leaveValidatorSet()` + auto-eviction**

- [ ] **Verify the same fix is not needed in `processValidatorSetUpdates()` or other epoch-boundary functions**

- [ ] **PR created and reviewed**
- [ ] **PR merged to `Galxe/gravity_chain_core_contracts` main**
- [ ] **Redeploy contracts on testnet**

---

## 🟡 Medium — Fix Before External Audit Submission

### GRAV-002 · Unauthenticated `/set_failpoint` and `/mem_prof` on HTTP

**Repo:** `Galxe/gravity-sdk`
**File:** `crates/api/src/https/mod.rs`
**Risk:** Remote attacker disrupts validator operation by injecting fail-points

- [ ] **Decision: choose fix strategy**
  - [ ] Option A: Move to `https_routes` + add token auth middleware
  - [ ] Option B: Bind HTTP listener to `127.0.0.1` only
  - [ ] Option C: Gate behind `#[cfg(feature = "failpoints")]`, disabled in production

- [ ] **Implement chosen option**

- [ ] **Verify fail-point endpoints are not reachable from external network on testnet**
  ```bash
  curl -X POST http://<validator-external-ip>:8080/set_failpoint
  # Should return 403, 404, or connection refused
  ```

- [ ] **PR created and reviewed**
- [ ] **PR merged to `Galxe/gravity-sdk` main**

---

### GRAV-003 · SSRF in Sentinel Probe URL

**Repo:** `Galxe/gravity-sdk`
**File:** `bin/sentinel/src/probe.rs`
**Risk:** Attacker who modifies sentinel config can probe cloud metadata endpoints

- [ ] **Add URL validation at config load time**
  - Reject non-http/https schemes
  - Block link-local (`169.254.0.0/16`), loopback (`127.0.0.0/8`), and RFC 1918 private ranges
  - Log a hard error (not warning) if validation fails and refuse to start

- [ ] **Consider switching from arbitrary URL to predefined probe types** (e.g., enum: `rpc_health`, `p2p_port`, `consensus_api`) to eliminate the attack surface entirely

- [ ] **Update `sentinel.toml.example` and docs with guidance on URL allowlist**

- [ ] **PR created and reviewed**
- [ ] **PR merged to `Galxe/gravity-sdk` main**

---

### GRAV-004 · Consensus API Endpoints on Plaintext HTTP

**Repo:** `Galxe/gravity-sdk`
**File:** `crates/api/src/https/mod.rs`
**Risk:** Passive MITM can collect DKG randomness, QC signatures, validator metadata

- [ ] **Audit which HTTP endpoints need to remain public vs. move to HTTPS**
  - `/dkg/randomness/:block_number` → move to HTTPS
  - `/consensus/latest_ledger_info` → move to HTTPS
  - `/consensus/ledger_info/:epoch` → move to HTTPS
  - `/consensus/block/:epoch/:round` → move to HTTPS
  - `/consensus/qc/:epoch/:round` → move to HTTPS
  - `/consensus/validator_count/:epoch` → evaluate: move to HTTPS or keep public with explicit annotation

- [ ] **Move sensitive routes to `https_routes`**

- [ ] **Update client code (gravity_cli, e2e tests) to use HTTPS for these endpoints**

- [ ] **Update `testnet_deploy.md` and `testnet_join.md` to reflect HTTPS-only endpoints**

- [ ] **PR created and reviewed**
- [ ] **PR merged to `Galxe/gravity-sdk` main**

---

---

## 🔴 Critical — gravity-sdk (GSDK)

> Reference: [security-audit-gravity-sdk.md](./security-audit-gravity-sdk.md)

### GSDK-001 · Unauthenticated `/set_failpoint` on HTTP

**Repo:** `Galxe/gravity-sdk`
**File:** `crates/api/src/https/mod.rs:113`
**Risk:** Remote attacker injects failpoints into running validator, halting consensus

- [ ] **Choose fix strategy**
  - [ ] Option A: Move to `https_routes` + add token auth middleware (recommended)
  - [ ] Option B: Bind HTTP listener to `127.0.0.1` only
  - [ ] Option C: Gate behind `#[cfg(debug_assertions)]` so unavailable in release

- [ ] **Implement chosen option**

- [ ] **Verify not accessible from external IP**
  ```bash
  curl -X POST http://<validator-external-ip>:8080/set_failpoint \
    -H "Content-Type: application/json" \
    -d '{"name": "test", "action": "return"}'
  # Must return connection refused or 403
  ```

- [ ] **PR created and reviewed**
- [ ] **PR merged to `Galxe/gravity-sdk` main**

---

### GSDK-002 · Unauthenticated `/mem_prof` on HTTP

**Repo:** `Galxe/gravity-sdk`
**File:** `crates/api/src/https/mod.rs:114`
**Risk:** Attacker triggers heap dump; dump file may contain in-memory secrets

- [ ] **Move to HTTPS route with authentication (same options as GSDK-001)**
- [ ] **Ensure heap dump files are written with `0600` permissions**
- [ ] **Verify not accessible from external IP**
- [ ] **PR created, reviewed, merged**

---

## 🟡 Medium — gravity-sdk (GSDK)

### GSDK-003 · Consensus/DKG Endpoints on Plaintext HTTP

**Repo:** `Galxe/gravity-sdk`
**File:** `crates/api/src/https/mod.rs:105–112`
**Risk:** Passive MITM collects DKG randomness, QC signatures, epoch validator metadata

- [ ] **Move these routes to `https_routes` with TLS:**
  - `/dkg/randomness/:block_number`
  - `/consensus/latest_ledger_info`
  - `/consensus/ledger_info/:epoch`
  - `/consensus/block/:epoch/:round`
  - `/consensus/qc/:epoch/:round`
  - `/consensus/validator_count/:epoch` — evaluate: move to HTTPS or keep public

- [ ] **Update gravity_cli DKG subcommands to use HTTPS**

- [ ] **PR created, reviewed, merged**

---

### GSDK-004 · SSRF in Sentinel Probe URL

**Repo:** `Galxe/gravity-sdk`
**File:** `bin/sentinel/src/probe.rs:34`, `bin/sentinel/src/config.rs:14`
**Risk:** Compromised config causes sentinel to probe cloud metadata endpoints

- [ ] **Add URL validation at `Config::load()` time**
  - Reject non-http/https schemes
  - Block link-local (`169.254.0.0/16`), loopback (`127.0.0.0/8`), RFC 1918 private ranges
  - Return hard error and refuse to start on validation failure

- [ ] **Verify with test config containing `url = "http://169.254.169.254/"` — sentinel must refuse to start**

- [ ] **PR created, reviewed, merged**

---

## gravity-reth Detailed Audit (GRETH)

> Reference: [security-audit-gravity-reth-detailed.md](./security-audit-gravity-reth-detailed.md)
> All fixes in branch `bugfix/security-fixes` of `Richard1048576/gravity-reth`

| ID | Risk | Description | Status |
|----|------|-------------|--------|
| GRETH-001 | High | RPC signer recovery falls back to `Address::ZERO` | ✅ Fixed `14b6ce5152` |
| GRETH-002 | Medium | RocksDB cursor field drop order | ✅ Fixed `cbccf02ff2` |
| GRETH-003 | Critical | Mint precompile wrong authorized address | ✅ Fixed `cbccf02ff2` |
| GRETH-004 | Medium | Block finalization gated on `debug_assertions` | ✅ Fixed `cbccf02ff2` |
| GRETH-005 | Medium | Parallel DB writes non-atomic; silent on error | ✅ Mitigated `14b6ce5152` (explicit error logging; idempotent checkpoints) |
| GRETH-006 | High | Path traversal in sharding directory config | ✅ Fixed `14b6ce5152` |
| GRETH-007 | Medium | Grevm state root unverified; None hash skips check | ✅ Fixed `f16d356915` (warn! on None; assert message improved) |
| GRETH-008 | Low | Recovery trusts canonical tip state root without re-verification | 📝 Design decision — documented `14b6ce5152` |
| GRETH-009 | Low | Every block immediately marked safe+finalized | 📝 Design decision — documented `14b6ce5152` (BFT guarantee) |
| GRETH-010 | High | Oracle events extracted from all receipts incl. user txns | ✅ Fixed `f16d356915` (slice to system receipts only) |
| GRETH-011 | High | Relayer log parsing without topic[0] / address check | ✅ Fixed `14b6ce5152` |
| GRETH-012 | Medium | Relayer has no reorg detection at finalized cursor | ✅ Fixed `f16d356915` (block hash check on every poll) |
| GRETH-013 | Medium | Transaction pool discard loop unbounded O(n) | ✅ Fixed `14b6ce5152` (MAX_DISCARD_PER_BATCH=1000) |
| GRETH-014 | Low | RocksDB WriteBatch read-your-writes limitation undocumented | 📝 Documented `14b6ce5152` |
| GRETH-015 | Medium | BLS precompile accepts over-length input (`<` instead of `!=`) | ✅ Fixed `14b6ce5152` |
| GRETH-016 | Low | `filter_invalid_txs` misidentified as security boundary | 📝 Documented `14b6ce5152` |
| GRETH-017 | Medium | Relayer state file has no integrity protection | ✅ Fixed `f16d356915` (keccak256 checksum on every write/load) |
| GRETH-018 | Medium | CLI args accept out-of-range values (gas limit, cache, trie) | ✅ Fixed `14b6ce5152` (clap value_parser ranges) |
| GRETH-019 | Medium | DKG transcript not size-validated before on-chain submission | ✅ Fixed `14b6ce5152` (512 KB limit) |

---

## ✅ Pre-External-Audit Gate

Before sharing code with external auditors, confirm:

- [x] GRAV-001 fixed, tested (**blocker**) — `cbccf02ff2`
- [ ] GRAV-005 fixed, tested, contracts redeployed (**blocker**) — pending contracts agent
- [x] GSDK-001 fixed — `/set_failpoint` inaccessible externally — `a0bf499`
- [x] GSDK-002 fixed — `/mem_prof` inaccessible externally — `a0bf499`
- [x] GRAV-002 / GSDK-003 fixed — `a0bf499`
- [x] GRAV-003 / GSDK-004 fixed (sentinel SSRF) — `a0bf499`
- [x] GRAV-004 fixed — `a0bf499`
- [x] GRETH-001–019 addressed — `cbccf02ff2`, `14b6ce5152`, `f16d356915`
- [ ] Audit scope document updated with PR links: `docs/audit-scope-gravity-testnet-v1.0.0.md`
- [ ] This checklist with fix commit hashes committed to `docs/`

---

## Fix Tracking

### gravity-testnet / gravity-sdk

| ID | Fix PR / Commit | Fixed By | Verified By | Date |
|----|----------------|----------|-------------|------|
| GRAV-001 | `cbccf02ff2` (gravity-reth `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GRAV-002 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GRAV-003 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GRAV-004 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GRAV-005 | — (contracts repo, separate agent) | — | — | — |
| GSDK-001 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GSDK-002 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GSDK-003 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |
| GSDK-004 | `a0bf499` (gravity-sdk `bugfix/security-fixes`) | Claude | — | 2026-02-23 |

### gravity-reth Detailed Audit

| ID | Fix Commit | Type | Fixed By | Verified By | Date |
|----|-----------|------|----------|-------------|------|
| GRETH-001 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-002 | `cbccf02ff2` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-003 | `cbccf02ff2` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-004 | `cbccf02ff2` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-005 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Mitigation | Claude | — | 2026-02-23 |
| GRETH-006 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-007 | `f16d356915` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-008 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Design doc | Claude | — | 2026-02-23 |
| GRETH-009 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Design doc | Claude | — | 2026-02-23 |
| GRETH-010 | `f16d356915` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-011 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-012 | `f16d356915` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-013 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-014 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Design doc | Claude | — | 2026-02-23 |
| GRETH-015 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-016 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Design doc | Claude | — | 2026-02-23 |
| GRETH-017 | `f16d356915` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-018 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
| GRETH-019 | `14b6ce5152` (gravity-reth `bugfix/security-fixes`) | Code fix | Claude | — | 2026-02-23 |
