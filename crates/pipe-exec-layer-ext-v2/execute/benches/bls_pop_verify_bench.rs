//! Benchmark for BLS12-381 PoP verification precompile.
//!
//! Measures the execution overhead of `verify_pop` and `bls_pop_verify_handler_raw`
//! to inform the `POP_VERIFY_GAS` constant.

use criterion::{criterion_group, criterion_main, Criterion};
use reth_pipe_exec_layer_ext_v2::bls_precompile;

/// Hardcoded valid BLS12-381 keypair for deterministic benchmarking.
///
/// private_key: 460dac87e9aebc8e82ec532b28419b3280eb58aa981e3f488bf6d0d596935c02
const VALID_PUBKEY: [u8; 48] = hex_literal::hex!(
    "b695d45916dc22714dbeed3e3456abf38f2f284f153d7284252b35fc7a04600a"
    "84373693994712fbb39ee7a54fc865b7"
);

const VALID_POP: [u8; 96] = hex_literal::hex!(
    "88a8a501e6895f0037095e61c09e2c9cdf8600c7fce10851177221a4c9f6fe83"
    "75662ac3c7b0220f0146d1cbb5a24533129bf981d69685e668ee4495a59e4d31"
    "4efc80ce427e4bc163b694ccb11fd82e5966191c5ec7d749987ccd66f45eecc6"
);

fn bench_bls_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("bls_pop_verify");

    // 1. Valid PoP — full happy path (2 pairings + hash-to-curve)
    group.bench_function("verify_pop_valid", |b| {
        b.iter(|| {
            let result = bls_precompile::verify_pop(&VALID_PUBKEY, &VALID_POP);
            assert!(result);
        });
    });

    // 2. Invalid PoP — pairing check fails (same cost as valid minus short-circuit)
    let mut bad_pop = VALID_POP;
    bad_pop[0] ^= 0xff;
    group.bench_function("verify_pop_invalid_pop", |b| {
        b.iter(|| {
            let result = bls_precompile::verify_pop(&VALID_PUBKEY, &bad_pop);
            assert!(!result);
        });
    });

    // 3. Invalid pubkey — early exit at deserialization
    let mut bad_pubkey = VALID_PUBKEY;
    bad_pubkey[0] ^= 0xff;
    group.bench_function("verify_pop_invalid_pubkey", |b| {
        b.iter(|| {
            let result = bls_precompile::verify_pop(&bad_pubkey, &VALID_POP);
            assert!(!result);
        });
    });

    // 4. Full precompile handler (input parsing + verify + ABI encoding)
    let mut full_input = Vec::with_capacity(144);
    full_input.extend_from_slice(&VALID_PUBKEY);
    full_input.extend_from_slice(&VALID_POP);
    group.bench_function("handler_raw_valid", |b| {
        b.iter(|| {
            let result = bls_precompile::bls_pop_verify_handler_raw(&full_input).unwrap();
            assert_eq!(result.bytes[31], 1);
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_bls_pop
}
criterion_main!(benches);
