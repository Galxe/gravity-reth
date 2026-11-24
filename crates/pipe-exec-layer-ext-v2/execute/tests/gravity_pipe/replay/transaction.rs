//! Single-transaction replays: every transaction of the block, traced on its own, must
//! match its committed receipt, and its reported error must match its trace.
//!
//! Each endpoint is compared on what it exposes:
//! - `debug_traceTransaction` (call tracer): the root frame's gas used and error;
//! - `trace_replayTransaction`: the root trace's error. Its `gasUsed` is not the receipt's: reth
//!   (`crates/rpc/rpc/src/trace.rs`) never sets the transaction's gas used on the root trace
//!   (revm-inspectors' `ParityTraceBuilder::set_transaction_gas_used`), so the builder reports the
//!   root call frame's gas, without intrinsic gas and refunds;
//! - `trace_transaction`, `trace_get` (empty path): the root trace's transaction hash, position and
//!   error (same gas caveat);
//! - `trace_transactionOpcodeGas`: the transaction hash (opcode gas excludes intrinsic gas and
//!   refunds);
//! - `ots_traceTransaction`: a non-empty trace (entries carry neither gas nor status);
//! - `ots_getInternalOperations`: nothing beyond a result (transfers carry neither);
//! - `ots_getTransactionError`: the revert output (see `check_transaction_error`).
//!
//! Anywhere else, an error or a missing trace is the mismatch.

use super::{
    call_tracer_options, check_call_frame, committed::Committed, present_or_record,
    result_or_record,
};
use crate::{report::BlockReport, rpc::RpcClient};
use alloy_primitives::Bytes;
use alloy_rpc_types_trace::{
    geth::CallFrame,
    opcode::TransactionOpcodeGas,
    otterscan::{InternalOperation, TraceEntry},
    parity::{LocalizedTransactionTrace, TraceResults},
};
use serde_json::json;

pub(super) fn check_transaction(
    report: &mut BlockReport<'_>,
    rpc: &RpcClient,
    committed: &Committed,
    index: usize,
) {
    let hash = committed.tx_hashes[index];
    let success = committed.receipts[index].success;

    // Step 1: the geth-style trace carries the receipt's gas and status.
    let endpoint = "debug_traceTransaction";
    let response = rpc.call::<CallFrame>(endpoint, json!([hash, call_tracer_options()]));
    let frame = result_or_record(report, endpoint, Some(index), response);
    if let Some(frame) = &frame {
        check_call_frame(report, endpoint, committed, index, frame);
    }

    // Step 2: parity-style traces carry the status in their root trace.
    let endpoint = "trace_replayTransaction";
    let response = rpc.call::<TraceResults>(endpoint, json!([hash, ["trace"]]));
    if let Some(results) = result_or_record(report, endpoint, Some(index), response) {
        let root_success = results.trace.first().map(|root| root.error.is_none());
        report.check_eq(endpoint, Some(index), "success", Some(success), root_success);
    }
    let endpoint = "trace_transaction";
    let response = rpc.call::<Option<Vec<LocalizedTransactionTrace>>>(endpoint, json!([hash]));
    if let Some(traces) = present_or_record(report, endpoint, Some(index), "response", response) {
        check_parity_root(report, endpoint, committed, index, traces.first());
    }
    let endpoint = "trace_get";
    let response = rpc.call::<Option<LocalizedTransactionTrace>>(endpoint, json!([hash, []]));
    if let Some(root) = present_or_record(report, endpoint, Some(index), "response", response) {
        check_parity_root(report, endpoint, committed, index, Some(&root));
    }

    // Step 3: endpoints that expose neither gas nor status.
    let endpoint = "trace_transactionOpcodeGas";
    let response = rpc.call::<Option<TransactionOpcodeGas>>(endpoint, json!([hash]));
    if let Some(opcode_gas) = present_or_record(report, endpoint, Some(index), "response", response)
    {
        let actual = opcode_gas.transaction_hash;
        report.check_eq(endpoint, Some(index), "transaction hash", hash, actual);
    }
    let endpoint = "ots_traceTransaction";
    let response = rpc.call::<Option<Vec<TraceEntry>>>(endpoint, json!([hash]));
    if let Some(entries) = present_or_record(report, endpoint, Some(index), "response", response) &&
        entries.is_empty()
    {
        report.record(endpoint, Some(index), "trace entries", "the root call", "none");
    }
    let endpoint = "ots_getInternalOperations";
    let response = rpc.call::<Vec<InternalOperation>>(endpoint, json!([hash]));
    result_or_record(report, endpoint, Some(index), response);

    // Step 4: the reported error is the revert output the trace shows.
    let endpoint = "ots_getTransactionError";
    let response = rpc.call::<Option<Bytes>>(endpoint, json!([hash]));
    check_transaction_error(report, endpoint, committed, index, frame.as_ref(), response);
}

/// The transaction's root trace: first in the flat trace list, with an empty trace address.
fn check_parity_root(
    report: &mut BlockReport<'_>,
    endpoint: &'static str,
    committed: &Committed,
    index: usize,
    root: Option<&LocalizedTransactionTrace>,
) {
    let Some(root) = root else {
        report.record(endpoint, Some(index), "root trace", "a trace", "none");
        return;
    };
    let tx_hash = Some(committed.tx_hashes[index]);
    report.check_eq(endpoint, Some(index), "transaction hash", tx_hash, root.transaction_hash);
    let position = Some(index as u64);
    report.check_eq(endpoint, Some(index), "position", position, root.transaction_position);
    let trace_address = &root.trace.trace_address;
    report.check_eq(endpoint, Some(index), "trace address", &Vec::new(), trace_address);
    let success = root.trace.error.is_none();
    report.check_eq(endpoint, Some(index), "success", committed.receipts[index].success, success);
}

/// `ots_getTransactionError` returns a reverted transaction's output and nothing
/// otherwise; `null` and empty bytes both read as nothing. The receipt only records that
/// a transaction failed, so a failed transaction's expected output is the root output of
/// its call trace, which likewise holds a revert's output and nothing for a halt.
fn check_transaction_error(
    report: &mut BlockReport<'_>,
    endpoint: &'static str,
    committed: &Committed,
    index: usize,
    frame: Option<&CallFrame>,
    response: Result<Option<Bytes>, String>,
) {
    let Some(actual) = result_or_record(report, endpoint, Some(index), response) else { return };
    let expected = if committed.receipts[index].success {
        Bytes::new()
    } else {
        // Without a trace the expected output is unknown; the trace error is recorded.
        let Some(frame) = frame else { return };
        frame.output.clone().unwrap_or_default()
    };
    report.check_eq(endpoint, Some(index), "revert output", expected, actual.unwrap_or_default());
}
