//! Moves block history between ERA files ([`reth_era`]) and node storage ([`reth_storage_api`]).
//!
//! Each ERA format plugs into a shared pipeline through a per-format seam ([`EraBlockReader`]).

mod history;

mod export;

pub use export::{export, export_as, EraBlockWriter, ExportBlock, ExportConfig};

pub use history::{
    build_index, calculate_td_by_number, decode, decode_with_receipts, import, import_with_options,
    open, process, process_iter, process_iter_with_receipts, process_with_receipts,
    save_stage_checkpoints, Era, Era1, EraBlockReader, EraBlockReaderWithReceipts, Ere,
    ImportPolicy,
};
