#![allow(missing_docs)]

//! Gravity pipe execution, driven along the mainnet hardfork timeline.
//!
//! One node starts from mainnet genesis and walks genesis → Prague → Alpha → Beta →
//! Gamma in wall-clock time. Every block is executed by the pipe (grevm), committed,
//! and persisted before the next one is built. Epoch changes happen throughout the
//! timeline, never on a hardfork activation block. After every block, and again for every
//! block once the node's tip is past every fork, each RPC endpoint that replays it must
//! reproduce the committed result; differences are collected and fail the test once the
//! timeline is done.

mod config_reads;
mod hardfork;
mod node;
mod replay;
mod report;
mod rpc;
mod timeline;

use gravity_storage::block_view_storage::BlockViewStorage;
use hardfork::{Chain, ScenarioBlock, Scenarios};
use node::{BlockInput, Builder, Node};
use report::MismatchReport;
use reth_node_builder::EngineNodeLauncher;
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};
use reth_pipe_exec_layer_ext_v2::{new_pipe_exec_layer_api, ExecutionArgs};
use reth_provider::{
    providers::BlockchainProvider, BlockHashReader, HeaderProvider, ProviderReadView,
};
use rpc::RpcClient;
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use timeline::{Fork, Phase, Timeline};

const DATADIR: &str = "data/gravity_pipe";

/// Sleep between two blocks; block timestamps are the wall-clock time at build.
const BLOCK_INTERVAL: Duration = Duration::from_secs(1);

/// Longest a block may take to be executed, committed and persisted. The slowest take a few
/// seconds; a pipe that hangs fails the run with the block's number instead of waiting for
/// nextest to kill it without a reason.
const BLOCK_TIMEOUT: Duration = Duration::from_secs(30);

const MIN_EPOCH_CHANGES: usize = 5;

#[test]
fn gravity_pipe() {
    let timeline = Timeline::starting_now();
    let genesis = timeline.genesis_json();
    node::run_node(&genesis, DATADIR, move |builder| run_timeline(builder, timeline));
}

async fn run_timeline(builder: Builder, timeline: Timeline) -> eyre::Result<()> {
    // Step 1: launch reth and attach the pipe execution layer at genesis.
    let handle = builder
        .with_types_and_provider::<EthereumNode, BlockchainProvider<_>>()
        .with_components(EthereumNode::gravity_components())
        .with_add_ons(EthereumAddOns::default())
        .launch_with_fn(|builder| {
            let launcher = EngineNodeLauncher::new(
                builder.task_executor().clone(),
                builder.config().datadir(),
                reth_engine_primitives::TreeConfig::default(),
            );
            builder.launch_with(launcher)
        })
        .await?;
    let chain_spec = handle.node.chain_spec();
    let eth_api = handle.node.rpc_registry.eth_api().clone();
    let rpc_addr =
        handle.node.rpc_server_handle().http_local_addr().expect("node runs with --http");
    let rpc = RpcClient::new(rpc_addr);
    let provider = handle.node.provider;

    let genesis_header = provider.header_by_number(0)?.unwrap();
    let genesis_hash = provider.block_hash(0)?.unwrap();
    let (args_tx, args_rx) = tokio::sync::oneshot::channel();
    let pipe = new_pipe_exec_layer_api(
        chain_spec.clone(),
        BlockViewStorage::new(provider.clone()),
        genesis_header.clone(),
        genesis_hash,
        args_rx,
        eth_api.clone(),
    );
    args_tx.send(ExecutionArgs { block_number_to_block_id: BTreeMap::new() }).unwrap();
    let mut node = Node::new(pipe, &eth_api, genesis_header.timestamp).await;
    let chain = Chain::new(&rpc, chain_spec.chain.id());

    // Step 2: produce blocks until the last phase has changed epoch, or the schedule
    // runs out; replay each block right after it is committed, then check its scenarios.
    let mut epoch_changes: Vec<(u64, Phase)> = Vec::new();
    let mut blocks = Vec::new();
    let mut scenarios = Scenarios::default();
    let mut report = MismatchReport::default();
    loop {
        tokio::time::sleep(BLOCK_INTERVAL).await;
        let timestamp_us = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        let parent_timestamp = node.parent_timestamp();
        let phase = timeline.phase(timestamp_us / 1_000_000, parent_timestamp);

        // A pending DKG transcript takes the first block that may change the epoch, so
        // scenario blocks never push an epoch change toward the end of a phase. Mainnet never
        // changed epoch on an activation block, and an epoch-change block drops user
        // transactions: scenarios get all remaining blocks.
        let epoch_change_due = node.dkg_in_progress() && !matches!(phase, Phase::Activation(_));
        let scenario = if epoch_change_due {
            None
        } else {
            tokio::task::block_in_place(|| {
                scenarios.next_block(&chain, phase, node.parent_number())
            })
        };
        let number = node.parent_number() + 1;
        let input = BlockInput {
            timestamp_us,
            may_change_epoch: epoch_change_due,
            ..scenario.map(ScenarioBlock::into_input).unwrap_or_default()
        };
        let block =
            tokio::time::timeout(BLOCK_TIMEOUT, node.produce_block(input)).await.unwrap_or_else(
                |_| panic!("block {number} ({phase}) not persisted within {BLOCK_TIMEOUT:?}"),
            );

        // The chain spec must agree with the timeline on which block activates a fork.
        for fork in Fork::ALL {
            assert_eq!(
                fork.transitions_at(&chain_spec, block.timestamp, parent_timestamp),
                phase == Phase::Activation(fork),
                "block {} ({phase}): chain spec disagrees on {fork:?} activation",
                block.number,
            );
        }
        if block.epoch_changed {
            // The first epoch change also shows that a block of the old epoch is rejected.
            if epoch_changes.is_empty() {
                node.assert_old_epoch_block_rejected().await;
            }
            epoch_changes.push((block.number, phase));
            config_reads::check_config_reads(
                node.pipe(),
                block.number,
                block.epoch,
                &mut report.for_block(block.number, phase),
            );
        }

        // The RPC client blocks on HTTP while the node serves it from its own runtime.
        tokio::task::block_in_place(|| {
            let provider = provider.consistent_view().unwrap();
            let mut block_report = report.for_block(block.number, phase);
            replay::check_block(&provider, &rpc, &block, phase, &mut block_report);
            scenarios.after_commit(&chain, &block, &mut block_report);
        });
        println!(
            "[gravity_pipe] block {} at {} ({phase}){}; {} mismatches so far",
            block.number,
            block.timestamp,
            if block.epoch_changed { ", epoch changed" } else { "" },
            report.len(),
        );

        let done = phase == Phase::After(Fork::Gamma) && block.epoch_changed ||
            block.timestamp >= timeline.deadline();
        blocks.push((block, phase));
        if done {
            break;
        }
    }

    // Step 3: every scenario got its blocks, every phase saw an epoch change, and there were
    // enough of them overall. A failed assertion exits the process, so the mismatches found so
    // far are printed first.
    report.print();
    scenarios.assert_all_ran();
    let required_phases =
        std::iter::once(Phase::Genesis).chain(Fork::ALL.into_iter().map(Phase::After));
    for phase in required_phases {
        assert!(
            epoch_changes.iter().any(|(_, p)| *p == phase),
            "no epoch change in phase {phase}; {timeline:?}; epoch changes: {epoch_changes:?}"
        );
    }
    assert!(
        epoch_changes.len() >= MIN_EPOCH_CHANGES,
        "only {} epoch changes: {epoch_changes:?}",
        epoch_changes.len()
    );

    // Step 4: every block goes through the replay check again, now that the tip is past every
    // fork. A replay must follow the replayed block's own timestamp and parent state (hardfork
    // gating, precompile set, gas exemption, Gamma migration), not the node's latest block;
    // right after commit the two coincide, so only this pass tells them apart.
    tokio::task::block_in_place(|| {
        let provider = provider.consistent_view().unwrap();
        for (block, phase) in &blocks {
            let mut block_report = report.for_block_after_timeline(block.number, *phase);
            replay::check_block(&provider, &rpc, block, *phase, &mut block_report);
        }
    });

    // Step 5: replays spanning many blocks reproduce them too.
    tokio::task::block_in_place(|| {
        let provider = provider.consistent_view().unwrap();
        replay::check_blocks(&provider, &rpc, &blocks, &mut report)
    });

    // Step 6: every replay reproduced the committed blocks.
    report.assert_empty();
    Ok(())
}
