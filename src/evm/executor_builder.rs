//! ExecutorBuilder 实现，使用 MintEvmFactory

use reth_node_builder::{
    components::ExecutorBuilder,
    node::{FullNodeTypes, NodeTypes},
    BuilderContext,
};
use reth_ethereum::EthPrimitives;
use reth_evm_ethereum::EthEvmConfig;
use reth_chainspec::ChainSpec;
use tracing::info;

/// ExecutorBuilder，使用 MintEvmFactory（默认）
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct MintExecutorBuilder;

impl<Node> ExecutorBuilder<Node> for MintExecutorBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
{
    type EVM = EthEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        info!(target: "evm::executor_builder", "Building EVM with MintEvmFactory (default)");
        
        // EthEvmConfig::new() 现在默认使用 MintEvmFactory
        let evm_config = EthEvmConfig::new(ctx.chain_spec());
        
        Ok(evm_config)
    }
}

