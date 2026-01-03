//! ExecutorBuilder 实现，使用 MintEvmFactory

use reth_node_builder::{
    components::ExecutorBuilder,
    node::{FullNodeTypes, NodeTypes},
    BuilderContext,
};
use reth_ethereum::EthPrimitives;
use reth_evm_ethereum::{EthEvmConfig, MintEvmFactory, MintStateQueue};
use reth_chainspec::ChainSpec;
use tracing::info;

/// ExecutorBuilder，使用 MintEvmFactory 和 Mint Token 预编译合约
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct MintExecutorBuilder;

impl<Node> ExecutorBuilder<Node> for MintExecutorBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
{
    type EVM = EthEvmConfig<ChainSpec, reth_evm_ethereum::MintEvmFactory>;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        info!(target: "evm::executor_builder", "Building EVM with MintEvmFactory (global queue)");
        
        let evm_config = EthEvmConfig::<ChainSpec, MintEvmFactory>::new_with_mint_evm_factory(
            ctx.chain_spec(),
        );
        
        Ok(evm_config)
    }
}

