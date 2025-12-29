//! ExecutorBuilder 实现，使用 MintEvmFactory

use reth_node_builder::{
    components::ExecutorBuilder,
    node::{FullNodeTypes, NodeTypes},
    BuilderContext,
};
use reth_ethereum::EthPrimitives;
use reth_evm_ethereum::{EthEvmConfig, MintStateQueue};
use reth_chainspec::ChainSpec;

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
        // 创建 mint_queue
        let mint_queue = MintStateQueue::default();
        
        // 使用 new_with_mint_evm_factory 创建配置
        let evm_config = EthEvmConfig::new_with_mint_evm_factory(
            ctx.chain_spec(),
            mint_queue,
        );
        
        Ok(evm_config)
    }
}

