//! Gravity node assembly: the Ethereum node with [`GravityEvmConfig`].

use crate::{
    EthereumConsensusBuilder, EthereumNetworkBuilder, EthereumNode, EthereumPayloadBuilder,
    EthereumPoolBuilder,
};
use reth_chainspec::ChainSpec;
use reth_ethereum_engine_primitives::{EthBuiltPayload, EthPayloadAttributes};
use reth_ethereum_primitives::EthPrimitives;
use reth_evm_ethereum::GravityEvmConfig;
pub use reth_evm_ethereum::ProviderChainReader;
use reth_node_builder::{
    components::{BasicPayloadServiceBuilder, ComponentsBuilder, ExecutorBuilder},
    node::{FullNodeTypes, NodeTypes},
    BuilderContext,
};
use reth_payload_primitives::PayloadTypes;
use std::sync::Arc;

impl EthereumNode {
    /// Returns a [`ComponentsBuilder`] configured for a Gravity node: the Ethereum components
    /// with [`GravityExecutorBuilder`].
    ///
    /// The node's RPC then re-executes committed blocks with Gravity's rules. The pipe keeps
    /// executing blocks with its own Ethereum configuration.
    pub fn gravity_components<Node>() -> ComponentsBuilder<
        Node,
        EthereumPoolBuilder,
        BasicPayloadServiceBuilder<EthereumPayloadBuilder>,
        EthereumNetworkBuilder,
        GravityExecutorBuilder,
        EthereumConsensusBuilder,
    >
    where
        Node: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
        <Node::Types as NodeTypes>::Payload:
            PayloadTypes<BuiltPayload = EthBuiltPayload, PayloadAttributes = EthPayloadAttributes>,
    {
        Self::components().executor(GravityExecutorBuilder)
    }
}

/// Builds [`GravityEvmConfig`] over the node's provider.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct GravityExecutorBuilder;

impl<Node> ExecutorBuilder<Node> for GravityExecutorBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
{
    type EVM = GravityEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        // The provider shares the in-memory canonical chain with RPC and the engine, so blocks
        // the pipe made canonical are readable before they are persisted.
        let reader = ProviderChainReader(ctx.provider().clone());
        Ok(GravityEvmConfig::new(ctx.chain_spec(), Arc::new(reader)))
    }
}
