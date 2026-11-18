//! This example shows how to implement a node with a custom EVM

#![warn(unused_crate_dependencies)]

use alloy_evm::{
    eth::{EthBlockExecutionCtx, EthBlockExecutorFactory, EthEvmContext},
    precompiles::PrecompilesMap,
    revm::{
        context::DBErrorMarker,
        handler::EthPrecompiles,
        precompile::{Precompile, PrecompileId},
    },
    EvmFactory,
};
use alloy_genesis::Genesis;
use alloy_primitives::{address, Bytes};
use alloy_rpc_types_engine::ExecutionData;
use reth_ethereum::{
    chainspec::{Chain, ChainSpec},
    evm::{
        primitives::{
            execute::BlockExecutionError, ConfigureEngineEvm, ConfigureEvm, ConfigureEvmForRead,
            Database, EvmEnv, EvmEnvFor, ExecutableTxIterator, ExecutionCtxFor,
            NextBlockEnvAttributes,
        },
        revm::{
            context::{BlockEnv, Context, TxEnv},
            context_interface::result::{EVMError, HaltReason},
            inspector::{Inspector, NoOpInspector},
            interpreter::interpreter::EthInterpreter,
            precompile::{PrecompileOutput, PrecompileSpecId, Precompiles},
            primitives::hardfork::SpecId,
            MainBuilder, MainContext,
        },
        EthBlockAssembler, EthEvm, EthEvmConfig, RethReceiptBuilder,
    },
    node::{
        api::{FullNodeTypes, NodeTypes},
        builder::{components::ExecutorBuilder, BuilderContext, NodeBuilder},
        core::{args::RpcServerArgs, node_config::NodeConfig},
        node::EthereumAddOns,
        EthereumNode,
    },
    primitives::{Header, SealedBlock, SealedHeader},
    tasks::Runtime,
    Block, EthPrimitives,
};
use reth_tracing::{RethTracer, Tracer};
use std::sync::{Arc, OnceLock};

/// Custom EVM configuration.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MyEvmFactory;

impl EvmFactory for MyEvmFactory {
    type Evm<DB: Database, I: Inspector<EthEvmContext<DB>, EthInterpreter>> =
        EthEvm<DB, I, Self::Precompiles>;
    type Tx = TxEnv;
    type Error<DBError: DBErrorMarker> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Context<DB: Database> = EthEvmContext<DB>;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        let spec = input.cfg_env.spec;
        let mut evm = Context::mainnet()
            .with_db(db)
            .with_cfg(input.cfg_env)
            .with_block(input.block_env)
            .build_mainnet_with_inspector(NoOpInspector {})
            .with_precompiles(PrecompilesMap::from_static(EthPrecompiles::new(spec).precompiles));

        if spec.is_enabled_in(SpecId::PRAGUE) {
            evm = evm.with_precompiles(PrecompilesMap::from_static(custom_precompiles(spec)));
        }

        EthEvm::new(evm, false)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>, EthInterpreter>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        EthEvm::new(self.create_evm(db, input).into_inner().with_inspector(inspector), true)
    }
}

/// Builds a regular ethereum block executor that uses the custom EVM.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct MyExecutorBuilder;

impl<Node> ExecutorBuilder<Node> for MyExecutorBuilder
where
    Node: FullNodeTypes<Types: NodeTypes<ChainSpec = ChainSpec, Primitives = EthPrimitives>>,
{
    type EVM = MyEvmConfig;

    async fn build_evm(self, ctx: &BuilderContext<Node>) -> eyre::Result<Self::EVM> {
        Ok(MyEvmConfig::new(ctx.chain_spec()))
    }
}

/// A node configuration that retains the example's custom EVM factory.
#[derive(Debug, Clone)]
pub struct MyEvmConfig {
    base: EthEvmConfig,
    custom: EthEvmConfig<ChainSpec, MyEvmFactory>,
}

impl MyEvmConfig {
    /// Configures block environments and the custom executor for the same chain.
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self {
            base: EthEvmConfig::new(chain_spec.clone()),
            custom: EthEvmConfig::new_with_evm_factory(chain_spec, MyEvmFactory),
        }
    }
}

impl ConfigureEvmForRead for MyEvmConfig {}

impl ConfigureEvm for MyEvmConfig {
    type Primitives = <EthEvmConfig as ConfigureEvm>::Primitives;
    type Error = <EthEvmConfig as ConfigureEvm>::Error;
    type NextBlockEnvCtx = <EthEvmConfig as ConfigureEvm>::NextBlockEnvCtx;
    type BlockExecutorFactory =
        EthBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, MyEvmFactory>;
    type BlockAssembler = EthBlockAssembler<ChainSpec>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.custom.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        self.base.block_assembler()
    }

    fn evm_env(&self, header: &Header) -> Result<EvmEnv<SpecId>, Self::Error> {
        self.base.evm_env(header)
    }

    fn next_evm_env(
        &self,
        parent: &Header,
        attributes: &NextBlockEnvAttributes,
    ) -> Result<EvmEnv<SpecId>, Self::Error> {
        self.base.next_evm_env(parent, attributes)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<Block>,
    ) -> Result<EthBlockExecutionCtx<'a>, Self::Error> {
        self.base.context_for_block(block)
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader,
        attributes: Self::NextBlockEnvCtx,
    ) -> Result<EthBlockExecutionCtx<'_>, Self::Error> {
        self.base.context_for_next_block(parent, attributes)
    }

    fn parallel_executor<'a, DB: reth_ethereum::evm::primitives::ParallelDatabase + 'a>(
        &self,
        db: DB,
    ) -> Box<
        dyn reth_ethereum::evm::primitives::parallel_execute::ParallelExecutor<
                Primitives = Self::Primitives,
                Error = BlockExecutionError,
            > + 'a,
    > {
        // The serial adapter executes with this example's custom factory.
        Box::new(reth_ethereum::evm::primitives::parallel_execute::WrapExecutor::new(
            reth_ethereum::evm::primitives::execute::BasicBlockExecutor::new(
                self.clone(),
                reth_ethereum::evm::revm::database_interface::WrapDatabaseRef(db),
            ),
        ))
    }
}

impl ConfigureEngineEvm<ExecutionData> for MyEvmConfig {
    fn evm_env_for_payload(&self, payload: &ExecutionData) -> Result<EvmEnvFor<Self>, Self::Error> {
        self.base.evm_env_for_payload(payload)
    }

    fn context_for_payload<'a>(
        &self,
        payload: &'a ExecutionData,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        self.base.context_for_payload(payload)
    }

    fn tx_iterator_for_payload(
        &self,
        payload: &ExecutionData,
    ) -> Result<impl ExecutableTxIterator<Self>, Self::Error> {
        self.base.tx_iterator_for_payload(payload)
    }
}

/// Returns the fork's precompiles with the custom address installed.
pub fn custom_precompiles(spec: SpecId) -> &'static Precompiles {
    static INSTANCES: [OnceLock<Precompiles>; PrecompileSpecId::NEXT as usize + 1] =
        [const { OnceLock::new() }; PrecompileSpecId::NEXT as usize + 1];
    let spec = PrecompileSpecId::from_spec_id(spec);
    INSTANCES[spec as usize].get_or_init(|| {
        let mut precompiles = Precompiles::new(spec).clone();
        // Custom precompile.
        let precompile = Precompile::new(
            PrecompileId::custom("custom"),
            address!("0x0000000000000000000000000000000000000999"),
            |_, _, _| Ok(PrecompileOutput::new(0, Bytes::new(), 0)),
        );
        precompiles.extend([precompile]);
        precompiles
    })
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _guard = RethTracer::new().init()?;

    let runtime = Runtime::test();

    // create a custom chain spec
    let spec = ChainSpec::builder()
        .chain(Chain::mainnet())
        .genesis(Genesis::default())
        .london_activated()
        .paris_activated()
        .shanghai_activated()
        .cancun_activated()
        .prague_activated()
        .build();

    let node_config =
        NodeConfig::test().with_rpc(RpcServerArgs::default().with_http()).with_chain(spec);

    let handle = NodeBuilder::new(node_config)
        .testing_node(runtime)
        // configure the node with regular ethereum types
        .with_types::<EthereumNode>()
        // use default ethereum components but with our executor
        .with_components(EthereumNode::components().executor(MyExecutorBuilder::default()))
        .with_add_ons(EthereumAddOns::default())
        .launch()
        .await
        .unwrap();

    println!("Node started");

    handle.node_exit_future.await
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_evm::{revm::database::EmptyDB, Evm};
    use alloy_primitives::TxKind;
    use reth_ethereum::evm::revm::{
        context::CfgEnv,
        context_interface::result::{ExecutionResult, SuccessReason},
    };

    #[test]
    fn custom_precompile_and_fork_builtins_survive_later_forks() {
        let config = MyEvmConfig::new(reth_ethereum::chainspec::MAINNET.clone());
        // Visit forks out of order to exercise the per-spec cache rather than its first entry.
        for (spec, reason) in [
            (SpecId::OSAKA, SuccessReason::Return),
            (SpecId::PRAGUE, SuccessReason::Return),
            (SpecId::AMSTERDAM, SuccessReason::Return),
            (SpecId::CANCUN, SuccessReason::Stop),
        ] {
            if spec.is_enabled_in(SpecId::PRAGUE) {
                let standard = Precompiles::new(PrecompileSpecId::from_spec_id(spec));
                let custom = custom_precompiles(spec);
                assert_eq!(custom.addresses().len(), standard.addresses().len() + 1);
                assert!(standard.addresses().all(|address| custom.contains(address)));
            }
            let env = EvmEnv {
                cfg_env: CfgEnv::new_with_spec(spec).with_chain_id(1),
                block_env: BlockEnv { gas_limit: 30_000_000, ..Default::default() },
            };
            let output = config
                .evm_with_env(EmptyDB::default(), env)
                .transact(TxEnv {
                    kind: TxKind::Call(address!("0x0000000000000000000000000000000000000999")),
                    gas_limit: 50_000,
                    ..Default::default()
                })
                .unwrap();
            assert!(
                matches!(output.result, ExecutionResult::Success { reason: actual, .. } if actual == reason)
            );
        }
    }
}
