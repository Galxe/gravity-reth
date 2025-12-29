//! 自定义 EvmFactory，集成 Mint Token 预编译合约

use crate::parallel_execute::{MintRequest, MintStateQueue};
use alloc::{sync::Arc, vec::Vec};
use alloy_evm::{
    eth::EthEvmContext,
    precompiles::{PrecompileInput, PrecompilesMap},
    revm::{
        context::Context,
        handler::EthPrecompiles,
        inspector::{Inspector, NoOpInspector},
        interpreter::interpreter::EthInterpreter,
        precompile::{PrecompileId, PrecompileOutput, PrecompileResult, PrecompileError},
        primitives::hardfork::SpecId,
        MainBuilder, MainContext,
    },
    Database, EvmEnv, EvmFactory, EthEvm,
};
use alloy_primitives::{address, Address, Bytes, U256};
use parking_lot::Mutex;
use reth_evm::precompiles::DynPrecompile;

/// 预编译合约地址
pub const MINT_TOKEN_PRECOMPILE_ADDRESS: Address = 
    address!("0x0000000000000000000000000000000000002024");

/// 函数ID定义（使用自定义格式避免选择器冲突）
const FUNC_MINT: u8 = 0x01;

/// Gas 消耗常量
const GAS_COST_BASE: u64 = 21000;              // 基础 gas
const GAS_COST_SLOAD: u64 = 100;               // 读取存储
const GAS_COST_SSTORE_RESET: u64 = 5000;       // 重置存储槽（账户存在）

/// 创建 Mint Token 预编译合约
fn create_mint_token_precompile(mint_queue: &MintStateQueue) -> DynPrecompile {
    let queue = mint_queue.as_shared();
    let precompile_id = PrecompileId::custom("mint_token");
    
    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        mint_token_handler(input, queue.clone())
    })
    .into()
}

/// Mint Token 处理函数
fn mint_token_handler(
    input: PrecompileInput<'_>,
    mint_queue: Arc<Mutex<Vec<MintRequest>>>,
) -> PrecompileResult {
    // 1. 参数长度检查
    // 需要：1字节函数ID + 20字节地址 + 32字节token数量 = 53字节
    if input.data.len() < 53 {
        return Err(PrecompileError::OutOfGas);
    }
    
    // 2. 解析函数ID
    if input.data[0] != FUNC_MINT {
        return Err(PrecompileError::OutOfGas);
    }
    
    // 3. 解析地址（偏移1字节，长度20字节）
    let recipient = Address::from_slice(&input.data[1..21]);
    
    // 4. 解析token数量（偏移21字节，长度32字节）
    let amount = match U256::from_be_slice(&input.data[21..53]).try_into() {
        Ok(amount) if amount > 0 => amount,
        _ => return Err(PrecompileError::OutOfGas),
    };
    
    // 5. 将 mint 请求加入队列（在区块执行后统一处理）
    mint_queue.lock().push(MintRequest {
        recipient,
        amount,
    });
    
    // 6. 计算并返回 gas
    let gas_used = GAS_COST_BASE + GAS_COST_SLOAD + GAS_COST_SSTORE_RESET;
    
    Ok(PrecompileOutput {
        gas_used,
        bytes: Bytes::new(),
        reverted: false,
    })
}

/// Trait for EvmFactory that provides mint queue access
pub trait MintQueueProvider {
    /// 获取 mint_queue 的引用
    fn mint_queue(&self) -> Option<&MintStateQueue>;
}

/// 自定义 EvmFactory，支持 Mint Token 预编译合约
#[derive(Debug, Clone)]
pub struct MintEvmFactory {
    /// Mint 请求队列
    mint_queue: MintStateQueue,
}

impl MintEvmFactory {
    /// 创建新的 MintEvmFactory
    pub fn new(mint_queue: MintStateQueue) -> Self {
        Self { mint_queue }
    }

    /// 获取 mint_queue 的引用
    pub fn mint_queue(&self) -> &MintStateQueue {
        &self.mint_queue
    }
}

impl MintQueueProvider for MintEvmFactory {
    fn mint_queue(&self) -> Option<&MintStateQueue> {
        Some(&self.mint_queue)
    }
}

impl EvmFactory for MintEvmFactory {
    type Evm<DB: Database, I: Inspector<EthEvmContext<DB>, EthInterpreter>> =
        EthEvm<DB, I, Self::Precompiles>;
    type Tx = alloy_evm::revm::context::TxEnv;
    type Error<DBError: core::error::Error + Send + Sync + 'static> =
        alloy_evm::revm::context_interface::result::EVMError<DBError>;
    type HaltReason = alloy_evm::revm::context_interface::result::HaltReason;
    type Context<DB: Database> = EthEvmContext<DB>;
    type Spec = SpecId;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        let spec = input.cfg_env.spec;
        let mut precompiles = PrecompilesMap::from_static(EthPrecompiles::default().precompiles);

        // 注册 Mint Token 预编译合约
        let mint_precompile = create_mint_token_precompile(&self.mint_queue);
        precompiles.apply_precompile(
            &MINT_TOKEN_PRECOMPILE_ADDRESS,
            |_| Some(mint_precompile),
        );

        let evm = Context::mainnet()
            .with_db(db)
            .with_cfg(input.cfg_env)
            .with_block(input.block_env)
            .build_mainnet_with_inspector(NoOpInspector {})
            .with_precompiles(precompiles);

        // 如果支持 Prague，可以在这里添加其他自定义预编译合约
        if spec == SpecId::PRAGUE {
            // 可以添加其他 Prague 特定的预编译合约
        }

        EthEvm::new(evm, false)
    }

    fn create_evm_with_inspector<
        DB: Database,
        I: Inspector<Self::Context<DB>, EthInterpreter>,
    >(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        EthEvm::new(
            self.create_evm(db, input).into_inner().with_inspector(inspector),
            true,
        )
    }
}

