//! 自定义 EvmFactory，集成 Mint Token 预编译合约

use crate::parallel_execute::{MintRequest, MintStateQueue};
use alloc::{sync::Arc, vec::Vec};
use tracing::{info, warn};
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
    Database, EvmEnv, EvmFactory,
};
use alloy_primitives::{address, Address, Bytes, U256};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use reth_evm::EthEvm;
use reth_evm::precompiles::DynPrecompile;

// ============================================================================
// 全局 Mint Queue
// ============================================================================

/// 全局 MintStateQueue 单例
/// RPC 和 block executor 共享此队列，确保预编译合约的 mint 请求能被正确处理
static GLOBAL_MINT_QUEUE: Lazy<MintStateQueue> = 
    Lazy::new(|| {
        info!(target: "evm::mint", "Initializing global MINT_QUEUE");
        MintStateQueue::default()
    });

/// 获取全局 MintStateQueue 引用
pub fn global_mint_queue() -> &'static MintStateQueue {
    &GLOBAL_MINT_QUEUE
}

// ============================================================================
// Mint Token 预编译合约
// ============================================================================

/// 预编译合约地址
pub const MINT_TOKEN_PRECOMPILE_ADDRESS: Address = 
    address!("0x0000000000000000000000000000000000002024");

/// 函数ID定义
const FUNC_MINT: u8 = 0x01;

/// Gas 消耗常量
const GAS_COST_BASE: u64 = 21000;
const GAS_COST_SLOAD: u64 = 100;
const GAS_COST_SSTORE_RESET: u64 = 5000;

/// 创建 Mint Token 预编译合约（使用全局队列）
fn create_mint_token_precompile() -> DynPrecompile {
    let queue = global_mint_queue().as_shared();
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
    // 使用原子计数器追踪调用次数
    use core::sync::atomic::{AtomicU64, Ordering};
    static CALL_COUNTER: AtomicU64 = AtomicU64::new(0);
    let call_id = CALL_COUNTER.fetch_add(1, Ordering::SeqCst);
    
    info!(
        target: "evm::precompile::mint_token",
        call_id = call_id,
        input_len = input.data.len(),
        queue_len_before = mint_queue.lock().len(),
        "mint_token precompile called"
    );

    // 1. 参数长度检查 (1字节函数ID + 20字节地址 + 32字节数量 = 53字节)
    if input.data.len() < 53 {
        warn!(
            target: "evm::precompile::mint_token",
            input_len = input.data.len(),
            "invalid input length"
        );
        return Err(PrecompileError::OutOfGas);
    }
    
    // 2. 解析函数ID
    if input.data[0] != FUNC_MINT {
        warn!(
            target: "evm::precompile::mint_token",
            func_id = input.data[0],
            "invalid function id"
        );
        return Err(PrecompileError::OutOfGas);
    }
    
    // 3. 解析地址和数量
    let recipient = Address::from_slice(&input.data[1..21]);
    let amount = match U256::from_be_slice(&input.data[21..53]).try_into() {
        Ok(amount) if amount > 0 => amount,
        _ => {
            warn!(target: "evm::precompile::mint_token", ?recipient, "invalid amount");
            return Err(PrecompileError::OutOfGas);
        }
    };

    info!(
        target: "evm::precompile::mint_token",
        call_id = call_id,
        ?recipient,
        amount,
        queue_len_after = mint_queue.lock().len(),
        "mint request added to queue"
    );
    
    // 4. 将 mint 请求加入队列
    mint_queue.lock().push(MintRequest { recipient, amount });
    
    // 5. 返回成功，消耗 gas
    Ok(PrecompileOutput {
        gas_used: GAS_COST_BASE + GAS_COST_SLOAD + GAS_COST_SSTORE_RESET,
        bytes: Bytes::new(),
        reverted: false,
    })
}

// ============================================================================
// MintEvmFactory
// ============================================================================

/// 自定义 EvmFactory，支持 Mint Token 预编译合约
/// 使用全局 mint_queue
#[derive(Debug, Clone, Default)]
pub struct MintEvmFactory;

impl MintEvmFactory {
    /// 创建新的 MintEvmFactory
    pub fn new() -> Self {
        Self
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
        use core::sync::atomic::{AtomicU64, Ordering};
        static EVM_CREATE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let evm_id = EVM_CREATE_COUNTER.fetch_add(1, Ordering::SeqCst);
        
        info!(
            target: "evm::mint_evm_factory",
            evm_id = evm_id,
            "MintEvmFactory::create_evm called"
        );
        
        // 创建带默认 precompiles 的 EVM
        let mut evm = Context::mainnet()
            .with_db(db)
            .with_cfg(input.cfg_env)
            .with_block(input.block_env)
            .build_mainnet_with_inspector(NoOpInspector {})
            .with_precompiles(PrecompilesMap::from_static(EthPrecompiles::default().precompiles));

        // 添加 mint token 预编译合约
        let mut precompiles = PrecompilesMap::from_static(EthPrecompiles::default().precompiles);
        let mint_precompile = create_mint_token_precompile();
        precompiles.apply_precompile(&MINT_TOKEN_PRECOMPILE_ADDRESS, |_| Some(mint_precompile));

        // 设置包含自定义预编译合约的 precompiles
        evm = evm.with_precompiles(precompiles);

        EthEvm::new(evm, false)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>, EthInterpreter>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        // 复用 create_evm 的逻辑
        let base_evm = self.create_evm(db, input);
        EthEvm::new(base_evm.into_inner().with_inspector(inspector), true)
    }
}
