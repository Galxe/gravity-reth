//! Mint Token 预编译合约实现
//!
//! 功能：mint(address recipient, uint256 amount)
//! 给指定地址增加指定数量的 token 余额

use alloy_primitives::{address, Address, Bytes, U256};
use alloy_evm::precompiles::PrecompileInput;
use reth_ethereum::evm::revm::precompile::{
    PrecompileId, PrecompileOutput, PrecompileResult, PrecompileError
};
use reth_evm::precompiles::DynPrecompile;
use tracing::{info, warn};

/// 预编译合约地址
pub const MINT_TOKEN_PRECOMPILE_ADDRESS: Address = 
    address!("0x0000000000000000000000000000000000002024");

/// 函数ID定义（使用自定义格式避免选择器冲突）
const FUNC_MINT: u8 = 0x01;

/// Gas 消耗常量
const GAS_COST_BASE: u64 = 21000;              // 基础 gas
const GAS_COST_SLOAD: u64 = 100;               // 读取存储
const GAS_COST_SSTORE_SET: u64 = 20000;        // 设置新存储槽（账户不存在）
const GAS_COST_SSTORE_RESET: u64 = 5000;       // 重置存储槽（账户存在）

// 从 reth-evm-ethereum 导入 MintRequest 和 MintStateQueue
// 这些类型在 parallel_execute.rs 中定义，用于在执行层和预编译合约之间传递 mint 请求
// 注意：这些类型已经在 reth-evm-ethereum 的 lib.rs 中重新导出
pub use reth_evm_ethereum::{MintRequest, MintStateQueue};

/// 创建 Mint Token 预编译合约
/// 
/// 这个预编译合约会解析参数，然后将 mint 请求放入队列
/// 实际的余额修改在区块执行后，通过 `increment_balances` 统一处理
/// （与 reward 流程一致）
pub fn create_mint_token_precompile(
    mint_queue: &MintStateQueue,
) -> DynPrecompile {
    let queue = mint_queue.as_shared();
    let precompile_id = PrecompileId::custom("mint_token");
    
    (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResult {
        mint_token_handler(input, queue.clone())
    })
    .into()
}

/// Mint Token 处理函数
/// 
/// 解析输入参数，验证有效性，然后将 mint 请求加入队列
fn mint_token_handler(
    input: PrecompileInput<'_>,
    mint_queue: std::sync::Arc<parking_lot::Mutex<Vec<MintRequest>>>,
) -> PrecompileResult {
    info!(
        target: "evm::precompile::mint_token",
        input_len = input.data.len(),
        "mint_token precompile called"
    );

    // 1. 参数长度检查
    // 需要：1字节函数ID + 20字节地址 + 32字节token数量 = 53字节
    if input.data.len() < 53 {
        warn!(
            target: "evm::precompile::mint_token",
            input_len = input.data.len(),
            "invalid input length, expected at least 53 bytes"
        );
        return Err(PrecompileError::OutOfGas);
    }
    
    // 2. 解析函数ID
    if input.data[0] != FUNC_MINT {
        warn!(
            target: "evm::precompile::mint_token",
            func_id = input.data[0],
            expected = FUNC_MINT,
            "invalid function id"
        );
        return Err(PrecompileError::OutOfGas);
    }
    
    // 3. 解析地址（偏移1字节，长度20字节）
    let recipient = Address::from_slice(&input.data[1..21]);
    
    // 4. 解析token数量（偏移21字节，长度32字节）
    let amount = match U256::from_be_slice(&input.data[21..53]).try_into() {
        Ok(amount) if amount > 0 => amount,
        _ => {
            warn!(
                target: "evm::precompile::mint_token",
                ?recipient,
                "invalid amount (zero or overflow)"
            );
            return Err(PrecompileError::OutOfGas);
        }
    };

    info!(
        target: "evm::precompile::mint_token",
        ?recipient,
        amount,
        "mint request parsed successfully"
    );
    
    // 5. 将 mint 请求加入队列（在区块执行后统一处理）
    mint_queue.lock().push(MintRequest {
        recipient,
        amount,
    });

    debug!(
        target: "evm::precompile::mint_token",
        ?recipient,
        amount,
        "mint request added to queue"
    );
    
    // 6. 计算并返回 gas
    let gas_used = calculate_mint_gas();
    
    Ok(PrecompileOutput {
        gas_used,
        bytes: Bytes::new(), // 可以返回新的余额，这里简化处理
        reverted: false,
    })
}

/// 计算 mint 操作的 gas
fn calculate_mint_gas() -> u64 {
    // 基础 gas
    let mut gas = GAS_COST_BASE;
    
    // 读取账户的 gas（SLOAD）
    gas += GAS_COST_SLOAD;
    
    // 写入余额的 gas
    // 如果账户不存在，使用 SSTORE_SET
    // 如果账户存在，使用 SSTORE_RESET
    // 这里简化处理，使用平均值（实际应该根据账户是否存在动态计算）
    gas += GAS_COST_SSTORE_RESET;
    
    gas
}

