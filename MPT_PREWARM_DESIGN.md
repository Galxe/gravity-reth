# MPT 预热机制设计文档

## 概述

本文档描述了在 Gravity Reth 中实现的 MPT（Merkle Patricia Trie）预热机制。该机制通过在交易执行完成后立即预热相关的 MPT 节点，将数据预加载到数据库缓存中，从而显著提升后续 MPT 计算的性能。

## 设计目标

1. **实时预热**：在交易提交后立即预热相关数据，不延迟后续交易执行
2. **异步非阻塞**：预热操作在后台异步执行，不阻塞主执行流程
3. **智能检查**：通过区块号检查避免过期预热，确保预热的有效性
4. **性能优化**：最小化对现有性能的影响，同时最大化预热效果

## 架构设计

### 核心组件

```
┌─────────────────────────────────────────────────────────────┐
│                        Node 初始化                           │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  创建 channel (sender, receiver)                       │ │
│  │  设置全局 sender: GLOBAL_PREWARM_SENDER                │ │
│  │  创建 PrewarmService(db, receiver, config)             │ │
│  │  tokio::spawn(prewarm_service.run())                  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    parallel_executor                         │
│  获取全局 sender → 传递给 GrevmExecutor                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     GrevmExecutor                            │
│  execute_transactions → scheduler.set_prewarm_sender()      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      grevm async_commit                      │
│  commit() → sender.send(PrewarmTask { block_number, ... }) │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   PrewarmService::run                        │
│  receiver.recv() → execute_prewarm() → metrics              │
└─────────────────────────────────────────────────────────────┘
```

## 配置参数

通过环境变量控制：

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `RETH_ENABLE_MPT_PREWARM` | `true` | 是否启用 MPT 预热 |
| `RETH_PREWARM_TIMEOUT_MS` | `100` | 预热超时时间（毫秒） |
| `RETH_PREWARM_CONTRACTS_ONLY` | `true` | 只预热合约账户 |
| `RETH_PREWARM_STORAGE_THRESHOLD` | `5` | 存储预热阈值 |

## 实现细节

### 1. grevm 修改

#### 1.1 async_commit.rs

```rust
use tokio::sync::mpsc;

/// 预热任务
pub struct PrewarmTask {
    pub block_number: u64,
    pub result_and_state: ResultAndState,
}

pub(crate) struct StateAsyncCommit<'a, DB>
where
    DB: DatabaseRef,
{
    // ... 现有字段 ...

    /// 预热任务发送器
    prewarm_sender: Option<mpsc::UnboundedSender<PrewarmTask>>,
    /// 当前区块号
    block_number: u64,
}

impl<'a, DB> StateAsyncCommit<'a, DB>
where
    DB: DatabaseRef,
{
    pub(crate) fn with_prewarm_sender(
        mut self,
        sender: mpsc::UnboundedSender<PrewarmTask>,
        block_number: u64,
    ) -> Self {
        self.prewarm_sender = Some(sender);
        self.block_number = block_number;
        self
    }

    pub(crate) fn commit(&mut self, txid: TxId, tx_env: &TxEnv, result_and_state: ResultAndState) {
        // ... 现有的 nonce 检查逻辑 ...

        self.state_mut().commit(state);

        // 发送预热任务到 channel（非阻塞）
        if let Some(ref sender) = self.prewarm_sender {
            let task = PrewarmTask {
                block_number: self.block_number,
                result_and_state,  // move 进去
            };
            let _ = sender.send(task);  // 忽略发送失败
        }

        // 增加矿工奖励
        assert!(self.state_mut().increment_balances(vec![(self.coinbase, lazy_reward)]).is_ok());
    }
}
```

#### 1.2 scheduler.rs

```rust
impl<DB> Scheduler<DB>
where
    DB: DatabaseRef + Send + Sync,
{
    /// 设置预热 sender
    pub fn set_prewarm_sender(&mut self, sender: mpsc::UnboundedSender<PrewarmTask>, block_number: u64) {
        // 传递给 StateAsyncCommit
        // 具体实现取决于 scheduler 如何创建 commiter
    }
}
```

### 2. Gravity-reth 实现

#### 2.1 全局变量管理

```rust
// crates/ethereum/evm/src/prewarm/global.rs

use tokio::sync::mpsc;
use std::sync::OnceLock;

/// 全局预热任务发送器
pub static GLOBAL_PREWARM_SENDER: OnceLock<mpsc::UnboundedSender<PrewarmTask>> = OnceLock::new();

/// 设置全局预热 sender
pub fn set_global_prewarm_sender(sender: mpsc::UnboundedSender<PrewarmTask>) -> Result<(), ()> {
    GLOBAL_PREWARM_SENDER.set(sender)
}

/// 获取全局预热 sender
pub fn get_global_prewarm_sender() -> Option<&'static mpsc::UnboundedSender<PrewarmTask>> {
    GLOBAL_PREWARM_SENDER.get()
}
```

#### 2.2 parallel_executor 使用全局变量

```rust
// crates/ethereum/evm/src/lib.rs

use crate::prewarm::global::get_global_prewarm_sender;

impl<ChainSpec> EthEvmConfig<ChainSpec>
where
    ChainSpec: EthExecutorSpec + EthChainSpec<Header = Header> + Hardforks + 'static,
{
    fn parallel_executor<'a, DB: ParallelDatabase + 'a>(
        &self,
        db: DB,
    ) -> Box<dyn ParallelExecutor<Primitives = Self::Primitives, Error = BlockExecutionError> + 'a>
    {
        if get_gravity_config().disable_grevm {
            Box::new(WrapExecutor::new(BasicBlockExecutor::new(self.clone(), WrapDatabaseRef(db))))
        } else {
            // 获取全局 prewarm_sender，传递给 executor
            let prewarm_sender = get_global_prewarm_sender().cloned();
            Box::new(GrevmExecutor::new(
                self.chain_spec().clone(),
                self,
                db,
                prewarm_sender,
            ))
        }
    }
}
```

#### 2.3 GrevmExecutor 接收 prewarm_sender

```rust
// crates/ethereum/evm/src/parallel_execute.rs

pub struct GrevmExecutor<DB, EvmConfig, ChainSpec> {
    chain_spec: Arc<ChainSpec>,
    evm_config: EvmConfig,
    state: Option<ParallelState<DB>>,
    system_caller: SystemCaller<Arc<ChainSpec>>,
    /// 预热任务发送器
    prewarm_sender: Option<mpsc::UnboundedSender<PrewarmTask>>,
}

impl<DB, EvmConfig, ChainSpec> GrevmExecutor<DB, EvmConfig, ChainSpec>
where
    EvmConfig: Clone + ConfigureEvm<Primitives = EthPrimitives, ...>,
    DB: ParallelDatabase,
    ChainSpec: EthExecutorSpec + EthChainSpec + Hardforks + 'static,
{
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        evm_config: &EvmConfig,
        db: DB,
        prewarm_sender: Option<mpsc::UnboundedSender<PrewarmTask>>,
    ) -> Self {
        let system_caller = SystemCaller::new(chain_spec.clone());
        let report_db_metrics = get_gravity_config().report_db_metrics;
        Self {
            state: Some(ParallelState::new(db, true, report_db_metrics)),
            chain_spec,
            evm_config: evm_config.clone(),
            system_caller,
            prewarm_sender,
        }
    }

    fn execute_transactions(
        &mut self,
        block: &RecoveredBlock<Block>,
    ) -> Result<ExecuteOutput<Receipt>, BlockExecutionError> {
        // ... 现有代码 ...

        let (results, state) = {
            let EvmEnv { cfg_env, block_env } = evm_env;
            let mut executor = Scheduler::new(cfg_env, block_env, txs, state, false);

            // 设置 prewarm_sender
            if let Some(ref sender) = self.prewarm_sender {
                executor.set_prewarm_sender(sender.clone(), block.number());
            }

            executor.parallel_execute(None)?;
            executor.take_result_and_state()
        };

        self.state = Some(state);
        // ... 其余代码 ...
    }
}
```

#### 2.4 PrewarmService 实现

```rust
// crates/ethereum/evm/src/prewarm/service.rs

use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc;

/// 预热配置
#[derive(Debug, Clone)]
pub struct PrewarmConfig {
    pub enabled: bool,
    pub timeout_ms: u64,
    pub contracts_only: bool,
    pub storage_threshold: usize,
}

impl PrewarmConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: std::env::var("RETH_ENABLE_MPT_PREWARM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(true),
            timeout_ms: std::env::var("RETH_PREWARM_TIMEOUT_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            contracts_only: std::env::var("RETH_PREWARM_CONTRACTS_ONLY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(true),
            storage_threshold: std::env::var("RETH_PREWARM_STORAGE_THRESHOLD")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5),
        }
    }
}

/// 预热服务
pub struct PrewarmService<DB> {
    /// 数据库
    db: DB,
    /// 预热任务接收器
    receiver: mpsc::UnboundedReceiver<PrewarmTask>,
    /// 预热配置
    config: PrewarmConfig,
    /// 预热指标
    pub metrics: PrewarmMetrics,
    /// 当前处理的区块号
    current_block: Arc<RwLock<u64>>,
    /// 当前区块已预热的账户
    prewarmed_accounts: Arc<RwLock<std::collections::HashSet<alloy_primitives::Address>>>,
}

impl<DB> PrewarmService<DB>
where
    DB: reth_storage_api::Database + Clone + Send + Sync + 'static,
{
    pub fn new(
        db: DB,
        receiver: mpsc::UnboundedReceiver<PrewarmTask>,
        config: PrewarmConfig,
    ) -> Self {
        Self {
            db,
            receiver,
            config,
            metrics: PrewarmMetrics::default(),
            current_block: Arc::new(RwLock::new(0)),
            prewarmed_accounts: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// 运行预热服务（由外部 tokio::spawn 调用）
    pub async fn run(mut self) {
        while let Some(task) = self.receiver.recv().await {
            // 更新区块号
            self.metrics.current_block_number.set(task.block_number as f64);

            // 检查区块是否过期
            let current = *self.current_block.read();
            if task.block_number < current {
                self.metrics.prewarm_skipped_expired_total.increment(1);
                continue;
            }

            // 如果是新区块，清空之前的缓存
            if task.block_number > current {
                self.prewarmed_accounts.write().clear();
                *self.current_block.write() = task.block_number;
            }

            // 执行预热并记录指标
            let start = std::time::Instant::now();
            match Self::execute_prewarm(&self.db, &self.config, &self.prewarmed_accounts, &task.result_and_state) {
                Ok(stats) => {
                    self.metrics.prewarm_txs_total.increment(1);
                    self.metrics.prewarm_accounts_total.increment(stats.accounts as u64);
                    self.metrics.prewarm_slots_total.increment(stats.slots as u64);
                    self.metrics.prewarm_duration.record(start.elapsed());
                }
                Err(e) => {
                    trace!(target: "evm::prewarm", "Prewarm failed: {:?}", e);
                }
            }
        }
    }

    /// 预热统计结果
    struct PrewarmStats {
        accounts: usize,
        slots: usize,
    }

    fn execute_prewarm(
        db: &DB,
        config: &PrewarmConfig,
        prewarmed_accounts: &parking_lot::RwLock<std::collections::HashSet<alloy_primitives::Address>>,
        result_and_state: &ResultAndState,
    ) -> Result<PrewarmStats, Box<dyn std::error::Error>> {
        // 创建数据库引用
        let db_ref = db.as_hashed_storage_ref();

        // 创建 MPT 实例
        let mut mpt = NestedTrie::new(db_ref);

        let mut prewarmed_accounts_local = std::collections::HashSet::new();
        let mut prewarmed_slots = 0usize;

        // 遍历状态变更
        for (address, account) in &result_and_state.state {
            // 检查是否已预热
            {
                let prewarmed = prewarmed_accounts.read();
                if prewarmed.contains(address) {
                    continue;
                }
            }

            // 检查是否只预热合约
            if config.contracts_only && !Self::is_contract(account) {
                continue;
            }

            // 预热账户
            Self::prewarm_account(&mut mpt, *address, account)?;
            prewarmed_accounts_local.insert(*address);

            // 预热存储（如果超过阈值）
            if account.storage.len() >= config.storage_threshold {
                prewarmed_slots += account.storage.len();
                Self::prewarm_storage(&mut mpt, *address, &account.storage)?;
            }
        }

        // 批量更新已预热账户
        if !prewarmed_accounts_local.is_empty() {
            let mut prewarmed = prewarmed_accounts.write();
            prewarmed.extend(prewarmed_accounts_local);
        }

        Ok(PrewarmStats {
            accounts: prewarmed_accounts_local.len(),
            slots: prewarmed_slots,
        })
    }

    fn is_contract(account: &revm::state::Account) -> bool {
        !account.code.is_empty()
    }

    fn prewarm_account(
        mpt: &mut NestedTrie<HashedStorageRef<DB>>,
        address: alloy_primitives::Address,
        _account: &revm::state::Account,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 将地址转换为 nibbles
        let address_nibbles = Nibbles::unpack(address.as_slice());

        // 使用简单的 mock 值触发预热
        // 我们不关心插入的值，只希望触发 insert_inner 中的 reader.read()
        let mock_node = Node::ValueNode(Vec::new());

        // 执行插入以触发数据库读取
        mpt.insert(address_nibbles, mock_node)?;

        Ok(())
    }

    fn prewarm_storage(
        mpt: &mut NestedTrie<HashedStorageRef<DB>>,
        address: alloy_primitives::Address,
        storage: &std::collections::HashMap<revm_primitives::U256, revm_primitives::U256>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 使用简单的 mock 值预热存储槽
        let mock_node = Node::ValueNode(Vec::new());

        for (slot, _value) in storage {
            // 构建存储键（keccak256(address || slot)）
            let storage_key = Self::keccak256(address, *slot);
            let key_nibbles = Nibbles::unpack(storage_key.as_slice());

            // 执行插入触发数据库读取
            mpt.insert(key_nibbles, mock_node.clone())?;
        }

        Ok(())
    }

    fn keccak256(address: alloy_primitives::Address, slot: revm_primitives::U256) -> alloy_primitives::B256 {
        use alloy_primitives::Hash;
        let mut hasher = alloy_primitives::Hasher::new();
        hasher.update(address.as_slice());
        hasher.update(slot.to_be_bytes::<32>());
        hasher.finalize()
    }
}
```

#### 2.5 Metrics 定义

```rust
// crates/ethereum/evm/src/prewarm/metrics.rs

use reth_metrics::{
    metrics::{Counter, Histogram, Gauge},
    Metrics,
};

/// Metrics for MPT prewarming
#[derive(Metrics)]
#[metrics(scope = "evm_prewarm")]
pub struct PrewarmMetrics {
    /// 预热的交易总数
    pub prewarm_txs_total: Counter,
    /// 预热的账户总数
    pub prewarm_accounts_total: Counter,
    /// 预热的存储槽总数
    pub prewarm_slots_total: Counter,
    /// 单次预热耗时（秒）
    pub prewarm_duration: Histogram,
    /// 跳过的过期任务数
    pub prewarm_skipped_expired_total: Counter,
    /// 当前处理的区块号
    pub current_block_number: Gauge,
}
```

#### 2.6 创建和启动 PrewarmService

在 Node 初始化时创建和启动预热服务：

```rust
// 在 Node 初始化或类似位置

use tokio::sync::mpsc;
use crate::evm::prewarm::{PrewarmService, PrewarmConfig, global::set_global_prewarm_sender};

// 创建 channel
let (prewarm_sender, prewarm_receiver) = mpsc::unbounded_channel();

// 设置全局 sender（必须在 spawn 之前）
set_global_prewarm_sender(prewarm_sender).expect("prewarm sender already set");

// 创建预热服务
let db = /* 从 provider 获取数据库 */;
let config = PrewarmConfig::from_env();
let prewarm_service = PrewarmService::new(db, prewarm_receiver, config);

// 在后台运行预热服务
tokio::spawn(async move {
    prewarm_service.run().await;
});

// 之后所有 parallel_executor 调用都会自动获取全局 sender
```

## 关键设计决策

### 1. 预热时机

在 `async_commit.rs` 的 `commit` 方法中：
- 交易状态已经真正提交
- 有完整的 `ResultAndState` 信息
- 发送任务到 channel，不阻塞主流程

### 2. 全局变量

使用 `OnceLock` 管理 sender：
- 避免层层透传
- 只需在 `parallel_executor` 中获取
- Node 初始化时设置一次

### 3. 区块号检查

- 过期任务（`block_number < current_block`）直接跳过
- 新区块自动清空缓存
- 防止区块重组导致的无效预热

### 4. 预热策略

- 使用 `Node::ValueNode(Vec::new())` 作为 mock 值
- 只触发 `reader.read()`，不实际写入正确数据
- 避免重复预热同一账户

## 性能考虑

### 预热开销

- **CPU 开销**：在独立线程中执行，不影响主交易执行
- **内存开销**：维护已预热账户集合，内存占用可控
- **I/O 开销**：真正的数据库读取操作，但可以预热到缓存

### 优化措施

1. **只预热合约账户**：跳过 EOAs
2. **存储阈值**：只预热有多个存储槽的账户
3. **避免重复**：同一区块内不重复预热同一账户

## 使用指南

### 启用预热

```bash
export RETH_ENABLE_MPT_PREWARM=true
export RETH_PREWARM_TIMEOUT_MS=100
export RETH_PREWARM_CONTRACTS_ONLY=true
export RETH_PREWARM_STORAGE_THRESHOLD=5
```

### 监控预热效果

```bash
curl http://localhost:6060/metrics | grep evm_prewarm
```

### 调优建议

1. **超时时间**：50-200ms
2. **存储阈值**：3-10
3. **预热范围**：根据负载动态调整
