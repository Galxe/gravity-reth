use crate::{ChainSpec, DepositContract};
use alloc::{boxed::Box, vec::Vec};
use alloy_chains::Chain;
use alloy_consensus::Header;
use alloy_eips::{calc_next_block_base_fee, eip1559::BaseFeeParams, eip7840::BlobParams};
use alloy_genesis::Genesis;
use alloy_primitives::{B256, U256};
use core::fmt::{Debug, Display};
use reth_ethereum_forks::EthereumHardforks;
use reth_network_peers::NodeRecord;
use reth_primitives_traits::{AlloyBlockHeader, BlockHeader};

/// Trait representing type configuring a chain spec.
#[auto_impl::auto_impl(&, Arc)]
pub trait EthChainSpec: Send + Sync + Unpin + Debug {
    /// The header type of the network.
    type Header: BlockHeader;

    /// Returns the [`Chain`] object this spec targets.
    fn chain(&self) -> Chain;

    /// Returns the chain id number
    fn chain_id(&self) -> u64 {
        self.chain().id()
    }

    /// Get the [`BaseFeeParams`] for the chain at the given timestamp.
    fn base_fee_params_at_timestamp(&self, timestamp: u64) -> BaseFeeParams;

    /// Get the [`BlobParams`] for the given timestamp
    fn blob_params_at_timestamp(&self, timestamp: u64) -> Option<BlobParams>;

    /// Returns the deposit contract data for the chain, if it's present
    fn deposit_contract(&self) -> Option<&DepositContract>;

    /// The genesis hash.
    fn genesis_hash(&self) -> B256;

    /// The delete limit for pruner, per run.
    fn prune_delete_limit(&self) -> usize;

    /// Returns a string representation of the hardforks.
    fn display_hardforks(&self) -> Box<dyn Display>;

    /// The genesis header.
    fn genesis_header(&self) -> &Self::Header;

    /// The genesis block specification.
    fn genesis(&self) -> &Genesis;

    /// The bootnodes for the chain, if any.
    fn bootnodes(&self) -> Option<Vec<NodeRecord>>;

    /// Returns `true` if this chain contains Optimism configuration.
    fn is_optimism(&self) -> bool {
        self.chain().is_optimism()
    }

    /// Returns `true` if this chain contains Ethereum configuration.
    fn is_ethereum(&self) -> bool {
        self.chain().is_ethereum()
    }

    /// Returns the final total difficulty if the Paris hardfork is known.
    fn final_paris_total_difficulty(&self) -> Option<U256>;

    /// Returns the Gravity-specific hardforks and their activation conditions.
    ///
    /// Callers use the generic [`Hardforks`] trait to query activation:
    /// ```ignore
    /// use reth_chainspec::GravityHardfork;
    /// chain_spec.gravity_hardforks().is_fork_active_at_timestamp(GravityHardfork::Alpha, ts);
    /// chain_spec.gravity_hardforks().fork(GravityHardfork::Gamma).transitions_at_block(n);
    /// ```
    fn gravity_hardforks(&self) -> &reth_ethereum_forks::ChainHardforks;

    /// Returns the Gravity protocol minimum base fee (in wei) applicable at the given
    /// block, or `None` if no floor applies at that height.
    ///
    /// The schedule of activation block(s) and any historical floor values is encoded
    /// in branch-specific code (see this branch's `ChainSpec` impl). Chainspecs that
    /// are not Gravity (e.g. Ethereum mainnet during reth history sync) return `None`
    /// for all blocks via the trait default.
    fn gravity_min_base_fee_at_block(&self, _block: u64) -> Option<u64> {
        None
    }

    /// See [`calc_next_block_base_fee`].
    ///
    /// When the Gravity floor is active for the next block (see
    /// [`Self::gravity_min_base_fee_at_block`]), the EIP-1559 recurrence is clamped at
    /// the floor and the floor is used as the parent fallback for pre-London headers.
    /// When no floor is active, upstream EIP-1559 behavior applies and pre-London
    /// parents return `None`.
    fn next_block_base_fee(&self, parent: &Self::Header, target_timestamp: u64) -> Option<u64> {
        let next_block = parent.number() + 1;
        let floor = self.gravity_min_base_fee_at_block(next_block);
        let parent_base_fee = parent.base_fee_per_gas().or(floor)?;
        let next = calc_next_block_base_fee(
            parent.gas_used(),
            parent.gas_limit(),
            parent_base_fee,
            self.base_fee_params_at_timestamp(target_timestamp),
        );
        Some(floor.map_or(next, |f| next.max(f)))
    }
}

impl EthChainSpec for ChainSpec {
    type Header = Header;

    fn chain(&self) -> Chain {
        self.chain
    }

    fn base_fee_params_at_timestamp(&self, timestamp: u64) -> BaseFeeParams {
        self.base_fee_params_at_timestamp(timestamp)
    }

    fn blob_params_at_timestamp(&self, timestamp: u64) -> Option<BlobParams> {
        if let Some(blob_param) = self.blob_params.active_scheduled_params_at_timestamp(timestamp) {
            Some(*blob_param)
        } else if self.is_osaka_active_at_timestamp(timestamp) {
            Some(self.blob_params.osaka)
        } else if self.is_prague_active_at_timestamp(timestamp) {
            Some(self.blob_params.prague)
        } else if self.is_cancun_active_at_timestamp(timestamp) {
            Some(self.blob_params.cancun)
        } else {
            None
        }
    }

    fn deposit_contract(&self) -> Option<&DepositContract> {
        self.deposit_contract.as_ref()
    }

    fn genesis_hash(&self) -> B256 {
        self.genesis_hash()
    }

    fn prune_delete_limit(&self) -> usize {
        self.prune_delete_limit
    }

    fn display_hardforks(&self) -> Box<dyn Display> {
        Box::new(Self::display_hardforks(self))
    }

    fn genesis_header(&self) -> &Self::Header {
        self.genesis_header()
    }

    fn genesis(&self) -> &Genesis {
        self.genesis()
    }

    fn bootnodes(&self) -> Option<Vec<NodeRecord>> {
        self.bootnodes()
    }

    fn is_optimism(&self) -> bool {
        false
    }

    fn final_paris_total_difficulty(&self) -> Option<U256> {
        self.paris_block_and_final_difficulty.map(|(_, final_difficulty)| final_difficulty)
    }

    fn gravity_hardforks(&self) -> &reth_ethereum_forks::ChainHardforks {
        &self.gravity_hardforks
    }

    /// Gravity base fee floor schedule for the **main** branch.
    ///
    /// Single segment: `[gravity_min_base_fee_activation_block, ∞)` returns the genesis
    /// `gravityMinBaseFee` value; earlier blocks and chainspecs without the genesis
    /// field return `None`. Released testnet branches read the activation block from
    /// genesis (so the rolling-upgrade height is configurable per-network) and may
    /// extend this function with hardcoded historical segments — e.g. a v1.6 upgrade
    /// stepping the floor at block N would add `[M, N) -> Some(50_000_000_000)` for
    /// the v1.5-era value.
    fn gravity_min_base_fee_at_block(&self, block: u64) -> Option<u64> {
        if block >= self.gravity_min_base_fee_activation_block {
            self.gravity_min_base_fee
        } else {
            None
        }
    }
}
