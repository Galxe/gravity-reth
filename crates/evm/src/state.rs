use alloy_primitives::{Address, U256};
use reth_grevm::ParallelState;
use revm::{
    db::{states::bundle_state::BundleRetention, BundleState},
    primitives::AccountInfo,
    Database,
};
use std::error::Error;

pub trait State {
    fn bundle_size_hint(&self) -> usize;

    fn take_bundle(&mut self) -> BundleState;

    fn merge_transitions(&mut self, retention: BundleRetention);

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Box<dyn Error>>;

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Box<dyn Error>>;
}

impl<DB> State for revm::db::states::State<DB>
where
    DB: crate::Database,
{
    fn bundle_size_hint(&self) -> usize {
        self.bundle_size_hint()
    }

    fn take_bundle(&mut self) -> BundleState {
        self.take_bundle()
    }

    fn merge_transitions(&mut self, retention: BundleRetention) {
        self.merge_transitions(retention);
    }

    fn basic(
        &mut self,
        address: Address,
    ) -> Result<Option<AccountInfo>, Box<dyn std::error::Error>> {
        Database::basic(self, address).map_err(Into::into)
    }

    fn storage(
        &mut self,
        address: Address,
        index: U256,
    ) -> Result<U256, Box<dyn std::error::Error>> {
        Database::storage(self, address, index).map_err(Into::into)
    }
}

impl<DB> State for ParallelState<DB>
where
    DB: crate::ParallelDatabase,
{
    fn bundle_size_hint(&self) -> usize {
        self.bundle_size_hint()
    }

    fn take_bundle(&mut self) -> BundleState {
        self.take_bundle()
    }

    fn merge_transitions(&mut self, retention: BundleRetention) {
        self.merge_transitions(retention);
    }

    fn basic(
        &mut self,
        address: Address,
    ) -> Result<Option<AccountInfo>, Box<dyn std::error::Error>> {
        Database::basic(self, address).map_err(Into::into)
    }

    fn storage(
        &mut self,
        address: Address,
        index: U256,
    ) -> Result<U256, Box<dyn std::error::Error>> {
        Database::storage(self, address, index).map_err(Into::into)
    }
}
