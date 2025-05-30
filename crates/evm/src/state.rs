use alloy_primitives::{map::HashMap, Address, U256};
use reth_grevm::{ParallelBundleState, ParallelState};
use revm::{
    db::{states::bundle_state::BundleRetention, BundleState},
    primitives::{Account, AccountInfo},
    Database, DatabaseCommit, TransitionState,
};
use std::error::Error;

pub trait State {
    fn bundle_size_hint(&self) -> usize;

    fn take_bundle(&mut self) -> BundleState;

    fn merge_transitions(&mut self, retention: BundleRetention);

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Box<dyn Error>>;

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Box<dyn Error>>;

    fn commit_changes(&mut self, changes: HashMap<Address, Account>);
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

    fn commit_changes(&mut self, changes: HashMap<Address, Account>) {
        // Load all accounts in the changes map to ensure they are cached before committing.
        for address in changes.keys() {
            self.load_cache_account(*address).unwrap();
        }
        self.commit(changes);
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
        if let Some(transition_state) = self.transition_state.as_mut().map(TransitionState::take) {
            self.bundle_state
                .parallel_apply_transitions_and_create_reverts(transition_state, retention);
        }
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

    fn commit_changes(&mut self, changes: HashMap<Address, Account>) {
        // Load all accounts in the changes map to ensure they are cached before committing.
        for address in changes.keys() {
            self.load_mut_cache_account(*address).unwrap();
        }
        self.commit(changes);
    }
}
