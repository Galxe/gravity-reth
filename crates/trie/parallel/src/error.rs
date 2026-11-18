//! Errors from state-root calculation and parallel proof workers.

use reth_execution_errors::{
    SparseStateTrieError, SparseTrieError, StateProofError, StorageRootError,
};
use reth_provider::ProviderError;
use reth_storage_errors::db::DatabaseError;
use thiserror::Error;

/// Error returned by the state-root task and parallel proof workers.
#[derive(Error, Debug)]
pub enum StateRootTaskError {
    /// Error while calculating a storage root in the retained parallel calculator.
    #[error(transparent)]
    StorageRoot(#[from] StorageRootError),
    /// Provider error.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Sparse trie error.
    #[error(transparent)]
    SparseTrie(#[from] SparseTrieError),
    /// Other unspecified error. Retained for downstream constructor compatibility.
    #[error("{_0}")]
    Other(String),
}

impl From<StateRootTaskError> for ProviderError {
    fn from(error: StateRootTaskError) -> Self {
        match error {
            StateRootTaskError::Provider(error) => error,
            StateRootTaskError::StorageRoot(StorageRootError::Database(error)) => {
                Self::Database(error)
            }
            StateRootTaskError::SparseTrie(error) => Self::other(error),
            StateRootTaskError::Other(error) => Self::Database(DatabaseError::Other(error)),
        }
    }
}

impl From<alloy_rlp::Error> for StateRootTaskError {
    fn from(error: alloy_rlp::Error) -> Self {
        Self::Provider(ProviderError::Rlp(error))
    }
}

impl From<StateProofError> for StateRootTaskError {
    fn from(error: StateProofError) -> Self {
        Self::Provider(error.into())
    }
}

impl From<SparseStateTrieError> for StateRootTaskError {
    fn from(error: SparseStateTrieError) -> Self {
        Self::Provider(ProviderError::other(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie_inconsistency_retains_typed_source_and_legacy_alias() {
        let error =
            StateRootTaskError::from(StateProofError::TrieInconsistency("missing branch".into()));
        let provider: ProviderError = error.into();
        assert!(matches!(provider.downcast_other_ref::<StateProofError>(),
            Some(StateProofError::TrieInconsistency(message)) if message == "missing branch"));

        let legacy = crate::root::ParallelStateRootError::Other("legacy caller".into());
        assert!(
            matches!(ProviderError::from(legacy), ProviderError::Database(DatabaseError::Other(message)) if message == "legacy caller")
        );
    }
}
