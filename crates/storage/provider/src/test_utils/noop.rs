//! Additional testing support for `NoopProvider`.

use crate::{providers::StaticFileProvider, StaticFileProviderFactory};
use reth_primitives_traits::NodePrimitives;
use std::path::PathBuf;

/// Re-exported for convenience
pub use reth_storage_api::noop::NoopProvider;

impl<C: Send + Sync, N: NodePrimitives> StaticFileProviderFactory for NoopProvider<C, N> {
    fn static_file_provider(&self) -> StaticFileProvider<Self::Primitives> {
        StaticFileProvider::read_only(PathBuf::default(), false).unwrap()
    }

    fn get_static_file_writer(
        &self,
        _block: alloy_primitives::BlockNumber,
        _segment: reth_static_file_types::StaticFileSegment,
    ) -> reth_errors::ProviderResult<
        crate::providers::StaticFileProviderRWRefMut<'_, Self::Primitives>,
    > {
        Err(reth_errors::ProviderError::ReadOnlyStaticFileAccess)
    }
}
