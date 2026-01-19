//! Data Source Factory
//!
//! Creates data sources from OracleTaskConfig.config bytes based on sourceType.

use crate::{
    blockchain_source::BlockchainEventSource,
    data_source::{source_types, DataSourceKind},
};
use alloy_primitives::{Bytes, U256};
use anyhow::{anyhow, Result};

/// Factory for creating data sources from chain configuration
///
/// This factory dispatches to the appropriate source implementation based on
/// the sourceType from OracleTaskConfig.
#[derive(Debug, Clone, Copy, Default)]
pub struct DataSourceFactory;

impl DataSourceFactory {
    /// Create a data source from chain configuration
    ///
    /// # Arguments
    /// * `source_type` - The source type (0=BLOCKCHAIN, etc.)
    /// * `source_id` - The source identifier (chain ID, etc.)
    /// * `config` - ABI-encoded configuration from OracleTaskConfig
    ///
    /// # Returns
    /// * `Result<DataSourceKind>` - The created data source or an error
    pub async fn create(
        source_type: u32,
        source_id: U256,
        config: Bytes,
    ) -> Result<DataSourceKind> {
        match source_type {
            source_types::BLOCKCHAIN => {
                let source = BlockchainEventSource::from_config(source_id, &config).await?;
                Ok(DataSourceKind::Blockchain(source))
            }
            _ => Err(anyhow!("Unknown source type: {}", source_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::OracleDataSource;

    use super::*;
    use alloy_primitives::address;
    use alloy_sol_types::SolValue;

    #[tokio::test]
    async fn test_create_blockchain_source() {
        // Encode config: (rpcUrl, portalAddress, startBlock)
        let config = (
            "https://rpc.example.com".to_string(),
            address!("5FbDB2315678afecb367f032d93F642f64180aa3"),
            0u64,
        )
            .abi_encode();

        let result = DataSourceFactory::create(
            source_types::BLOCKCHAIN,
            U256::from(1), // Ethereum chain ID
            Bytes::from(config),
        )
        .await;

        assert!(result.is_ok());
        let source = result.unwrap();
        assert_eq!(source.source_type(), source_types::BLOCKCHAIN);
        assert_eq!(source.source_id(), U256::from(1));
    }

    #[tokio::test]
    async fn test_create_unknown_source_type() {
        let result = DataSourceFactory::create(
            99, // Unknown type
            U256::from(1),
            Bytes::new(),
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown source type"));
    }
}
