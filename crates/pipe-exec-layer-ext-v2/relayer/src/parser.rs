//! URI parser for gravity protocol tasks

use alloy_primitives::{Address, B256};
use alloy_rpc_types::{BlockNumberOrTag, Filter, Topic};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

/// Defines supported task type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GravityTask {
    /// Monitor event task, contains a Filter object that can be directly used with Alloy
    MonitorEvent(Filter),
    /// Monitor block head task
    MonitorBlockHead,
    /// Monitor storage slot task
    MonitorStorage {
        /// Account address to monitor storage for
        account: Address,
        /// Storage slot to monitor
        slot: B256,
    },
    /// Monitor account activity task (abstract layer)
    MonitorAccount {
        /// Address of the account to monitor
        address: Address,
        /// Type of activity to monitor for this account
        activity_type: AccountActivityType,
    },
}

/// Account activity types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountActivityType {
    /// ERC20 token transfer
    Erc20Transfer,
    /// All transactions
    AllTransactions,
}

/// Represents a parsed gravity protocol task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedTask {
    /// The parsed gravity task to be executed
    pub task: GravityTask,
    /// The original URI string that was parsed
    pub original_uri: String,
    /// The chain identifier (e.g., "mainnet", "testnet")
    pub chain_specifier: String,
}

/// URI parser for gravity protocol tasks
///
/// This struct provides functionality to parse gravity protocol URIs
/// into structured task objects that can be executed by the relayer.
#[derive(Debug, Default)]
pub struct UriParser;

impl UriParser {
    /// Creates a new UriParser instance
    ///
    /// # Returns
    /// * `UriParser` - A new URI parser instance
    pub fn new() -> Self {
        Self
    }

    /// Parse gravity URI
    ///
    /// Supported new formats:
    /// - gravity://mainnet/block?strategy=head - Monitor latest block
    /// - gravity://mainnet/event?address=0x...&topic0=0x... - Monitor events
    /// - gravity://mainnet/storage?account=0x...&slot=0x... - Monitor storage slot
    /// - gravity://mainnet/account/0x.../activity?type=erc20_transfer - Monitor account activity
    pub fn parse(&self, uri_str: &str) -> Result<ParsedTask> {
        let uri = Url::parse(uri_str)?;

        if uri.scheme() != "gravity" {
            return Err(anyhow!("Invalid scheme: expected 'gravity'"));
        }

        let chain_specifier =
            uri.host_str().ok_or_else(|| anyhow!("Missing chain specifier in URI"))?.to_string();

        let path = uri.path();
        let params: HashMap<_, _> = uri.query_pairs().into_owned().collect();

        let task = match path {
            "/event" => self.parse_event_task(&params)?,
            "/block" => self.parse_block_task(&params)?,
            "/storage" => self.parse_storage_task(&params)?,
            path if path.starts_with("/account/") => self.parse_account_task(path, &params)?,
            _ => return Err(anyhow!("Unsupported resource path: {}", path)),
        };

        Ok(ParsedTask { task, original_uri: uri_str.to_string(), chain_specifier })
    }

    /// Parses event monitoring task parameters
    ///
    /// # Arguments
    /// * `params` - Query parameters containing event filter configuration
    ///
    /// # Returns
    /// * `Result<GravityTask>` - The parsed event monitoring task or error
    ///
    /// # Errors
    /// * Returns an error if required parameters are missing or invalid
    fn parse_event_task(&self, params: &HashMap<String, String>) -> Result<GravityTask> {
        let mut filter = Filter::new();

        if let Some(address_str) = params.get("address") {
            let address: Address = address_str
                .parse()
                .map_err(|e| anyhow!("Invalid address '{}': {}", address_str, e))?;
            filter = filter.address(address);
        }

        let topics: Result<Vec<Topic>, _> = (0..4)
            .filter_map(|i| {
                let topic_key = format!("topic{}", i);
                params.get(&topic_key).map(|topic_val_str| {
                    topic_val_str
                        .split(',')
                        .map(|s| s.trim().parse::<B256>())
                        .collect::<Result<Vec<B256>, _>>()
                        .map(Topic::from)
                        .map_err(|e| anyhow!("Invalid topic{} value '{}': {}", i, topic_val_str, e))
                })
            })
            .collect();

        filter = topics?.into_iter().enumerate().fold(filter, |filter, (i, topic)| match i {
            0 => filter.event_signature(topic),
            1 => filter.topic1(topic),
            2 => filter.topic2(topic),
            3 => filter.topic3(topic),
            _ => filter,
        });

        // Can add more filter conditions, such as fromBlock, toBlock, etc.
        if let Some(from_block_str) = params.get("fromBlock") {
            if from_block_str == "latest" {
                filter = filter.from_block(BlockNumberOrTag::Latest);
            } else if from_block_str == "earliest" {
                filter = filter.from_block(BlockNumberOrTag::Earliest);
            } else if from_block_str == "finalized" {
                filter = filter.from_block(BlockNumberOrTag::Finalized);
            } else if let Ok(block_num) = from_block_str.parse::<u64>() {
                filter = filter.from_block(BlockNumberOrTag::Number(block_num));
            }
        }

        Ok(GravityTask::MonitorEvent(filter))
    }

    /// Parses block monitoring task parameters
    ///
    /// # Arguments
    /// * `params` - Query parameters containing block monitoring strategy
    ///
    /// # Returns
    /// * `Result<GravityTask>` - The parsed block monitoring task or error
    ///
    /// # Errors
    /// * Returns an error if the strategy parameter is missing or unsupported
    fn parse_block_task(&self, params: &HashMap<String, String>) -> Result<GravityTask> {
        match params.get("strategy").map(|s| s.as_str()) {
            Some("head") => Ok(GravityTask::MonitorBlockHead),
            Some(strategy) => Err(anyhow!("Unsupported block strategy: {}", strategy)),
            None => Err(anyhow!("Missing 'strategy' parameter for block monitoring")),
        }
    }

    /// Parses storage monitoring task parameters
    ///
    /// # Arguments
    /// * `params` - Query parameters containing account and slot information
    ///
    /// # Returns
    /// * `Result<GravityTask>` - The parsed storage monitoring task or error
    ///
    /// # Errors
    /// * Returns an error if account or slot parameters are missing or invalid
    fn parse_storage_task(&self, params: &HashMap<String, String>) -> Result<GravityTask> {
        let account_str = params
            .get("account")
            .ok_or_else(|| anyhow!("Missing 'account' parameter for storage monitoring"))?;
        let slot_str = params
            .get("slot")
            .ok_or_else(|| anyhow!("Missing 'slot' parameter for storage monitoring"))?;

        let account: Address = account_str
            .parse()
            .map_err(|e| anyhow!("Invalid account address '{}': {}", account_str, e))?;
        let slot: B256 =
            slot_str.parse().map_err(|e| anyhow!("Invalid slot value '{}': {}", slot_str, e))?;

        Ok(GravityTask::MonitorStorage { account, slot })
    }

    /// Parses account activity monitoring task parameters
    ///
    /// # Arguments
    /// * `path` - The URI path containing account address
    /// * `params` - Query parameters containing activity type
    ///
    /// # Returns
    /// * `Result<GravityTask>` - The parsed account monitoring task or error
    ///
    /// # Errors
    /// * Returns an error if the path format is invalid or activity type is unsupported
    fn parse_account_task(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> Result<GravityTask> {
        // Path format: /account/0x.../activity
        let path_parts: Vec<&str> = path.split('/').collect();
        if path_parts.len() != 4 || path_parts[1] != "account" || path_parts[3] != "activity" {
            return Err(anyhow!("Invalid account path format: {}", path));
        }

        let address_str = path_parts[2];
        let address: Address = address_str
            .parse()
            .map_err(|e| anyhow!("Invalid account address '{}': {}", address_str, e))?;

        let activity_type = match params.get("type").map(|s| s.as_str()) {
            Some("erc20_transfer") => AccountActivityType::Erc20Transfer,
            Some("all_transactions") => AccountActivityType::AllTransactions,
            Some(activity_type) => {
                return Err(anyhow!("Unsupported activity type: {}", activity_type))
            }
            None => {
                return Err(anyhow!("Missing 'type' parameter for account activity monitoring"))
            }
        };

        Ok(GravityTask::MonitorAccount { address, activity_type })
    }

    /// Parse multiple URIs in batch
    ///
    /// # Arguments
    /// * `uris` - A slice of URI strings to parse
    ///
    /// # Returns
    /// * `Result<Vec<ParsedTask>>` - A vector of parsed tasks or error
    ///
    /// # Errors
    /// * Returns an error if any URI in the batch fails to parse
    pub fn parse_batch(&self, uris: &[String]) -> Result<Vec<ParsedTask>> {
        let mut tasks = Vec::new();
        for uri in uris {
            tasks.push(self.parse(uri)?);
        }
        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_parse_block_head_uri() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/block?strategy=head";

        let result = parser.parse(uri).unwrap();

        assert_eq!(result.chain_specifier, "mainnet");
        assert_eq!(result.original_uri, uri);

        match result.task {
            GravityTask::MonitorBlockHead => {}
            _ => panic!("Expected MonitorBlockHead task type"),
        }
    }

    #[test]
    fn test_parse_event_uri() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

        let result = parser.parse(uri).unwrap();

        assert_eq!(result.chain_specifier, "mainnet");

        match result.task {
            GravityTask::MonitorEvent(filter) => {
                println!("filter: {:?}", filter);
                // Verify filter contains correct address and topic
                assert!(filter.has_topics());
            }
            _ => panic!("Expected MonitorEvent task type"),
        }
    }

    #[test]
    fn test_parse_storage_uri() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/storage?account=0x123456789abcdef123456789abcdef1234567890&slot=0x0000000000000000000000000000000000000000000000000000000000000001";

        let result = parser.parse(uri).unwrap();

        match result.task {
            GravityTask::MonitorStorage { account, slot } => {
                assert_eq!(
                    account,
                    Address::from_str("0x123456789abcdef123456789abcdef1234567890").unwrap()
                );
                assert_eq!(
                    slot,
                    B256::from_str(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )
                    .unwrap()
                );
            }
            _ => panic!("Expected MonitorStorage task type"),
        }
    }

    #[test]
    fn test_parse_account_activity_uri() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/account/0x123456789abcdef123456789abcdef1234567890/activity?type=erc20_transfer";

        let result = parser.parse(uri).unwrap();

        match result.task {
            GravityTask::MonitorAccount { address, activity_type } => {
                assert_eq!(
                    address,
                    Address::from_str("0x123456789abcdef123456789abcdef1234567890").unwrap()
                );
                assert_eq!(activity_type, AccountActivityType::Erc20Transfer);
            }
            _ => panic!("Expected MonitorAccount task type"),
        }
    }

    #[test]
    fn test_parse_event_with_multiple_topics() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef&topic1=0x000000000000000000000000123456789abcdef123456789abcdef1234567890";

        let result = parser.parse(uri).unwrap();

        match result.task {
            GravityTask::MonitorEvent(filter) => {
                // Test passes if parsing is successful
                println!("filter: {:?}", filter);
                // Verify filter contains correct address and topic
                assert!(filter.has_topics());
            }
            _ => panic!("Expected MonitorEvent task type"),
        }
    }

    #[test]
    fn test_parse_event_with_or_condition() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/event?topic0=0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef,0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";

        let result = parser.parse(uri).unwrap();

        match result.task {
            GravityTask::MonitorEvent(_filter) => {
                // Test passes if parsing is successful
            }
            _ => panic!("Expected MonitorEvent task type"),
        }
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let parser = UriParser::new();
        let uri = "http://mainnet/block?strategy=head";

        let result = parser.parse(uri);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid scheme"));
    }

    #[test]
    fn test_parse_missing_chain_specifier() {
        let parser = UriParser::new();
        let uri = "gravity:///block?strategy=head";

        let result = parser.parse(uri);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing chain specifier"));
    }

    #[test]
    fn test_parse_unsupported_resource() {
        let parser = UriParser::new();
        let uri = "gravity://mainnet/unknown";

        let result = parser.parse(uri);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported resource path"));
    }

    #[test]
    fn test_parse_batch() {
        let parser = UriParser::new();
        let uris = vec![
            "gravity://mainnet/block?strategy=head".to_string(),
            "gravity://mainnet/event?address=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
                .to_string(),
        ];

        let results = parser.parse_batch(&uris).unwrap();
        assert_eq!(results.len(), 2);

        match &results[0].task {
            GravityTask::MonitorBlockHead => {}
            _ => panic!("Expected MonitorBlockHead task type"),
        }

        match &results[1].task {
            GravityTask::MonitorEvent(_) => {}
            _ => panic!("Expected MonitorEvent task type"),
        }
    }
}
