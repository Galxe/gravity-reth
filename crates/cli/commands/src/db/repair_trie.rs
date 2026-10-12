use clap::Parser;
use reth_provider::{providers::ProviderNodeTypes, ProviderFactory};

/// The arguments for the `reth db repair-trie` command
#[derive(Parser, Debug)]
pub struct Command {
    /// Only show inconsistencies without making any repairs
    #[arg(long)]
    pub(crate) dry_run: bool,
}

impl Command {
    /// The legacy verifier cannot inspect the V2 trie node format.
    pub fn execute<N: ProviderNodeTypes>(
        self,
        _provider_factory: ProviderFactory<N>,
    ) -> eyre::Result<()> {
        Err(eyre::eyre!("db repair-trie does not support the V2 trie; no repairs were made"))
    }
}
