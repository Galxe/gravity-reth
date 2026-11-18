#![allow(missing_docs)]

use reth_ethereum_primitives::Block;
use reth_provider::test_utils::MockEthProvider;

mod big_pooled_txs_req;
mod connect;
mod eth72;
mod multiplex;
mod requests;
mod session;
mod startup;
mod transaction_hash_fetching;
mod txgossip;

const fn main() {}

/// Seeds a mock provider with the chain-spec genesis block.
///
/// `MockEthProvider::with_genesis_block` was a v2.3.0-only helper; the provider crate
/// was restored to the gravity baseline which lacks it. The eth pool validator eagerly
/// resolves the latest header on construction, so tests must seed the genesis block or
/// the validator panics with `BestBlockNotFound`.
fn provider_with_genesis_block() -> MockEthProvider {
    // Pool validation uses the tip gas limit; mainnet genesis (5,000) cannot fit test txs.
    let mut genesis = reth_chainspec::MAINNET.genesis.clone();
    genesis.gas_limit = 30_000_000;
    let chain_spec = reth_chainspec::ChainSpecBuilder::mainnet().genesis(genesis).build();
    let provider = MockEthProvider::default().with_chain_spec(chain_spec);
    let genesis_hash = provider.chain_spec.genesis_hash();
    let genesis_header = provider.chain_spec.genesis_header().clone();
    provider.add_block(genesis_hash, Block::new(genesis_header, Default::default()));
    provider
}
