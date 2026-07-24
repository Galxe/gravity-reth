#![allow(missing_docs)]

use reth_ethereum_primitives::Block;
use reth_provider::test_utils::MockEthProvider;

mod big_pooled_txs_req;
mod connect;
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
    let provider = MockEthProvider::default();
    let genesis_hash = provider.chain_spec.genesis_hash();
    let genesis_header = provider.chain_spec.genesis_header().clone();
    provider.add_block(genesis_hash, Block::new(genesis_header, Default::default()));
    provider
}
