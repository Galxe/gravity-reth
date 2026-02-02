export TEST_URI="gravity://31337/event?address=0xe7f1725E7734CE288F8367e1Bb143E90bb3F0512&topic0=0x3915136b10c16c5f181f4774902f3baf9e44a5f700cabf5c826ee1caed313624";
export RPC_URL="http://localhost:8848"
cargo test --package reth-pipe-exec-layer-relayer --lib -- manager::tests::test_manager_run --exact --show-output
