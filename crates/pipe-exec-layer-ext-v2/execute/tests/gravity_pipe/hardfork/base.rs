//! Scenarios that do not depend on a hardfork and run along the whole timeline.
//!
//! On every committed block: the header carries the number, hash and consensus parent id the
//! pipe reported, and an epoch-change block moves the on-chain epoch by one. (A block of the
//! previous epoch pushed after the first epoch change is rejected; that one needs the pipe
//! itself and is driven from `main`.) A pre-Alpha epoch-change block, whose body leaves out
//! `onBlockStart`, needs no scenario of its own: the replay check covers it.
//!
//! Before Alpha, one block each: user transactions calling the BLS precompile with enough
//! gas and with too little; a bridge deposit minting through `GBridgeReceiver`; a user
//! transaction calling the mint precompile's address; assorted invalid transactions between
//! valid ones (Beta runs that block again once EIP-7702 is unlocked). Before Alpha a red replay of
//! these blocks points at the scenario itself, not at Alpha's gas-exempt system transactions.

use super::{Chain, ScenarioBlock};
use crate::{
    node::{
        eip1559_tx, eip7702_tx, legacy_tx, BridgeDeposits, CommittedBlock, SignedTx, TestAccount,
        DELEGATE, MAX_FEE_PER_GAS,
    },
    report::BlockReport,
    timeline::{Fork, Phase},
};
use alloy_consensus::{TxEip1559, TxEip2930, TxEip4844, TxEip7702, TxLegacy};
use alloy_primitives::{hex, uint, Address, Bytes, TxKind, B256, U256};
use alloy_sol_macro::sol;
use alloy_sol_types::{SolCall, SolEvent};
use gravity_precompiles::{
    bls_pop_verify::BLS_PRECOMPILE_ADDR, mint::AUTHORIZED_CALLER as G_BRIDGE_RECEIVER,
};
use reth_pipe_exec_layer_ext_v2::onchain_config::{
    epoch::Reconfiguration, EPOCH_MANAGER_ADDR, NATIVE_MINT_PRECOMPILE_ADDR,
};
use revm::primitives::eip3860::MAX_INITCODE_SIZE;

sol! {
    event NativeMinted(address indexed recipient, uint256 amount, uint128 indexed nonce);
}

/// Base scenarios that take a block of their own, in the order they run.
const SCENARIO_BLOCKS: [PlanFn; 4] = [
    Base::bls_precompile,
    Base::bridge_mint,
    Base::user_calls_mint_address,
    Base::invalid_transactions,
];

/// A public key and its proof of possession; the precompile returns true for them.
const VALID_BLS_POP_INPUT: [u8; 144] = hex!(
    "8ae7e5822ba97ab07877ea318e747499da648b27302414f9d0b9bb7e3646d248"
    "be90c9fdaddfdb93485a6e9334f01093"
    "b16db5b947dda6c513b24b8724b659996826bfb69a8914f1b295e39572f40923"
    "e08150a0bdd12d0ee920e9a1e33acf81192230e9f074e350555315a427264246"
    "ab03b99601738c4179746e73913388b68285a854e85be32b1539ec925dd3d7fe"
);
/// Leaves the precompile well over its flat 110 000 gas.
pub(super) const BLS_ENOUGH_GAS: u64 = 200_000;
/// Above the intrinsic gas, so the transaction is executed, but after it less than the
/// precompile's flat charge is left: the call runs out of gas inside the precompile.
const BLS_TOO_LITTLE_GAS: u64 = 100_000;

/// Receives the bridge deposit and nothing else, so its balance change is the deposit.
const DEPOSIT_RECIPIENT: Address = Address::repeat_byte(0xd1);
const DEPOSIT_AMOUNT: U256 = uint!(1_234_567_890_123_456_789_U256);

/// Named in a user transaction's mint request; user transactions have no mint precompile,
/// so it never receives anything.
const MINT_REQUEST_RECIPIENT: Address = Address::repeat_byte(0xd2);

/// Init code deploying the one-byte runtime `STOP`: `MSTORE8(0, 0) RETURN(0, 1)`.
const STOP_CONTRACT_INIT_CODE: [u8; 10] = hex!("600060005360016000f3");
/// Gas limit of scenario transactions that do more than a transfer; ample for each.
pub(super) const GAS_LIMIT: u64 = 100_000;

/// Plans one scenario block; the block is checked against the returned expectation once
/// committed. `parent` is the last committed block.
type PlanFn = fn(&mut Base, &Chain<'_>, u64) -> (ScenarioBlock, Expected);

#[derive(Debug, Default)]
pub(super) struct Base {
    /// How many of `SCENARIO_BLOCKS` have been planned.
    planned: usize,
    /// What the block planned last must show once committed.
    expected: Option<Expected>,
    bridge: BridgeDeposits,
}

impl Base {
    pub(super) fn next_block(
        &mut self,
        chain: &Chain<'_>,
        phase: Phase,
        parent: u64,
    ) -> Option<ScenarioBlock> {
        // Activation blocks belong to their hardfork's scenarios.
        if phase.has_activated(Fork::Alpha) || matches!(phase, Phase::Activation(_)) {
            return None;
        }
        let plan = SCENARIO_BLOCKS.get(self.planned)?;
        self.planned += 1;
        let (block, expected) = plan(self, chain, parent);
        self.expected = Some(expected);
        Some(block)
    }

    pub(super) fn after_commit(
        &mut self,
        chain: &Chain<'_>,
        block: &CommittedBlock,
        report: &mut BlockReport<'_>,
    ) {
        check_header(chain, block, report);
        if block.epoch_changed {
            check_epoch(chain, block, report);
        }
        if let Some(expected) = self.expected.take() {
            expected.check(chain, block, report);
        }
    }

    pub(super) fn assert_all_ran(&self) {
        assert_eq!(
            self.planned,
            SCENARIO_BLOCKS.len(),
            "only {} of {} base scenario blocks ran before Alpha",
            self.planned,
            SCENARIO_BLOCKS.len()
        );
    }

    /// Alice calls the BLS precompile with enough gas, Bob with too little: Alice's call
    /// succeeds, Bob's runs out of gas and fails without stopping the pipe.
    fn bls_precompile(&mut self, chain: &Chain<'_>, parent: u64) -> (ScenarioBlock, Expected) {
        let enough_gas = bls_call(chain, parent, TestAccount::Alice, BLS_ENOUGH_GAS);
        let out_of_gas = bls_call(chain, parent, TestAccount::Bob, BLS_TOO_LITTLE_GAS);
        let expected = Expected::BlsPrecompile {
            enough_gas: enough_gas.hash(),
            out_of_gas: out_of_gas.hash(),
        };
        (
            ScenarioBlock { transactions: vec![enough_gas, out_of_gas], ..Default::default() },
            expected,
        )
    }

    /// A bridge deposit arrives as block extra data; the oracle callback mints it.
    fn bridge_mint(&mut self, _chain: &Chain<'_>, _parent: u64) -> (ScenarioBlock, Expected) {
        let deposit = self.bridge.deposit(DEPOSIT_RECIPIENT, DEPOSIT_AMOUNT);
        let expected =
            Expected::BridgeMint { recipient: DEPOSIT_RECIPIENT, amount: DEPOSIT_AMOUNT };
        (ScenarioBlock { extra_data: vec![deposit], ..Default::default() }, expected)
    }

    /// Carol sends a well-formed mint request to the mint precompile's address. Only system
    /// transactions have the precompile, so for her the address is an empty account: the
    /// call succeeds and mints nothing.
    fn user_calls_mint_address(
        &mut self,
        chain: &Chain<'_>,
        parent: u64,
    ) -> (ScenarioBlock, Expected) {
        let account = TestAccount::Carol;
        // Function 0x01 (mint), recipient, amount: the layout GBridgeReceiver sends.
        let request =
            [&[0x01], MINT_REQUEST_RECIPIENT.as_slice(), &DEPOSIT_AMOUNT.to_be_bytes::<32>()]
                .concat();
        let call = account.sign(TxEip1559 {
            gas_limit: GAS_LIMIT,
            input: request.into(),
            ..eip1559_tx(
                chain.chain_id,
                chain.nonce(account.address(), parent),
                TxKind::Call(NATIVE_MINT_PRECOMPILE_ADDR),
            )
        });
        let expected = Expected::UserCallsMintAddress { tx: call.hash() };
        (ScenarioBlock { transactions: vec![call], ..Default::default() }, expected)
    }

    /// Assorted invalid transactions between valid ones, with EIP-7702 still locked down.
    fn invalid_transactions(
        &mut self,
        chain: &Chain<'_>,
        parent: u64,
    ) -> (ScenarioBlock, Expected) {
        let (block, inclusion) = invalid_transactions_block(chain, parent, false);
        (block, Expected::InvalidTransactions { inclusion })
    }
}

/// Dave's valid transactions of every enabled envelope, including a contract creation, with
/// invalid ones between them that the pipe must drop before execution. Each invalid transaction
/// is valid but for one defect, so that defect is what drops it.
///
/// Until Beta, the lockdown makes an EIP-7702 transaction one of the invalid ones. Once
/// `eip7702_unlocked`, it is one of the valid ones, and EIP-7702 transactions without an
/// authorization or below their intrinsic gas take its place among the invalid ones.
pub(super) fn invalid_transactions_block(
    chain: &Chain<'_>,
    parent: u64,
    eip7702_unlocked: bool,
) -> (ScenarioBlock, Inclusion) {
    let (account, chain_id) = (TestAccount::Dave, chain.chain_id);
    let nonce = chain.nonce(account.address(), parent);
    let alice = TestAccount::Alice.address();
    let to_alice = TxKind::Call(alice);
    let one_gwei = U256::from(1_000_000_000u64);
    let bob = TestAccount::Bob;
    let delegate_bob =
        || vec![bob.authorize(chain_id, DELEGATE, chain.nonce(bob.address(), parent))];

    let mut valid = vec![
        account.sign(TxLegacy { value: one_gwei, ..legacy_tx(chain_id, nonce, to_alice) }),
        account.sign(TxEip2930 {
            chain_id,
            nonce: nonce + 1,
            gas_price: MAX_FEE_PER_GAS,
            gas_limit: 21_000,
            to: to_alice,
            value: one_gwei,
            access_list: Default::default(),
            input: Bytes::new(),
        }),
        account.sign(TxEip1559 {
            gas_limit: GAS_LIMIT,
            input: Bytes::from(STOP_CONTRACT_INIT_CODE),
            ..eip1559_tx(chain_id, nonce + 2, TxKind::Create)
        }),
    ];
    if eip7702_unlocked {
        valid.push(account.sign(eip7702_tx(chain_id, nonce + 3, alice, delegate_bob())));
    }

    // Unless the defect is its nonce, every invalid transaction takes the nonce the last valid
    // one then uses.
    let next = nonce + valid.len() as u64;
    let template = eip1559_tx(chain_id, next, to_alice);
    let eip7702 = eip7702_tx(chain_id, next, alice, delegate_bob());
    let mut invalid = vec![
        // Nonce too high, nonce already used.
        account.sign(TxEip1559 { nonce: next + 1, ..template.clone() }),
        account.sign(TxEip1559 { nonce, ..template.clone() }),
        // Signed for Ethereum mainnet.
        account.sign(legacy_tx(1, next, to_alice)),
        // Fee cap below the base fee; tip above the fee cap.
        account.sign(TxEip1559 {
            max_fee_per_gas: 1_000_000_000,
            max_priority_fee_per_gas: 0,
            ..template.clone()
        }),
        account.sign(TxEip1559 {
            max_priority_fee_per_gas: template.max_fee_per_gas + 1,
            ..template.clone()
        }),
        // Below the intrinsic gas; more value than the balance.
        account.sign(TxEip1559 { gas_limit: 20_999, ..template.clone() }),
        account.sign(TxEip1559 { value: U256::MAX, ..template.clone() }),
        // Blob transactions are unsupported on Gravity.
        account.sign(TxEip4844 {
            chain_id,
            nonce: next,
            gas_limit: GAS_LIMIT,
            max_fee_per_gas: template.max_fee_per_gas,
            max_priority_fee_per_gas: template.max_priority_fee_per_gas,
            to: alice,
            value: U256::ZERO,
            access_list: Default::default(),
            blob_versioned_hashes: vec![],
            max_fee_per_blob_gas: 1,
            input: Bytes::new(),
        }),
        // Init code over the EIP-3860 limit.
        account.sign(TxEip1559 {
            gas_limit: 1_000_000,
            input: Bytes::from(vec![0; MAX_INITCODE_SIZE + 1]),
            ..eip1559_tx(chain_id, next, TxKind::Create)
        }),
    ];
    if eip7702_unlocked {
        // No authorization; one gas below the intrinsic gas of its one authorization.
        invalid.push(account.sign(TxEip7702 { authorization_list: vec![], ..eip7702.clone() }));
        invalid.push(account.sign(TxEip7702 { gas_limit: eip7702.gas_limit - 1, ..eip7702 }));
    } else {
        invalid.push(account.sign(eip7702));
    }
    // A sender absent from state.
    invalid.push(TestAccount::Unfunded.sign(eip1559_tx(chain_id, 0, to_alice)));
    let last_valid = account.sign(template);

    let transactions = valid
        .into_iter()
        .map(|tx| (tx, true))
        .chain(invalid.into_iter().map(|tx| (tx, false)))
        .chain([(last_valid, true)])
        .collect();
    Inclusion::plan(transactions)
}

/// `account` calls the BLS precompile with a valid proof of possession and `gas_limit`, as its
/// next transaction after block `parent`.
pub(super) fn bls_call(
    chain: &Chain<'_>,
    parent: u64,
    account: TestAccount,
    gas_limit: u64,
) -> SignedTx {
    account.sign(TxEip1559 {
        gas_limit,
        input: Bytes::from(VALID_BLS_POP_INPUT),
        ..eip1559_tx(
            chain.chain_id,
            chain.nonce(account.address(), parent),
            TxKind::Call(BLS_PRECOMPILE_ADDR),
        )
    })
}

/// Transactions submitted in one block, of which exactly the `included` ones must be in it, in
/// submission order.
#[derive(Debug)]
pub(super) struct Inclusion {
    submitted: Vec<B256>,
    pub(super) included: Vec<B256>,
}

impl Inclusion {
    /// The block carrying `transactions` in order, and which of them it must include.
    pub(super) fn plan(transactions: Vec<(SignedTx, bool)>) -> (ScenarioBlock, Self) {
        let submitted = transactions.iter().map(|(tx, _)| tx.hash()).collect();
        let included = transactions
            .iter()
            .filter(|(_, included)| *included)
            .map(|(tx, _)| tx.hash())
            .collect();
        let transactions = transactions.into_iter().map(|(tx, _)| tx).collect();
        (ScenarioBlock { transactions, ..Default::default() }, Self { submitted, included })
    }

    /// The committed body holds exactly the included transactions, in order, and none of the
    /// others.
    pub(super) fn check(
        &self,
        chain: &Chain<'_>,
        block: &CommittedBlock,
        report: &mut BlockReport<'_>,
        source: &'static str,
    ) {
        let included: Vec<B256> = chain
            .block(block.number)
            .transactions
            .hashes()
            .filter(|hash| self.submitted.contains(hash))
            .collect();
        report.check_eq(source, None, "included test transactions", &self.included, &included);
    }
}

/// What a scenario block must show once committed.
#[derive(Debug)]
enum Expected {
    BlsPrecompile {
        enough_gas: B256,
        out_of_gas: B256,
    },
    BridgeMint {
        recipient: Address,
        amount: U256,
    },
    UserCallsMintAddress {
        tx: B256,
    },
    /// Of the submitted transactions, exactly the valid ones are in the block, in order.
    InvalidTransactions {
        inclusion: Inclusion,
    },
}

impl Expected {
    fn check(self, chain: &Chain<'_>, block: &CommittedBlock, report: &mut BlockReport<'_>) {
        match self {
            Self::BlsPrecompile { enough_gas, out_of_gas } => {
                let source = "scenario: BLS precompile";
                check_tx_success(chain, report, source, enough_gas, true);
                check_tx_success(chain, report, source, out_of_gas, false);
            }
            Self::BridgeMint { recipient, amount } => {
                let source = "scenario: bridge deposit mint";
                check_balance_increase(chain, report, source, block.number, recipient, amount);
                let minted: Vec<(Address, U256)> = chain
                    .receipts(block.number)
                    .iter()
                    .flat_map(|receipt| receipt.inner.logs())
                    .filter(|log| log.address() == G_BRIDGE_RECEIVER)
                    .filter_map(|log| NativeMinted::decode_log(&log.inner).ok())
                    .map(|log| (log.data.recipient, log.data.amount))
                    .collect();
                report.check_eq(
                    source,
                    None,
                    "NativeMinted logs of GBridgeReceiver (recipient, amount)",
                    vec![(recipient, amount)],
                    minted,
                );
            }
            Self::UserCallsMintAddress { tx } => {
                let source = "scenario: user call to mint address";
                check_tx_success(chain, report, source, tx, true);
                check_balance_increase(
                    chain,
                    report,
                    source,
                    block.number,
                    MINT_REQUEST_RECIPIENT,
                    U256::ZERO,
                );
            }
            Self::InvalidTransactions { inclusion } => {
                inclusion.check(chain, block, report, "scenario: invalid transactions");
            }
        }
    }
}

/// The committed header carries what the pipe reported for the block.
fn check_header(chain: &Chain<'_>, block: &CommittedBlock, report: &mut BlockReport<'_>) {
    let source = "scenario: block header";
    let header = chain.block(block.number).header;
    report.check_eq(source, None, "number", block.number, header.number);
    report.check_eq(source, None, "hash", block.hash, header.hash);
    let parent_hash = chain.block(block.number - 1).header.hash;
    report.check_eq(source, None, "parent hash", parent_hash, header.parent_hash);
    // The pipe stores the consensus id of the parent where Ethereum keeps the beacon root.
    report.check_eq(
        source,
        None,
        "parent beacon block root (consensus parent id)",
        Some(block.parent_id),
        header.parent_beacon_block_root,
    );
}

/// The block that delivered the DKG transcript moved the on-chain epoch by one.
fn check_epoch(chain: &Chain<'_>, block: &CommittedBlock, report: &mut BlockReport<'_>) {
    let source = "scenario: epoch change";
    let epoch_after = |number| {
        let input = Reconfiguration::currentEpochCall {}.abi_encode().into();
        let output = chain.call(EPOCH_MANAGER_ADDR, input, number);
        Reconfiguration::currentEpochCall::abi_decode_returns(&output).unwrap()
    };
    report.check_eq(source, None, "epoch before", block.epoch - 1, epoch_after(block.number - 1));
    report.check_eq(source, None, "epoch after", block.epoch, epoch_after(block.number));
}

/// The transaction is in a committed block and succeeded (or failed) as expected.
pub(super) fn check_tx_success(
    chain: &Chain<'_>,
    report: &mut BlockReport<'_>,
    source: &'static str,
    tx: B256,
    success: bool,
) {
    let actual = chain.receipt(tx).map(|receipt| receipt.status());
    report.check_eq(source, None, format!("{tx} included, success"), Some(success), actual);
}

/// `address` gained exactly `amount` in block `number`.
fn check_balance_increase(
    chain: &Chain<'_>,
    report: &mut BlockReport<'_>,
    source: &'static str,
    number: u64,
    address: Address,
    amount: U256,
) {
    let increase = chain.balance(address, number).checked_sub(chain.balance(address, number - 1));
    report.check_eq(source, None, format!("{address} balance increase"), Some(amount), increase);
}
