//! Transaction environment conversion before execution.

use alloy_consensus::transaction::Recovered;
use alloy_evm::{FromRecoveredTx, ToTxEnv};
use reth_ethereum_primitives::TransactionSigned;
use revm::context::TxEnv;

/// A recovered transaction whose large authorization list is recovered in parallel.
///
/// Conversion stays lazy so callers can pass this through their configured `tx_env` hook.
#[derive(Debug)]
pub struct ParallelTxEnv<'a> {
    transaction: Recovered<&'a TransactionSigned>,
}

impl<'a> ParallelTxEnv<'a> {
    /// Wraps a transaction without recovering any authorizations yet.
    pub const fn new(transaction: Recovered<&'a TransactionSigned>) -> Self {
        Self { transaction }
    }
}

impl ToTxEnv<TxEnv> for ParallelTxEnv<'_> {
    fn to_tx_env(&self) -> TxEnv {
        #[cfg(feature = "std")]
        if let TransactionSigned::Eip7702(signed) = self.transaction.inner() &&
            signed.tx().authorization_list.len() >= PARALLEL_AUTHORIZATION_THRESHOLD
        {
            return parallel_authorization_tx_env(signed.tx(), self.transaction.signer());
        }

        TxEnv::from_recovered_tx(self.transaction.inner(), self.transaction.signer())
    }
}

#[cfg(feature = "std")]
const PARALLEL_AUTHORIZATION_THRESHOLD: usize = 16;

#[cfg(feature = "std")]
fn parallel_authorization_tx_env(
    tx: &alloy_consensus::TxEip7702,
    caller: alloy_primitives::Address,
) -> TxEnv {
    use alloy_consensus::crypto::secp256k1;
    use alloy_eips::{
        eip7702::{RecoveredAuthority, RecoveredAuthorization},
        Typed2718,
    };
    use alloy_primitives::TxKind;
    use rayon::prelude::*;
    use revm::context_interface::either::Either;

    // Match Alloy's serial conversion, including the signature backend and invalid entries.
    // Indexed parallel collection preserves the order consumed by EIP-7702 nonce validation.
    let authorization_list = tx
        .authorization_list
        .par_iter()
        .map(|auth| {
            Either::Right(RecoveredAuthorization::new_unchecked(
                auth.inner().clone(),
                auth.signature()
                    .ok()
                    .and_then(|signature| {
                        secp256k1::recover_signer(&signature, auth.signature_hash()).ok()
                    })
                    .map_or(RecoveredAuthority::Invalid, RecoveredAuthority::Valid),
            ))
        })
        .collect();

    TxEnv {
        tx_type: tx.ty(),
        caller,
        gas_limit: tx.gas_limit,
        gas_price: tx.max_fee_per_gas,
        kind: TxKind::Call(tx.to),
        value: tx.value,
        data: tx.input.clone(),
        nonce: tx.nonce,
        chain_id: Some(tx.chain_id),
        gas_priority_fee: Some(tx.max_priority_fee_per_gas),
        access_list: tx.access_list.clone(),
        authorization_list,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip7702};
    use alloy_eips::eip7702::{Authorization, SignedAuthorization};
    use alloy_primitives::{Address, Bytes, Signature, U256};
    use reth_evm::{ConfigureEvm, Evm};
    use revm::{
        context::{BlockEnv, CfgEnv},
        database::{CacheDB, EmptyDB},
        primitives::hardfork::SpecId,
        state::AccountInfo,
    };

    fn transaction(count: usize) -> TransactionSigned {
        let authorization_list = (0..count)
            .map(|index| {
                let auth = Authorization {
                    chain_id: U256::from(1),
                    address: Address::repeat_byte(9),
                    // Repeated entries must reach the EVM in the same order, including invalid
                    // ones.
                    nonce: (index % 2) as u64,
                };
                if index % 3 == 1 {
                    SignedAuthorization::new_unchecked(auth, 2, U256::ZERO, U256::ZERO)
                } else {
                    auth.into_signed(Signature::test_signature())
                }
            })
            .collect();
        TxEip7702 {
            chain_id: 1,
            nonce: 0,
            gas_limit: 3_000_000,
            max_fee_per_gas: 2,
            max_priority_fee_per_gas: 1,
            to: Address::repeat_byte(3),
            value: U256::from(7),
            access_list: Default::default(),
            authorization_list,
            input: Bytes::from_static(&[0, 1, 2]),
        }
        .into_signed(Signature::test_signature())
        .into()
    }

    #[test]
    fn parallel_authorizations_match_serial_conversion_at_threshold() {
        let caller = Address::repeat_byte(1);
        for count in [0, 1, 15, 16, 17, 64] {
            let tx = transaction(count);
            let expected = TxEnv::from_recovered_tx(&tx, caller);
            let actual = crate::EthEvmConfig::mainnet()
                .tx_env(ParallelTxEnv::new(Recovered::new_unchecked(&tx, caller)));
            assert_eq!(actual.authorization_list.len(), count);
            assert_eq!(actual, expected, "authorization count {count}");
        }
    }

    #[test]
    fn parallel_authorizations_preserve_execution_and_order() {
        let caller = Address::repeat_byte(1);
        let tx = transaction(64);
        let serial = TxEnv::from_recovered_tx(&tx, caller);
        let parallel = ParallelTxEnv::new(Recovered::new_unchecked(&tx, caller)).to_tx_env();
        let execute = |tx_env| {
            let mut db = CacheDB::<EmptyDB>::default();
            db.insert_account_info(
                caller,
                AccountInfo { balance: U256::from(10u128.pow(20)), ..Default::default() },
            );
            let env = alloy_evm::EvmEnv {
                cfg_env: CfgEnv::new_with_spec(SpecId::PRAGUE).with_chain_id(1),
                block_env: BlockEnv { gas_limit: 30_000_000, ..Default::default() },
            };
            crate::EthEvmConfig::mainnet().evm_with_env(db, env).transact(tx_env).unwrap()
        };
        assert_eq!(execute(parallel), execute(serial));
    }

    #[derive(Debug, Clone)]
    struct HookedConfig {
        inner: crate::EthEvmConfig,
        calls: alloc::sync::Arc<core::sync::atomic::AtomicUsize>,
    }

    impl ConfigureEvm for HookedConfig {
        type Primitives = <crate::EthEvmConfig as ConfigureEvm>::Primitives;
        type Error = <crate::EthEvmConfig as ConfigureEvm>::Error;
        type NextBlockEnvCtx = <crate::EthEvmConfig as ConfigureEvm>::NextBlockEnvCtx;
        type BlockExecutorFactory = <crate::EthEvmConfig as ConfigureEvm>::BlockExecutorFactory;
        type BlockAssembler = <crate::EthEvmConfig as ConfigureEvm>::BlockAssembler;

        fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
            self.inner.block_executor_factory()
        }
        fn block_assembler(&self) -> &Self::BlockAssembler {
            self.inner.block_assembler()
        }
        fn evm_env(
            &self,
            header: &alloy_consensus::Header,
        ) -> Result<reth_evm::EvmEnvFor<Self>, Self::Error> {
            self.inner.evm_env(header)
        }
        fn next_evm_env(
            &self,
            parent: &alloy_consensus::Header,
            attrs: &Self::NextBlockEnvCtx,
        ) -> Result<reth_evm::EvmEnvFor<Self>, Self::Error> {
            self.inner.next_evm_env(parent, attrs)
        }
        fn context_for_block<'a>(
            &self,
            block: &'a reth_primitives_traits::SealedBlock<reth_ethereum_primitives::Block>,
        ) -> Result<reth_evm::ExecutionCtxFor<'a, Self>, Self::Error> {
            self.inner.context_for_block(block)
        }
        fn context_for_next_block(
            &self,
            parent: &reth_primitives_traits::SealedHeader,
            attrs: Self::NextBlockEnvCtx,
        ) -> Result<reth_evm::ExecutionCtxFor<'_, Self>, Self::Error> {
            self.inner.context_for_next_block(parent, attrs)
        }
        fn tx_env(&self, transaction: impl alloy_evm::IntoTxEnv<TxEnv>) -> TxEnv {
            self.calls.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
            let mut env = transaction.into_tx_env();
            env.gas_price += 10;
            env
        }
        #[cfg(feature = "std")]
        fn parallel_executor<'a, DB: reth_evm::ParallelDatabase + 'a>(
            &self,
            db: DB,
        ) -> alloc::boxed::Box<
            dyn reth_evm::parallel_execute::ParallelExecutor<
                    Primitives = Self::Primitives,
                    Error = reth_evm::execute::BlockExecutionError,
                > + 'a,
        > {
            alloc::boxed::Box::new(crate::parallel_execute::GrevmExecutor::new(
                self.inner.chain_spec().clone(),
                self,
                db,
            ))
        }
    }

    #[test]
    fn parallel_conversion_preserves_custom_tx_env_hook() {
        let config =
            HookedConfig { inner: crate::EthEvmConfig::mainnet(), calls: Default::default() };
        let tx = transaction(16);
        let caller = Address::repeat_byte(1);
        let actual = config.tx_env(ParallelTxEnv::new(Recovered::new_unchecked(&tx, caller)));
        let mut expected = TxEnv::from_recovered_tx(&tx, caller);
        expected.gas_price += 10;
        assert_eq!(actual, expected);
        assert_eq!(config.calls.load(core::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn parallel_adapter_preserves_other_transaction_types() {
        let tx: TransactionSigned =
            alloy_consensus::TxLegacy::default().into_signed(Signature::test_signature()).into();
        let caller = Address::repeat_byte(1);
        assert_eq!(
            ParallelTxEnv::new(Recovered::new_unchecked(&tx, caller)).to_tx_env(),
            TxEnv::from_recovered_tx(&tx, caller),
        );
    }
}
