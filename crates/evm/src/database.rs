use alloy_primitives::{Address, B256, U256};
use revm_primitives::{AccountInfo, Bytecode};

pub trait Database: revm::Database<Error: core::error::Error + Send + Sync + 'static> {}
impl<T> Database for T where T: revm::Database<Error: core::error::Error + Send + Sync + 'static> {}

pub trait ParallelDatabase:
    revm::DatabaseRef<Error: core::error::Error + Send + Sync + 'static + Clone> + Send + Sync
{
}
impl<T> ParallelDatabase for T where
    T: revm::DatabaseRef<Error: core::error::Error + Send + Sync + 'static + Clone> + Send + Sync
{
}

pub enum DatabaseEnum<DB: Database, PDB: ParallelDatabase> {
    Serial(DB),
    Parallel(PDB),
}
pub struct NoopDatabase;

impl revm::Database for NoopDatabase {
    type Error = core::convert::Infallible;

    fn basic(&mut self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        unreachable!()
    }

    fn code_by_hash(&mut self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        unreachable!()
    }

    fn storage(&mut self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        unreachable!()
    }

    fn block_hash(&mut self, _number: u64) -> Result<B256, Self::Error> {
        unreachable!()
    }
}

impl revm::DatabaseRef for NoopDatabase {
    type Error = core::convert::Infallible;

    fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        unreachable!()
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        unreachable!()
    }

    fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        unreachable!()
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        unreachable!()
    }
}
