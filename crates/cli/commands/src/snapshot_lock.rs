use eyre::{Result, WrapErr};
use reth_fs_util as fs;
use reth_node_core::dirs::{ChainPath, DataDirPath};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
};

const LOCK_FILE: &str = ".reth-snapshot.lock";

/// Coordinates node startup and snapshot installation before either opens or mutates storage.
pub(crate) struct SnapshotLock {
    _files: Vec<File>,
}

impl SnapshotLock {
    pub(crate) fn acquire(
        datadir: &ChainPath<DataDirPath>,
        shards: Option<&str>,
        replaces_storage: bool,
    ) -> Result<Self> {
        let data = datadir.data_dir();
        let storage_roots = [datadir.db(), data.join("rocksdb"), datadir.static_files()];
        let roots = std::iter::once(data.to_owned())
            .chain(storage_roots.iter().cloned())
            .chain(["state", "account_trie", "storage_trie"].map(|name| datadir.db().join(name)))
            .chain(configured_shard_paths(shards));
        let mut paths = BTreeSet::new();
        for root in roots {
            fs::create_dir_all(&root)?;
            paths.insert(root.canonicalize()?.join(LOCK_FILE));
        }

        if replaces_storage {
            // Clearing the whole datadir through a static-file override would also
            // remove unrelated node data such as peer keys and download state.
            let data = data.canonicalize()?;
            for root in &storage_roots {
                let root = root.canonicalize()?;
                eyre::ensure!(
                    !data.starts_with(&root),
                    "snapshot cleanup would overlap the node data directory: {}",
                    root.display()
                );
            }
        }

        let mut files = Vec::with_capacity(paths.len());
        for path in paths {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .wrap_err_with(|| format!("opening snapshot lock {}", path.display()))?;
            match file.try_lock() {
                Ok(()) => files.push(file),
                Err(TryLockError::WouldBlock) => {
                    eyre::bail!("node or snapshot installation already holds {}", path.display())
                }
                Err(TryLockError::Error(err)) => {
                    return Err(err).wrap_err_with(|| format!("locking {}", path.display()))
                }
            }
        }
        Ok(Self { _files: files })
    }
}

/// The installer reserves its lock file in both tar entries and manifest outputs.
pub(crate) fn is_lock_path(path: &Path) -> bool {
    path.components().any(|part| part.as_os_str() == LOCK_FILE)
}

// Mirror the CLI's two-root and three-shard layouts so an explicitly shared shard
// is also protected when two invocations use different base datadirs.
pub(crate) fn configured_shard_paths(shards: Option<&str>) -> Vec<PathBuf> {
    let paths = shards
        .into_iter()
        .flat_map(|value| value.split(';'))
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    if let [first, second] = paths.as_slice() {
        vec![first.join("state"), first.join("account_trie"), second.join("storage_trie")]
    } else {
        paths
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::Chain;
    use reth_node_core::args::DatadirArgs;
    use std::process::Command;

    fn datadir(root: PathBuf, static_files: PathBuf) -> ChainPath<DataDirPath> {
        DatadirArgs {
            datadir: root.into(),
            static_files_path: Some(static_files),
            ..Default::default()
        }
        .resolve_datadir(Chain::mainnet())
    }

    #[test]
    fn shared_external_static_root_locks_across_processes() {
        const TEST_ROOT: &str = "RETH_SNAPSHOT_LOCK_TEST_ROOT";
        if let Some(root) = std::env::var_os(TEST_ROOT) {
            let root = PathBuf::from(root);
            let data = datadir(root.join("other-node"), root.join("static"));
            let blocked = std::env::var_os("RETH_SNAPSHOT_LOCK_TEST_BLOCKED").is_some();
            assert_eq!(SnapshotLock::acquire(&data, None, true).is_err(), blocked);
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let data = datadir(dir.path().join("node"), dir.path().join("static"));
        let lock = SnapshotLock::acquire(&data, None, false).unwrap();
        let run_child = |blocked| {
            let test =
                concat!(module_path!(), "::shared_external_static_root_locks_across_processes");
            let mut child = Command::new(std::env::current_exe().unwrap());
            child.args(["--exact", test.split_once("::").unwrap().1]).env(TEST_ROOT, dir.path());
            if blocked {
                child.env("RETH_SNAPSHOT_LOCK_TEST_BLOCKED", "1");
            }
            let result = child.output().unwrap();
            assert!(
                result.status.success() &&
                    String::from_utf8_lossy(&result.stdout).contains("1 passed"),
                "{}",
                String::from_utf8_lossy(&result.stdout)
            );
        };
        run_child(true);
        drop(lock);
        run_child(false);
    }

    #[test]
    fn destructive_install_refuses_a_static_root_that_contains_the_locks() {
        let dir = tempfile::tempdir().unwrap();
        let data = datadir(dir.path().join("node"), dir.path().join("node"));
        assert!(SnapshotLock::acquire(&data, None, true).is_err());
        assert!(SnapshotLock::acquire(&data, None, false).is_ok());
    }
}
