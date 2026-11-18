//! Removes snapshot-managed data that the selected download plan does not list.

use super::{
    managed_datadir_path, planning::PlannedArchive, verify::OutputVerifier, MANAGED_DATADIR_PATHS,
};
use eyre::{Result, WrapErr};
use reth_db::lockfile::StorageLock;
use reth_fs_util as fs;
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use tracing::info;

/// Removes every file under the paths that `--force` clears (`db`, `rocksdb`, `static_files`,
/// and `reth.toml`) that no planned archive lists as an output.
///
/// Listed files are kept so the startup integrity check can reuse the ones that still verify.
/// Once the download completes, those paths hold exactly the snapshot's files, with no leftovers
/// such as blocks or history indices from a chain the node extended past the snapshot block.
///
/// This relies on each archive's `output_files` listing every file the archive extracts. A file
/// an archive extracts without listing it is removed here and not restored, because the archive
/// is reused once its listed outputs verify.
///
/// A managed path that is itself a symlink is followed, because operators link whole data
/// directories to other disks. Symlinks below it are removed rather than followed, so pruning
/// never deletes files outside the data dir. Nested directories left empty are removed; the
/// managed directories themselves are kept, since they may be mount points.
///
/// Fails if a running node holds the database lock, since pruning would otherwise delete its
/// lock and files from under it.
pub(crate) fn prune_unlisted_outputs(
    planned: &[PlannedArchive],
    target_dir: &Path,
    static_files_dir: Option<&Path>,
) -> Result<()> {
    let db_dir = target_dir.join("db");
    if db_dir.is_dir() {
        drop(StorageLock::try_acquire(&db_dir).wrap_err("refusing to prune a data dir in use")?);
    }

    let verifier = OutputVerifier::new(target_dir, static_files_dir);
    let listed = planned
        .iter()
        .flat_map(|planned| &planned.archive.output_files)
        .map(|output| verifier.output_path(output))
        .collect::<Result<HashSet<_>>>()?;

    // Gravity stores separate RocksDB shards below the managed roots. Check every
    // native lock before deleting any output, without following nested symlinks.
    for entry in MANAGED_DATADIR_PATHS {
        let root = managed_datadir_path(entry, target_dir, static_files_dir);
        if root.is_dir() {
            ensure_rocksdb_tree_not_in_use(&root)?;
        }
    }

    let mut removed = 0;
    for entry in MANAGED_DATADIR_PATHS {
        let root = managed_datadir_path(entry, target_dir, static_files_dir);
        if !root.try_exists()? {
            continue
        }

        if fs::metadata(&root)?.is_dir() {
            removed += prune_dir(&root, &listed)?;
        } else if !listed.contains(&root) {
            fs::remove_file(&root)?;
            removed += 1;
        }
    }

    info!(target: "reth::cli", removed, "Removed existing files not listed in the snapshot plan");
    Ok(())
}

/// Checks native shard locks below one managed directory, without following nested symlinks.
fn ensure_rocksdb_tree_not_in_use(dir: &Path) -> Result<()> {
    ensure_rocksdb_not_in_use(dir)?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            ensure_rocksdb_tree_not_in_use(&entry.path())?;
        }
    }
    Ok(())
}

/// Checks RocksDB's native LOCK as well as the separate Reth process lock.
/// This runs before the download command opens its database.
fn ensure_rocksdb_not_in_use(db_dir: &Path) -> Result<()> {
    let path = db_dir.join("LOCK");
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).wrap_err("checking RocksDB lock"),
    };
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: flock is a C record containing only integer fields; all-zero is valid.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        // RocksDB uses F_SETLK over the whole LOCK file. F_GETLK only queries an external
        // holder: acquiring and dropping an independent lock here could disturb a live database.
        // SAFETY: the descriptor stays open and lock points to a valid writable flock record.
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut lock) };
        if result == -1 {
            return Err(std::io::Error::last_os_error()).wrap_err("checking RocksDB lock")
        }
        #[allow(
            clippy::unnecessary_cast,
            reason = "libc lock constants have different integer types on Unix targets"
        )]
        let unlocked = libc::F_UNLCK as libc::c_short;
        eyre::ensure!(
            lock.l_type == unlocked,
            "refusing to prune a RocksDB data dir in use by process {}",
            lock.l_pid
        );
    }
    #[cfg(not(unix))]
    {
        drop(file);
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        file.try_lock().wrap_err("refusing to prune a RocksDB data dir in use")?;
    }
    Ok(())
}

/// Removes unlisted entries below `dir` without following symlinks, and returns how many files
/// or links it removed. Removing a symlink leaves its target intact.
fn prune_dir(dir: &Path, listed: &HashSet<PathBuf>) -> Result<usize> {
    let mut removed = 0;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        // Unlike `fs::metadata`, this does not follow symlinks.
        let file_type = entry
            .file_type()
            .wrap_err_with(|| format!("failed to read file type of {}", path.display()))?;
        if file_type.is_dir() {
            removed += prune_dir(&path, listed)?;
            remove_dir_if_empty(&path)?;
            continue
        }
        if listed.contains(&path) {
            continue
        }

        // Windows removes a symlink to a directory with `remove_dir`, not `remove_file`.
        #[cfg(windows)]
        if std::os::windows::fs::FileTypeExt::is_symlink_dir(&file_type) {
            std::fs::remove_dir(&path)
                .wrap_err_with(|| format!("failed to remove {}", path.display()))?;
            removed += 1;
            continue
        }
        fs::remove_file(&path)?;
        removed += 1;
    }
    Ok(removed)
}

/// Removes `dir` if pruning left it empty.
fn remove_dir_if_empty(dir: &Path) -> Result<()> {
    if fs::read_dir(dir)?.next().is_none() {
        std::fs::remove_dir(dir)
            .wrap_err_with(|| format!("failed to remove empty directory {}", dir.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::manifest::{OutputFileChecksum, SnapshotArchive, SnapshotComponentType};
    use tempfile::tempdir;

    fn planned_with_outputs(paths: &[&str]) -> Vec<PlannedArchive> {
        vec![PlannedArchive {
            ty: SnapshotComponentType::State,
            component: "State".to_string(),
            archive: SnapshotArchive {
                url: "https://example.com/state.tar.zst".to_string(),
                file_name: "state.tar.zst".to_string(),
                size: 1,
                blake3: None,
                output_files: paths
                    .iter()
                    .map(|path| OutputFileChecksum {
                        path: (*path).to_string(),
                        size: 1,
                        blake3: String::new(),
                    })
                    .collect(),
            },
        }]
    }

    fn write_file(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"data").unwrap();
    }

    #[test]
    fn prune_removes_unlisted_files_and_keeps_listed_ones() {
        let dir = tempdir().unwrap();
        let target = dir.path();
        let listed = ["db/snapshot_marker.dat", "static_files/static_file_headers_0_499999"];
        let leftovers = [
            "db/stale.dat",
            "rocksdb/000042.sst",
            "static_files/static_file_headers_500000_999999",
            "reth.toml",
        ];
        for path in listed.iter().chain(&leftovers).chain(&["known-peers.json"]) {
            write_file(&target.join(path));
        }

        prune_unlisted_outputs(&planned_with_outputs(&listed), target, None).unwrap();

        assert!(listed.iter().all(|path| target.join(path).exists()));
        assert!(leftovers.iter().all(|path| !target.join(path).exists()));
        assert!(target.join("known-peers.json").exists());
    }

    #[test]
    fn prune_removes_nested_directories_it_empties_and_keeps_managed_ones() {
        let dir = tempdir().unwrap();
        write_file(&dir.path().join("rocksdb/nested/000042.sst"));

        prune_unlisted_outputs(
            &planned_with_outputs(&["db/snapshot_marker.dat"]),
            dir.path(),
            None,
        )
        .unwrap();

        assert!(!dir.path().join("rocksdb/nested").exists());
        assert!(dir.path().join("rocksdb").is_dir());
    }

    #[test]
    fn prune_resolves_static_files_in_a_custom_directory() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("datadir");
        let static_files = dir.path().join("custom-static-files");
        write_file(&static_files.join("static_file_headers_0_499999"));
        write_file(&static_files.join("static_file_headers_500000_999999"));

        prune_unlisted_outputs(
            &planned_with_outputs(&["static_files/static_file_headers_0_499999"]),
            &target,
            Some(&static_files),
        )
        .unwrap();

        assert!(static_files.join("static_file_headers_0_499999").exists());
        assert!(!static_files.join("static_file_headers_500000_999999").exists());
    }

    #[test]
    fn prune_skips_missing_managed_paths() {
        let dir = tempdir().unwrap();

        prune_unlisted_outputs(
            &planned_with_outputs(&["db/snapshot_marker.dat"]),
            dir.path(),
            None,
        )
        .unwrap();

        assert!(!dir.path().join("db").exists());
    }

    #[cfg(unix)]
    #[test]
    fn prune_removes_nested_symlinks_without_following_them() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("datadir");
        let outside = dir.path().join("outside");
        write_file(&outside.join("unrelated.dat"));
        write_file(&target.join("static_files/static_file_headers_0_499999"));
        std::os::unix::fs::symlink(&outside, target.join("static_files/linked")).unwrap();

        prune_unlisted_outputs(
            &planned_with_outputs(&["static_files/static_file_headers_0_499999"]),
            &target,
            None,
        )
        .unwrap();

        assert!(std::fs::symlink_metadata(target.join("static_files/linked")).is_err());
        assert!(outside.join("unrelated.dat").exists());
    }

    #[cfg(unix)]
    #[test]
    fn prune_follows_a_symlinked_managed_directory() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("datadir");
        let other_disk = dir.path().join("other-disk");
        write_file(&other_disk.join("static_file_headers_0_499999"));
        write_file(&other_disk.join("static_file_headers_500000_999999"));
        std::fs::create_dir_all(&target).unwrap();
        std::os::unix::fs::symlink(&other_disk, target.join("static_files")).unwrap();

        prune_unlisted_outputs(
            &planned_with_outputs(&["static_files/static_file_headers_0_499999"]),
            &target,
            None,
        )
        .unwrap();

        assert!(std::fs::symlink_metadata(target.join("static_files")).unwrap().is_symlink());
        assert!(other_disk.join("static_file_headers_0_499999").exists());
        assert!(!other_disk.join("static_file_headers_500000_999999").exists());
    }
    #[cfg(unix)]
    #[test]
    fn prune_refuses_live_rocksdb_process() {
        use std::{
            process::{Command, Stdio},
            time::{Duration, Instant},
        };
        const DB_ENV: &str = "RETH_PRUNE_TEST_DB";
        if let Some(path) = std::env::var_os(DB_ENV) {
            let path = PathBuf::from(path);
            let db = reth_db::init_db(&path, reth_db::DatabaseArguments::default()).unwrap();
            let parent = path.parent().unwrap();
            std::fs::write(parent.join("ready"), b"ready").unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while parent.join("release").exists() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            drop(db);
            return
        }
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                if self.0.try_wait().ok().flatten().is_none() {
                    let _ = self.0.kill();
                    let _ = self.0.wait();
                }
            }
        }
        let dir = tempdir().unwrap();
        let db_dir = dir.path().join("db");
        write_file(&db_dir.join("stale.dat"));
        std::fs::write(dir.path().join("release"), b"wait").unwrap();
        let full_name = concat!(module_path!(), "::prune_refuses_live_rocksdb_process");
        let mut child = Child(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", full_name.split_once("::").unwrap().1, "--nocapture"])
                .env(DB_ENV, &db_dir)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.path().join("ready").exists() && Instant::now() < deadline {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "database fixture exited before locking"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(dir.path().join("ready").exists(), "database fixture did not become ready");
        let error = prune_unlisted_outputs(&[], dir.path(), None).unwrap_err();
        assert!(error.to_string().contains("RocksDB data dir in use"), "{error:#}");
        assert!(db_dir.join("stale.dat").exists());
        assert!(db_dir.join("state/CURRENT").exists());
        std::fs::remove_file(dir.path().join("release")).unwrap();
        assert!(child.0.wait().unwrap().success());
    }
}
