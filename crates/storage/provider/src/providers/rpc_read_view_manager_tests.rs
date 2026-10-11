use super::*;
use std::{sync::mpsc, thread, time::Duration};

#[derive(Debug)]
struct TestView;

impl DatabaseReadView for TestView {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn view(number: u64) -> PublishedRpcView {
    PublishedRpcView {
        bounds: RpcReadViewBounds {
            block_number: number,
            block_hash: B256::repeat_byte(number as u8),
            next_tx_num: number * 2,
        },
        storage: Arc::new(TestView),
    }
}

#[test]
fn publication_replaces_bounds_without_waiting_for_readers() {
    let manager = Arc::new(RpcViewManager::new(0));
    assert!(manager.lease().is_err());
    manager.publish(view(1), true);
    let lease = manager.lease().unwrap();
    let first = manager.view(&lease).unwrap();
    manager.publish(view(2), true);
    assert_eq!(first.bounds.block_number, 1);
    assert_eq!(manager.view(&lease).unwrap().bounds.block_number, 2);
    assert!(manager.maintenance(false).is_none());
    drop(lease);
    assert!(manager.maintenance(false).is_some());
}

#[test]
fn maintenance_waits_for_readers_and_closes_admission() {
    let manager = Arc::new(RpcViewManager::new(0));
    manager.publish(view(1), true);
    let lease = manager.lease().unwrap();
    let (send, recv) = mpsc::channel();
    let other = manager.clone();
    let worker = thread::spawn(move || {
        let mut guard = other.maintenance(true).unwrap();
        send.send(()).unwrap();
        other.publish(view(1), true);
        guard.complete();
    });
    {
        let mut state = manager.state.lock();
        while !state.maintenance {
            assert!(!manager.changed.wait_for(&mut state, Duration::from_secs(5)).timed_out());
        }
    }
    assert!(manager.lease().is_err());
    assert!(manager.maintenance(false).is_none());
    assert!(matches!(recv.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(manager.view(&lease).unwrap().bounds.block_number, 1);
    drop(lease);
    recv.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    assert!(manager.lease().is_ok());
}

#[test]
fn interruption_requires_recovery_and_verified_publication() {
    let manager = Arc::new(RpcViewManager::new(0));
    manager.publish(view(1), true);
    let lease = manager.lease().unwrap();
    let old = manager.view(&lease).unwrap();
    manager.interrupt(3);
    assert!(manager.view(&lease).is_err());
    assert_eq!(old.bounds.block_number, 1);
    drop(lease);
    manager.publish(view(3), true);
    assert!(manager.lease().is_err());
    let mut guard = manager.maintenance(true).unwrap();
    guard.complete();
    drop(guard);
    assert!(manager.lease().is_err());
    let mut guard = manager.maintenance(true).unwrap();
    manager.publish(view(3), true);
    guard.complete();
    // Admission remains closed until the entire maintenance range ends.
    assert!(manager.lease().is_err());
    drop(guard);
    assert!(manager.lease().is_ok());
}

#[test]
fn retry_cannot_complete_maintenance_using_a_publication_before_interruption() {
    let manager = Arc::new(RpcViewManager::new(0));
    manager.publish(view(1), true);
    let mut guard = manager.maintenance(true).unwrap();
    manager.publish(view(2), true);
    manager.interrupt(3);
    manager.publication_failed();
    guard.complete();
    drop(guard);
    assert!(manager.lease().is_err());
    let mut guard = manager.maintenance(true).unwrap();
    manager.publish(view(3), true);
    guard.complete();
    drop(guard);
    assert!(manager.lease().is_ok());
}

#[test]
fn publication_failure_closes_reads_without_requiring_storage_recovery() {
    let manager = Arc::new(RpcViewManager::new(0));
    manager.publish(view(1), true);
    let mut guard = manager.maintenance(true).unwrap();
    manager.publication_failed();
    guard.complete();
    drop(guard);
    assert!(manager.lease().is_err());
    assert!(manager.requires_validation(2));
    manager.publish(view(2), false);
    assert!(manager.lease().is_err());
    manager.publish(view(2), true);
    assert!(manager.lease().is_ok());
    assert!(!manager.requires_validation(2));
}

#[test]
fn startup_floor_is_cleared_only_by_verified_view_and_foreign_leases_are_rejected() {
    let manager = Arc::new(RpcViewManager::new(3));
    manager.publish(view(2), true);
    assert!(manager.lease().is_err());
    manager.publish(view(3), false);
    assert!(manager.lease().is_err());
    manager.publish(view(3), true);
    assert!(manager.lease().is_ok());
    let other = Arc::new(RpcViewManager::new(0));
    other.publish(view(1), true);
    assert!(manager.view(&other.lease().unwrap()).is_err());
    // A later intentional unwind can publish a lower height.
    let mut guard = manager.maintenance(true).unwrap();
    manager.publish(view(1), true);
    guard.complete();
    drop(guard);
    assert_eq!(manager.view(&manager.lease().unwrap()).unwrap().bounds.block_number, 1);
}
