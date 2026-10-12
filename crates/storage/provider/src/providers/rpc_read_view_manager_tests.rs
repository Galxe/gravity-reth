use super::*;

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
fn recovery_requires_a_verified_publication_after_the_last_interruption() {
    let manager = Arc::new(RpcViewManager::new(0));
    assert!(manager.lease().is_err());
    manager.publish(view(1), true);
    assert!(manager.lease().is_ok());

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
    assert!(manager.lease().is_err());
    drop(guard);
    assert!(manager.lease().is_ok());

    manager.publication_failed();
    assert!(manager.lease().is_err());
    assert!(manager.requires_validation(3));
    manager.publish(view(3), false);
    assert!(manager.lease().is_err());
    manager.publish(view(3), true);
    assert!(manager.lease().is_ok());
    assert!(!manager.requires_validation(3));
}
