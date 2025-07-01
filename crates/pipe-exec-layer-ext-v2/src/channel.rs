use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    sync::{Mutex, MutexGuard},
};

use tokio::sync::oneshot;

#[derive(Debug)]
pub(crate) struct Channel<K, V> {
    inner: Mutex<Inner<K, V>>,
}

#[derive(Debug)]
enum State<V> {
    Waiting(oneshot::Sender<V>),
    Notified(V),
}

#[derive(Debug)]
struct Inner<K, V> {
    states: HashMap<K, State<V>>,
    closed: bool,
}

impl<K: Eq + Clone + Debug + Hash, V> Channel<K, V> {
    pub(crate) fn new() -> Self {
        Self { inner: Mutex::new(Inner { states: HashMap::new(), closed: false }) }
    }

    pub(crate) fn new_with_states<I: IntoIterator<Item = (K, V)>>(states: I) -> Self {
        let mut inner = Inner { states: HashMap::new(), closed: false };
        for (k, v) in states {
            inner.states.insert(k, State::Notified(v));
        }
        Self { inner: Mutex::new(inner) }
    }

    /// Wait until the key is notified.
    /// Returns `None` if the barrier has been closed.
    pub(crate) async fn wait(&self, key: K) -> Option<V> {
        // ATTN: We can guarantee that `.await` will not occur within the critical zone, which means
        // `MutexGuard` will not be sent across threads.
        struct SendMutexGuard<'a, T>(MutexGuard<'a, T>);
        unsafe impl<'a, T> Send for SendMutexGuard<'a, T> {}

        let mut inner = SendMutexGuard(self.inner.lock().unwrap());
        if inner.0.closed {
            return None;
        }

        let state = inner.0.states.remove(&key);
        match state {
            Some(State::Notified(v)) => Some(v),
            Some(State::Waiting(_)) => {
                panic!("unexpected state: {key:?}");
            }
            None => {
                let (tx, rx) = oneshot::channel();
                inner.0.states.insert(key, State::Waiting(tx));
                drop(inner);

                rx.await.ok()
            }
        }
    }

    /// Notify the key with the value.
    /// Returns `None` if the barrier has been closed.
    pub(crate) fn notify(&self, key: K, val: V) -> Option<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return None;
        }

        let state = inner.states.remove(&key);
        match state {
            Some(State::Waiting(tx)) => {
                let _ = tx.send(val);
            }
            Some(State::Notified(_)) => {
                panic!("unexpected state: {key:?}");
            }
            None => {
                inner.states.insert(key, State::Notified(val));
            }
        }
        Some(())
    }

    pub(crate) fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.closed = true;
        inner.states.clear();
    }
}

#[cfg(test)]
mod test {
    use rand::{rng, Rng};
    use std::sync::Arc;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_pipe_barrier() {
        let barrier = Arc::new(super::Channel::new_with_states([(0, 0)]));

        let mut tasks = JoinSet::new();
        for i in 1..10 {
            let barrier = barrier.clone();
            let sleep_ms = rng().random_range(100..1000);
            tasks.spawn(async move {
                let v = barrier.wait(i - 1).await.unwrap();
                assert_eq!(v, i - 1);
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                barrier.notify(i, i).unwrap();
            });
        }

        tasks.join_all().await;
    }
}
