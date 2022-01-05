// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! The concurrency manager is responsible for concurrency control of
//! transactions.
//!
//! The concurrency manager contains a lock table in memory. Lock information
//! can be stored in it and reading requests can check if these locks block
//! the read.
//!
//! In order to mutate the lock of a key stored in the lock table, it needs
//! to be locked first using `lock_key` or `lock_keys`.

#[macro_use]
extern crate tikv_util;

mod key_handle;
mod lock_table;

pub use self::key_handle::{KeyHandle, KeyHandleGuard};
pub use self::lock_table::LockTable;
use std::sync::RwLock;
use tikv_util::HandyRwLock;

use std::collections::HashMap;
use std::{
    mem::{self, MaybeUninit},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use txn_types::{Key, Lock, TimeStamp};

#[derive(Clone)]
pub struct RegionRawLockGuard {
    region_id: u64,
    cm: Arc<ConcurrencyManager>,
}

impl Drop for RegionRawLockGuard {
    fn drop(&mut self) {
        self.cm.region_raw_unlock(self.region_id);
    }
}

// Pay attention that the async functions of ConcurrencyManager should not hold
// the mutex.
#[derive(Clone)]
pub struct ConcurrencyManager {
    max_ts: Arc<AtomicU64>,
    lock_table: LockTable,

    regions_raw_lock: Arc<RwLock<HashMap<u64, AtomicU64>>>,
}

impl ConcurrencyManager {
    pub fn new(latest_ts: TimeStamp) -> Self {
        ConcurrencyManager {
            max_ts: Arc::new(AtomicU64::new(latest_ts.into_inner())),
            lock_table: LockTable::default(),
            regions_raw_lock: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn max_ts(&self) -> TimeStamp {
        TimeStamp::new(self.max_ts.load(Ordering::SeqCst))
    }

    /// Updates max_ts with the given new_ts. It has no effect if
    /// max_ts >= new_ts or new_ts is TimeStamp::max().
    pub fn update_max_ts(&self, new_ts: TimeStamp) {
        if new_ts != TimeStamp::max() {
            self.max_ts.fetch_max(new_ts.into_inner(), Ordering::SeqCst);
        }
    }

    /// Acquires a mutex of the key and returns an RAII guard. When the guard goes
    /// out of scope, the mutex will be unlocked.
    ///
    /// The guard can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_key(&self, key: &Key) -> KeyHandleGuard {
        self.lock_table.lock_key(key).await
    }

    /// Acquires mutexes of the keys and returns the RAII guards. The order of the
    /// guards is the same with the given keys.
    ///
    /// The guards can be used to store Lock in the table. The stored lock
    /// is visible to `read_key_check` and `read_range_check`.
    pub async fn lock_keys(&self, keys: impl Iterator<Item = &Key>) -> Vec<KeyHandleGuard> {
        let mut keys_with_index: Vec<_> = keys.enumerate().collect();
        // To prevent deadlock, we sort the keys and lock them one by one.
        keys_with_index.sort_by_key(|(_, key)| *key);
        let mut result: Vec<MaybeUninit<KeyHandleGuard>> = Vec::new();
        result.resize_with(keys_with_index.len(), MaybeUninit::uninit);
        for (index, key) in keys_with_index {
            result[index] = MaybeUninit::new(self.lock_table.lock_key(key).await);
        }
        #[allow(clippy::unsound_collection_transmute)]
        unsafe {
            mem::transmute(result)
        }
    }

    /// Checks if there is a memory lock of the key which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    pub fn read_key_check<E>(
        &self,
        key: &Key,
        check_fn: impl FnOnce(&Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        self.lock_table.check_key(key, check_fn)
    }

    /// Checks if there is a memory lock in the range which blocks the read.
    /// The given `check_fn` should return false iff the lock passed in
    /// blocks the read.
    pub fn read_range_check<E>(
        &self,
        start_key: Option<&Key>,
        end_key: Option<&Key>,
        check_fn: impl FnMut(&Key, &Lock) -> Result<(), E>,
    ) -> Result<(), E> {
        self.lock_table.check_range(start_key, end_key, check_fn)
    }

    /// Find the minimum start_ts among all locks in memory.
    pub fn global_min_lock_ts(&self) -> Option<TimeStamp> {
        let mut min_lock_ts = None;
        // TODO: The iteration looks not so efficient. It's better to be optimized.
        self.lock_table.for_each(|handle| {
            if let Some(curr_ts) = handle.with_lock(|lock| lock.as_ref().map(|l| l.ts)) {
                if min_lock_ts.map(|ts| ts > curr_ts).unwrap_or(true) {
                    min_lock_ts = Some(curr_ts);
                }
            }
        });

        let min_raw_ts = self.regions_min_raw_lock_ts();
        if let (Some(lock_ts), Some(raw_ts)) = (min_lock_ts, min_raw_ts) {
            min_lock_ts = Some(std::cmp::min(raw_ts, lock_ts));
        } else {
            min_lock_ts = min_raw_ts
        }

        min_lock_ts
    }

    pub fn region_raw_lock(&self, region_id: u64, lock_ts: TimeStamp) -> TimeStamp {
        let old_value;
        let rl = self.regions_raw_lock.rl();
        if let Some(ts) = rl.get(&region_id) {
            old_value = ts.swap(lock_ts.into_inner(), Ordering::Release);
            if old_value != 0 {
                error!("ConcurrencyManager::region_raw_lock conflict"; "region" => region_id, "lock_ts" => ?lock_ts, "old_value" => old_value);
            }
        } else {
            drop(rl);
            let mut wl = self.regions_raw_lock.wl();
            let ts = wl.entry(region_id).or_insert_with(|| AtomicU64::new(0));
            old_value = ts.swap(lock_ts.into_inner(), Ordering::Release);
            if old_value != 0 {
                error!("ConcurrencyManager::region_raw_lock conflict"; "region" => region_id, "lock_ts" => ?lock_ts, "old_value" => old_value);
            }
        }
        debug!("ConcurrencyManager::region_raw_lock"; "region" => region_id, "lock_ts" => ?lock_ts, "old_value" => old_value);
        TimeStamp::from(old_value)
    }

    pub fn region_raw_lock_with_guard(
        &self,
        region_id: u64,
        lock_ts: TimeStamp,
    ) -> (TimeStamp, RegionRawLockGuard) {
        let old_value = self.region_raw_lock(region_id, lock_ts);
        (
            old_value,
            RegionRawLockGuard {
                region_id,
                cm: Arc::new(self.clone()),
            },
        )
    }

    pub fn region_raw_unlock(&self, region_id: u64) -> TimeStamp {
        let mut old_value = 0;
        if let Some(ts) = self.regions_raw_lock.rl().get(&region_id) {
            old_value = ts.swap(0, Ordering::Release);
        }
        debug!("ConcurrencyManager::region_raw_unlock"; "region" => region_id, "old_value" => old_value);
        TimeStamp::from(old_value)
    }

    pub fn regions_min_raw_lock_ts(&self) -> Option<TimeStamp> {
        let mut min_lock_ts = None;
        for (_, ts) in self.regions_raw_lock.rl().iter() {
            let curr_ts = ts.load(Ordering::Acquire);
            if curr_ts > 0 && min_lock_ts.map(|ts| ts > curr_ts).unwrap_or(true) {
                min_lock_ts = Some(curr_ts);
            }
        }
        debug!(
            "ConcurrencyManager::regions_min_raw_lock_ts: {:?}",
            min_lock_ts
        );
        min_lock_ts.map(TimeStamp::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use txn_types::LockType;

    #[tokio::test]
    async fn test_lock_keys_order() {
        let concurrency_manager = ConcurrencyManager::new(1.into());
        let keys: Vec<_> = [b"c", b"a", b"b"]
            .iter()
            .map(|k| Key::from_raw(*k))
            .collect();
        let guards = concurrency_manager.lock_keys(keys.iter()).await;
        for (key, guard) in keys.iter().zip(&guards) {
            assert_eq!(key, guard.key());
        }
    }

    #[tokio::test]
    async fn test_update_max_ts() {
        let concurrency_manager = ConcurrencyManager::new(10.into());
        concurrency_manager.update_max_ts(20.into());
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        concurrency_manager.update_max_ts(5.into());
        assert_eq!(concurrency_manager.max_ts(), 20.into());

        concurrency_manager.update_max_ts(TimeStamp::max());
        assert_eq!(concurrency_manager.max_ts(), 20.into());
    }

    fn new_lock(ts: impl Into<TimeStamp>, primary: &[u8], lock_type: LockType) -> Lock {
        let ts = ts.into();
        Lock::new(lock_type, primary.to_vec(), ts, 0, None, 0.into(), 1, ts)
    }

    #[tokio::test]
    async fn test_global_min_lock_ts() {
        let concurrency_manager = ConcurrencyManager::new(1.into());

        assert_eq!(concurrency_manager.global_min_lock_ts(), None);
        let guard = concurrency_manager.lock_key(&Key::from_raw(b"a")).await;
        assert_eq!(concurrency_manager.global_min_lock_ts(), None);
        guard.with_lock(|l| *l = Some(new_lock(10, b"a", LockType::Put)));
        assert_eq!(concurrency_manager.global_min_lock_ts(), Some(10.into()));
        drop(guard);
        assert_eq!(concurrency_manager.global_min_lock_ts(), None);

        let ts_seqs = vec![
            vec![20, 30, 40],
            vec![40, 30, 20],
            vec![20, 40, 30],
            vec![30, 20, 40],
        ];
        let keys: Vec<_> = vec![b"a", b"b", b"c"]
            .into_iter()
            .map(|k| Key::from_raw(k))
            .collect();

        for ts_seq in ts_seqs {
            let guards = concurrency_manager.lock_keys(keys.iter()).await;
            assert_eq!(concurrency_manager.global_min_lock_ts(), None);
            for (ts, guard) in ts_seq.into_iter().zip(guards.iter()) {
                guard.with_lock(|l| *l = Some(new_lock(ts, b"pk", LockType::Put)));
            }
            assert_eq!(concurrency_manager.global_min_lock_ts(), Some(20.into()));
        }
    }

    #[tokio::test]
    async fn test_raw_locks() {
        let cm = ConcurrencyManager::new(1.into());
        assert_eq!(cm.global_min_lock_ts(), None);

        assert_eq!(cm.region_raw_lock(1, 110.into()), 0.into());
        assert_eq!(cm.region_raw_lock(2, 100.into()), 0.into());
        assert_eq!(cm.global_min_lock_ts(), Some(100.into()));

        assert_eq!(cm.region_raw_unlock(2), 100.into());
        assert_eq!(cm.global_min_lock_ts(), Some(110.into()));

        assert_eq!(cm.region_raw_lock(1, 120.into()), 110.into()); // wrong usage
        assert_eq!(cm.global_min_lock_ts(), Some(120.into()));

        assert_eq!(cm.region_raw_unlock(1), 120.into());
        assert_eq!(cm.global_min_lock_ts(), None);
    }

    #[tokio::test]
    async fn test_raw_lock_guards() {
        let cm = ConcurrencyManager::new(1.into());
        assert_eq!(cm.global_min_lock_ts(), None);

        {
            let (old_value, _guard) = cm.region_raw_lock_with_guard(2, 100.into());
            assert_eq!(old_value, 0.into());
            assert_eq!(cm.global_min_lock_ts(), Some(100.into()));
        }
        assert_eq!(cm.global_min_lock_ts(), None);
    }
}
