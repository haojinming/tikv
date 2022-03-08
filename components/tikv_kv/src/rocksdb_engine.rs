// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engine_rocks::get_env;
use engine_rocks::raw::DBOptions;
use engine_rocks::raw_util::CFOptions;
use engine_rocks::{RocksEngine as BaseRocksEngine, RocksEngineIterator};
use engine_traits::CfName;
use engine_traits::{
    Engines, IterOptions, Iterable, Iterator, KvEngine, Peekable, ReadOptions, SeekKey,
};
use file_system::IORateLimiter;
use kvproto::kvrpcpb::Context;
use kvproto::raft_cmdpb;
use tempfile::{Builder, TempDir};
use txn_types::{Key, Value};

use tikv_util::worker::{Runnable, Scheduler, Worker};

use super::{
    modifies_to_requests, write_modifies, Callback, DummySnapshotExt, Engine, Error, ErrorInner,
    ExtCallback, Iterator as EngineIterator, Modify, Result, SnapContext, Snapshot, WriteData,
};

pub use engine_rocks::RocksSnapshot;
use raftstore::store::msg::RaftRequestCallback;

// Duplicated in test_engine_builder
const TEMP_DIR: &str = "";

enum Task {
    Write(Vec<Modify>, Callback<()>),
    Snapshot(Callback<Arc<RocksSnapshot>>),
    Pause(Duration),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Write(..) => write!(f, "write task"),
            Task::Snapshot(_) => write!(f, "snapshot task"),
            Task::Pause(_) => write!(f, "pause"),
        }
    }
}

struct Runner(Engines<BaseRocksEngine, BaseRocksEngine>);

impl Runnable for Runner {
    type Task = Task;

    fn run(&mut self, t: Task) {
        match t {
            Task::Write(modifies, cb) => cb(write_modifies(&self.0.kv, modifies)),
            Task::Snapshot(cb) => cb(Ok(Arc::new(self.0.kv.snapshot()))),
            Task::Pause(dur) => std::thread::sleep(dur),
        }
    }
}

struct RocksEngineCore {
    // only use for memory mode
    temp_dir: Option<TempDir>,
    worker: Worker,
}

impl Drop for RocksEngineCore {
    fn drop(&mut self) {
        self.worker.stop();
    }
}

/// The RocksEngine is based on `RocksDB`.
///
/// This is intended for **testing use only**.
#[derive(Clone)]
pub struct RocksEngine {
    core: Arc<Mutex<RocksEngineCore>>,
    sched: Scheduler<Task>,
    engines: Engines<BaseRocksEngine, BaseRocksEngine>,
    not_leader: Arc<AtomicBool>,
}

impl RocksEngine {
    pub fn new(
        path: &str,
        cfs: &[CfName],
        cfs_opts: Option<Vec<CFOptions<'_>>>,
        shared_block_cache: bool,
        io_rate_limiter: Option<Arc<IORateLimiter>>,
    ) -> Result<RocksEngine> {
        info!("RocksEngine: creating for path"; "path" => path);
        let (path, temp_dir) = match path {
            TEMP_DIR => {
                let td = Builder::new().prefix("temp-rocksdb").tempdir().unwrap();
                (td.path().to_str().unwrap().to_owned(), Some(td))
            }
            _ => (path.to_owned(), None),
        };
        let worker = Worker::new("engine-rocksdb");
        let mut db_opts = DBOptions::new();
        db_opts.set_env(get_env(None /*key_manager*/, io_rate_limiter).unwrap());
        let db = Arc::new(engine_rocks::raw_util::new_engine(
            &path,
            Some(db_opts),
            cfs,
            cfs_opts,
        )?);
        // It does not use the raft_engine, so it is ok to fill with the same
        // rocksdb.
        let mut kv_engine = BaseRocksEngine::from_db(db.clone());
        let mut raft_engine = BaseRocksEngine::from_db(db);
        kv_engine.set_shared_block_cache(shared_block_cache);
        raft_engine.set_shared_block_cache(shared_block_cache);
        let engines = Engines::new(kv_engine, raft_engine);
        let sched = worker.start("engine-rocksdb", Runner(engines.clone()));
        Ok(RocksEngine {
            sched,
            core: Arc::new(Mutex::new(RocksEngineCore { temp_dir, worker })),
            not_leader: Arc::new(AtomicBool::new(false)),
            engines,
        })
    }

    pub fn trigger_not_leader(&self) {
        self.not_leader.store(true, Ordering::SeqCst);
    }

    pub fn pause(&self, dur: Duration) {
        self.sched.schedule(Task::Pause(dur)).unwrap();
    }

    pub fn engines(&self) -> Engines<BaseRocksEngine, BaseRocksEngine> {
        self.engines.clone()
    }

    pub fn get_rocksdb(&self) -> BaseRocksEngine {
        self.engines.kv.clone()
    }

    pub fn stop(&self) {
        let core = self.core.lock().unwrap();
        core.worker.stop();
    }
}

impl Display for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RocksDB")
    }
}

impl Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RocksDB [is_temp: {}]",
            self.core.lock().unwrap().temp_dir.is_some()
        )
    }
}

fn requests_to_modifies(requests: Vec<raft_cmdpb::Request>) -> Vec<Modify> {
    let mut modifies = Vec::with_capacity(requests.len());
    for mut req in requests {
        let m = match req.get_cmd_type() {
            raft_cmdpb::CmdType::Delete => {
                let delete = req.mut_delete();
                Modify::Delete(
                    engine_traits::name_to_cf(delete.get_cf()).unwrap(),
                    Key::from_encoded(delete.take_key()),
                )
            }
            raft_cmdpb::CmdType::Put if req.get_put().get_cf() == engine_traits::CF_LOCK => {
                let put = req.mut_put();
                let lock = txn_types::Lock::parse(put.get_value()).unwrap();
                Modify::PessimisticLock(
                    Key::from_encoded(put.take_key()),
                    txn_types::PessimisticLock {
                        primary: lock.primary.into_boxed_slice(),
                        start_ts: lock.ts,
                        ttl: lock.ttl,
                        for_update_ts: lock.for_update_ts,
                        min_commit_ts: lock.min_commit_ts,
                    },
                )
            }
            raft_cmdpb::CmdType::Put => {
                let put = req.mut_put();
                Modify::Put(
                    engine_traits::name_to_cf(put.get_cf()).unwrap(),
                    Key::from_encoded(put.take_key()),
                    put.take_value(),
                )
            }
            raft_cmdpb::CmdType::DeleteRange => {
                let delete_range = req.mut_delete_range();
                Modify::DeleteRange(
                    engine_traits::name_to_cf(delete_range.get_cf()).unwrap(),
                    Key::from_encoded(delete_range.take_start_key()),
                    Key::from_encoded(delete_range.take_end_key()),
                    delete_range.get_notify_only(),
                )
            }
            _ => {
                unimplemented!()
            }
        };
        modifies.push(m);
    }
    modifies
}

impl Engine for RocksEngine {
    type Snap = Arc<RocksSnapshot>;
    type Local = BaseRocksEngine;

    fn kv_engine(&self) -> BaseRocksEngine {
        self.engines.kv.clone()
    }

    fn snapshot_on_kv_engine(&self, _: &[u8], _: &[u8]) -> Result<Self::Snap> {
        self.snapshot(Default::default())
    }

    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> Result<()> {
        write_modifies(&self.engines.kv, modifies)
    }

    fn async_write(&self, ctx: &Context, batch: WriteData, cb: Callback<()>) -> Result<()> {
        self.async_write_ext(ctx, batch, cb, None, None, None)
    }

    fn async_write_ext(
        &self,
        _: &Context,
        mut batch: WriteData,
        cb: Callback<()>,
        pre_propose_cb: Option<RaftRequestCallback>,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> Result<()> {
        fail_point!("rockskv_async_write", |_| Err(box_err!("write failed")));

        if batch.modifies.is_empty() {
            return Err(Error::from(ErrorInner::EmptyRequest));
        }
        if let Some(cb) = pre_propose_cb {
            let mut reqs = modifies_to_requests(batch.modifies.clone());
            cb(&mut reqs);
            batch.modifies = requests_to_modifies(reqs);
        }
        if let Some(cb) = proposed_cb {
            cb();
        }
        if let Some(cb) = committed_cb {
            cb();
        }
        box_try!(self.sched.schedule(Task::Write(batch.modifies, cb)));
        Ok(())
    }

    fn async_snapshot(&self, _: SnapContext<'_>, cb: Callback<Self::Snap>) -> Result<()> {
        fail_point!("rockskv_async_snapshot", |_| Err(box_err!(
            "snapshot failed"
        )));
        let not_leader = {
            let mut header = kvproto::errorpb::Error::default();
            header.mut_not_leader().set_region_id(100);
            header
        };
        fail_point!("rockskv_async_snapshot_not_leader", |_| {
            Err(Error::from(ErrorInner::Request(not_leader.clone())))
        });
        if self.not_leader.load(Ordering::SeqCst) {
            return Err(Error::from(ErrorInner::Request(not_leader)));
        }
        box_try!(self.sched.schedule(Task::Snapshot(cb)));
        Ok(())
    }
}

impl Snapshot for Arc<RocksSnapshot> {
    type Iter = RocksEngineIterator;
    type Ext<'a> = DummySnapshotExt;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get"; "key" => %key);
        let v = self.get_value(key.as_encoded())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf(&self, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf"; "cf" => cf, "key" => %key);
        let v = self.get_value_cf(cf, key.as_encoded())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn get_cf_opt(&self, opts: ReadOptions, cf: CfName, key: &Key) -> Result<Option<Value>> {
        trace!("RocksSnapshot: get_cf"; "cf" => cf, "key" => %key);
        let v = self.get_value_cf_opt(&opts, cf, key.as_encoded())?;
        Ok(v.map(|v| v.to_vec()))
    }

    fn iter(&self, iter_opt: IterOptions) -> Result<Self::Iter> {
        trace!("RocksSnapshot: create iterator");
        Ok(self.iterator_opt(iter_opt)?)
    }

    fn iter_cf(&self, cf: CfName, iter_opt: IterOptions) -> Result<Self::Iter> {
        trace!("RocksSnapshot: create cf iterator");
        Ok(self.iterator_cf_opt(cf, iter_opt)?)
    }

    fn ext(&self) -> DummySnapshotExt {
        DummySnapshotExt
    }
}

impl EngineIterator for RocksEngineIterator {
    fn next(&mut self) -> Result<bool> {
        Iterator::next(self).map_err(Error::from)
    }

    fn prev(&mut self) -> Result<bool> {
        Iterator::prev(self).map_err(Error::from)
    }

    fn seek(&mut self, key: &Key) -> Result<bool> {
        Iterator::seek(self, key.as_encoded().as_slice().into()).map_err(Error::from)
    }

    fn seek_for_prev(&mut self, key: &Key) -> Result<bool> {
        Iterator::seek_for_prev(self, key.as_encoded().as_slice().into()).map_err(Error::from)
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        Iterator::seek(self, SeekKey::Start).map_err(Error::from)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        Iterator::seek(self, SeekKey::End).map_err(Error::from)
    }

    fn valid(&self) -> Result<bool> {
        Iterator::valid(self).map_err(Error::from)
    }

    fn key(&self) -> &[u8] {
        Iterator::key(self)
    }

    fn value(&self) -> &[u8] {
        Iterator::value(self)
    }
}
