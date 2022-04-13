// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::metrics::*;
use crate::Result;
use api_version::{dispatch_api_version, APIVersion, KeyMode};
use file_system::IOType;
use futures::Future;
use kvproto::kvrpcpb::ApiVersion;
use tokio::io::Result as TokioResult;
use tokio::runtime::Runtime;
use txn_types::{Key, TimeStamp};

use tikv_util::error;

// use 1 as timestamp as ts is desc encoded in key,
// when seeking, use user_key+0 as the ending key(exclusive), so 0 should not be used.
pub const BACKUP_V1_TO_V2_TS: u64 = 1;
/// DaemonRuntime is a "background" runtime, which contains "daemon" tasks:
/// any task spawn into it would run until finish even the runtime isn't referenced.
pub struct DaemonRuntime(Option<Runtime>);

impl DaemonRuntime {
    /// spawn a daemon task to the runtime.
    pub fn spawn(self: &Arc<Self>, f: impl Future<Output = ()> + Send + 'static) {
        let wkr = self.clone();
        self.0.as_ref().unwrap().spawn(async move {
            f.await;
            drop(wkr)
        });
    }

    /// create a daemon runtime from some runtime.
    pub fn from_runtime(rt: Runtime) -> Arc<Self> {
        Arc::new(Self(Some(rt)))
    }
}

impl Drop for DaemonRuntime {
    fn drop(&mut self) {
        // it is safe because all tasks should be finished.
        self.0.take().unwrap().shutdown_background()
    }
}
pub struct ControlThreadPool {
    pub(crate) size: usize,
    workers: Option<Arc<DaemonRuntime>>,
}

impl ControlThreadPool {
    pub fn new() -> Self {
        ControlThreadPool {
            size: 0,
            workers: None,
        }
    }

    pub fn spawn<F>(&self, func: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.workers
            .as_ref()
            .expect("ControlThreadPool: please call adjust_with() before spawn()")
            .spawn(func);
    }

    /// Lazily adjust the thread pool's size
    ///
    /// Resizing if the thread pool need to expend or there
    /// are too many idle threads. Otherwise do nothing.
    pub fn adjust_with(&mut self, new_size: usize) {
        if self.size >= new_size && self.size - new_size <= 10 {
            return;
        }
        // TODO: after tokio supports adjusting thread pool size(https://github.com/tokio-rs/tokio/issues/3329),
        //   adapt it.
        let workers = create_tokio_runtime(new_size, "bkwkr")
            .expect("failed to create tokio runtime for backup worker.");
        self.workers = Some(DaemonRuntime::from_runtime(workers));
        self.size = new_size;
        BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64);
    }
}

/// Create a standard tokio runtime.
/// (which allows io and time reactor, involve thread memory accessor),
/// and set the I/O type to export.
pub fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name(thread_name)
        .enable_io()
        .enable_time()
        .on_thread_start(|| {
            tikv_alloc::add_thread_memory_accessor();
            file_system::set_io_type(IOType::Export);
        })
        .on_thread_stop(|| {
            tikv_alloc::remove_thread_memory_accessor();
        })
        .worker_threads(thread_count)
        .build()
}

#[derive(Debug, Copy, Clone)]
pub struct KeyValueCodec {
    pub is_raw_kv: bool,
    pub cur_api_ver: ApiVersion,
    pub dst_api_ver: ApiVersion,
}

impl KeyValueCodec {
    pub fn new(is_raw_kv: bool, cur_api_ver: ApiVersion, dst_api_ver: ApiVersion) -> Self {
        KeyValueCodec {
            is_raw_kv,
            cur_api_ver,
            dst_api_ver,
        }
    }

    pub fn is_valid_raw_value(&self, key: &[u8], value: &[u8], ts: u64) -> Result<bool> {
        if !self.is_raw_kv {
            return Ok(false);
        }
        dispatch_api_version!(self.cur_api_ver, {
            let key_mode = API::parse_key_mode(key);
            if key_mode != KeyMode::Raw && key_mode != KeyMode::Unknown {
                return Ok(false);
            }
            let raw_value = API::decode_raw_value(value)?;
            return Ok(raw_value.is_valid(ts));
        })
    }

    pub fn convert_to_dst_raw_key(&self, key: &[u8]) -> Result<Key> {
        let ret = dispatch_api_version!(self.dst_api_ver, {
            API::convert_raw_key_from(
                self.cur_api_ver,
                key,
                Some(TimeStamp::from(BACKUP_V1_TO_V2_TS)),
            )
        });
        ret.map_err(|err| {
            error!("convert raw key fails";
                "key" => &log_wrappers::Value::key(key),
                "cur_api_version" => ?self.cur_api_ver,
                "dst_api_ver" => ?self.dst_api_ver,
            );
            err.into()
        })
    }

    pub fn convert_to_dst_raw_value(&self, value: &[u8]) -> Result<Vec<u8>> {
        let ret = dispatch_api_version!(self.dst_api_ver, {
            API::convert_raw_value_from(self.cur_api_ver, value)
        });
        ret.map_err(|err| {
            error!("convert raw value fails";
                "value" => &log_wrappers::Value::value(value),
                "cur_api_version" => ?self.cur_api_ver,
                "dst_api_version" => ?self.dst_api_ver,
            );
            err.into()
        })
    }

    pub fn use_raw_mvcc_snapshot(&self) -> bool {
        self.is_raw_kv && self.cur_api_ver == ApiVersion::V2
    }

    pub fn encode_backup_key(key: Vec<u8>, is_raw_kv: bool, api_ver: ApiVersion) -> Option<Key> {
        if key.is_empty() {
            return None;
        }
        if !is_raw_kv || api_ver == ApiVersion::V2 {
            return Some(Key::from_raw(&key));
        }
        Some(Key::from_encoded(key))
    }

    pub fn decode_backup_key(key: Key, is_raw_kv: bool, api_ver: ApiVersion) -> Vec<u8> {
        if !is_raw_kv || api_ver == ApiVersion::V2 {
            return key.into_raw().unwrap();
        }
        key.into_encoded()
    }
}
