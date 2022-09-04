// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::CausalTsProvider;

/// CausalObserver appends timestamp for RawKV V2 data, and invoke
/// causal_ts_provider.flush() on specified event, e.g. leader
/// transfer, snapshot apply.
/// Should be used ONLY when API v2 is enabled.
pub struct CausalObserver<Ts: CausalTsProvider> {
    causal_ts_provider: Arc<Ts>,
}

impl<Ts: CausalTsProvider> Clone for CausalObserver<Ts> {
    fn clone(&self) -> Self {
        Self {
            causal_ts_provider: self.causal_ts_provider.clone(),
        }
    }
}

// TODO: should keep region change observer.
