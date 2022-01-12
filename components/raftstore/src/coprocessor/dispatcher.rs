// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::mem;
use std::ops::Deref;

use engine_traits::{CfName, KvEngine};
use kvproto::metapb::Region;
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{ComputeHashRequest, RaftCmdRequest};
use raft::eraftpb;

use super::*;
use crate::store::CasualRouter;

struct Entry<T> {
    priority: u32,
    observer: T,
}

impl<T: Clone> Clone for Entry<T> {
    fn clone(&self) -> Self {
        Self {
            priority: self.priority,
            observer: self.observer.clone(),
        }
    }
}

pub trait ClonableObserver: 'static + Send {
    type Ob: ?Sized + Send;
    fn inner(&self) -> &Self::Ob;
    fn inner_mut(&mut self) -> &mut Self::Ob;
    fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send>;
}

macro_rules! impl_box_observer {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name(Box<dyn ClonableObserver<Ob = dyn $ob> + Send>);
        impl $name {
            pub fn new<T: 'static + $ob + Clone>(observer: T) -> $name {
                $name(Box::new($wrapper { inner: observer }))
            }
        }
        impl Clone for $name {
            fn clone(&self) -> $name {
                $name((**self).box_clone())
            }
        }
        impl Deref for $name {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob> + Send> {
                &self.0
            }
        }

        struct $wrapper<T: $ob + Clone> {
            inner: T,
        }
        impl<T: 'static + $ob + Clone> ClonableObserver for $wrapper<T> {
            type Ob = dyn $ob;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                })
            }
        }
    };
}

// This is the same as impl_box_observer_g except $ob has a typaram
macro_rules! impl_box_observer_g {
    ($name:ident, $ob: ident, $wrapper: ident) => {
        pub struct $name<E: KvEngine>(Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send>);
        impl<E: KvEngine + 'static + Send> $name<E> {
            pub fn new<T: 'static + $ob<E> + Clone>(observer: T) -> $name<E> {
                $name(Box::new($wrapper {
                    inner: observer,
                    _phantom: PhantomData,
                }))
            }
        }
        impl<E: KvEngine + 'static> Clone for $name<E> {
            fn clone(&self) -> $name<E> {
                $name((**self).box_clone())
            }
        }
        impl<E: KvEngine> Deref for $name<E> {
            type Target = Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send>;

            fn deref(&self) -> &Box<dyn ClonableObserver<Ob = dyn $ob<E>> + Send> {
                &self.0
            }
        }

        struct $wrapper<E: KvEngine, T: $ob<E> + Clone> {
            inner: T,
            _phantom: PhantomData<E>,
        }
        impl<E: KvEngine + 'static + Send, T: 'static + $ob<E> + Clone> ClonableObserver
            for $wrapper<E, T>
        {
            type Ob = dyn $ob<E>;
            fn inner(&self) -> &Self::Ob {
                &self.inner as _
            }

            fn inner_mut(&mut self) -> &mut Self::Ob {
                &mut self.inner as _
            }

            fn box_clone(&self) -> Box<dyn ClonableObserver<Ob = Self::Ob> + Send> {
                Box::new($wrapper {
                    inner: self.inner.clone(),
                    _phantom: PhantomData,
                })
            }
        }
    };
}

impl_box_observer!(BoxAdminObserver, AdminObserver, WrappedAdminObserver);
impl_box_observer!(BoxQueryObserver, QueryObserver, WrappedQueryObserver);
impl_box_observer!(
    BoxApplySnapshotObserver,
    ApplySnapshotObserver,
    WrappedApplySnapshotObserver
);
impl_box_observer_g!(
    BoxSplitCheckObserver,
    SplitCheckObserver,
    WrappedSplitCheckObserver
);
impl_box_observer!(BoxRoleObserver, RoleObserver, WrappedRoleObserver);
impl_box_observer!(
    BoxRegionChangeObserver,
    RegionChangeObserver,
    WrappedRegionChangeObserver
);
impl_box_observer!(
    BoxRegionSplitObserver,
    RegionSplitObserver,
    WrappedRegionSplitObserver
);
impl_box_observer!(
    BoxRegionMergeObserver,
    RegionMergeObserver,
    WrappedRegionMergeObserver
);
impl_box_observer!(
    BoxReadIndexObserver,
    ReadIndexObserver,
    WrappedReadIndexObserver
);
impl_box_observer_g!(BoxCmdObserver, CmdObserver, WrappedCmdObserver);
impl_box_observer_g!(
    BoxConsistencyCheckObserver,
    ConsistencyCheckObserver,
    WrappedConsistencyCheckObserver
);

/// Registry contains all registered coprocessors.
#[derive(Clone)]
pub struct Registry<E>
where
    E: KvEngine + 'static,
{
    admin_observers: Vec<Entry<BoxAdminObserver>>,
    query_observers: Vec<Entry<BoxQueryObserver>>,
    apply_snapshot_observers: Vec<Entry<BoxApplySnapshotObserver>>,
    split_check_observers: Vec<Entry<BoxSplitCheckObserver<E>>>,
    consistency_check_observers: Vec<Entry<BoxConsistencyCheckObserver<E>>>,
    role_observers: Vec<Entry<BoxRoleObserver>>,
    region_change_observers: Vec<Entry<BoxRegionChangeObserver>>,
    region_split_observers: Vec<Entry<BoxRegionSplitObserver>>,
    region_merge_observers: Vec<Entry<BoxRegionMergeObserver>>,
    cmd_observers: Vec<Entry<BoxCmdObserver<E>>>,
    read_index_observers: Vec<Entry<BoxReadIndexObserver>>,
    // TODO: add endpoint
    global_cmd_observers: Vec<Entry<BoxCmdObserver<E>>>,
}

impl<E: KvEngine> Default for Registry<E> {
    fn default() -> Registry<E> {
        Registry {
            admin_observers: Default::default(),
            query_observers: Default::default(),
            apply_snapshot_observers: Default::default(),
            split_check_observers: Default::default(),
            consistency_check_observers: Default::default(),
            role_observers: Default::default(),
            region_change_observers: Default::default(),
            region_split_observers: Default::default(),
            region_merge_observers: Default::default(),
            cmd_observers: Default::default(),
            read_index_observers: Default::default(),
            global_cmd_observers: Default::default(),
        }
    }
}

macro_rules! push {
    ($p:expr, $t:ident, $vec:expr) => {
        $t.inner().start();
        let e = Entry {
            priority: $p,
            observer: $t,
        };
        let vec = &mut $vec;
        vec.push(e);
        vec.sort_by(|l, r| l.priority.cmp(&r.priority));
    };
}

impl<E: KvEngine> Registry<E> {
    pub fn register_admin_observer(&mut self, priority: u32, ao: BoxAdminObserver) {
        push!(priority, ao, self.admin_observers);
    }

    pub fn register_query_observer(&mut self, priority: u32, qo: BoxQueryObserver) {
        push!(priority, qo, self.query_observers);
    }

    pub fn register_apply_snapshot_observer(
        &mut self,
        priority: u32,
        aso: BoxApplySnapshotObserver,
    ) {
        push!(priority, aso, self.apply_snapshot_observers);
    }

    pub fn register_split_check_observer(&mut self, priority: u32, sco: BoxSplitCheckObserver<E>) {
        push!(priority, sco, self.split_check_observers);
    }

    pub fn register_consistency_check_observer(
        &mut self,
        priority: u32,
        cco: BoxConsistencyCheckObserver<E>,
    ) {
        push!(priority, cco, self.consistency_check_observers);
    }

    pub fn register_role_observer(&mut self, priority: u32, ro: BoxRoleObserver) {
        push!(priority, ro, self.role_observers);
    }

    pub fn register_region_change_observer(&mut self, priority: u32, rlo: BoxRegionChangeObserver) {
        push!(priority, rlo, self.region_change_observers);
    }

    pub fn register_region_split_observer(&mut self, priority: u32, rlo: BoxRegionSplitObserver) {
        push!(priority, rlo, self.region_split_observers);
    }

    pub fn register_region_merge_observer(&mut self, priority: u32, rlo: BoxRegionMergeObserver) {
        push!(priority, rlo, self.region_merge_observers);
    }

    pub fn register_cmd_observer(&mut self, priority: u32, rlo: BoxCmdObserver<E>) {
        push!(priority, rlo, self.cmd_observers);
    }

    pub fn register_read_index_observer(&mut self, priority: u32, rio: BoxReadIndexObserver) {
        push!(priority, rio, self.read_index_observers);
    }

    pub fn register_global_cmd_observer(&mut self, priority: u32, rlo: BoxCmdObserver<E>) {
        push!(priority, rlo, self.global_cmd_observers);
    }
}

/// A macro that loops over all observers and returns early when error is found or
/// bypass is set. `try_loop_ob` is expected to be used for hook that returns a `Result`.
macro_rules! try_loop_ob {
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _res, $r, $obs, $hook, $($args)*)
    };
}

/// A macro that loops over all observers and returns early when bypass is set.
///
/// Using a macro so we don't need to write tests for every observers.
macro_rules! loop_ob {
    // Execute a hook, return early if error is found.
    (_exec _res, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)?
    };
    // Execute a hook.
    (_exec _tup, $o:expr, $hook:ident, $ctx:expr, $($args:tt)*) => {
        $o.inner().$hook($ctx, $($args)*)
    };
    // When the try loop finishes successfully, the value to be returned.
    (_done _res) => {
        Ok(())
    };
    // When the loop finishes successfully, the value to be returned.
    (_done _tup) => {{}};
    // Actual implementation of the for loop.
    (_imp $res_type:tt, $r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {{
        let mut ctx = ObserverContext::new($r);
        for o in $obs {
            loop_ob!(_exec $res_type, o.observer, $hook, &mut ctx, $($args)*);
            if ctx.bypass {
                break;
            }
        }
        loop_ob!(_done $res_type)
    }};
    // Loop over all observers and return early when bypass is set.
    // This macro is expected to be used for hook that returns `()`.
    ($r:expr, $obs:expr, $hook:ident, $($args:tt)*) => {
        loop_ob!(_imp _tup, $r, $obs, $hook, $($args)*)
    };
}

/// Admin and invoke all coprocessors.
#[derive(Clone)]
pub struct CoprocessorHost<E>
where
    E: KvEngine + 'static,
{
    pub registry: Registry<E>,
    pub cfg: Config,
}

impl<E: KvEngine> Default for CoprocessorHost<E>
where
    E: 'static,
{
    fn default() -> Self {
        CoprocessorHost {
            registry: Default::default(),
            cfg: Default::default(),
        }
    }
}

use prometheus::local::LocalHistogram;
pub struct CoprocessorHostMetrics<'a> {
    pub on_apply_cmd_1: &'a mut LocalHistogram,
    pub on_apply_cmd_2: &'a mut LocalHistogram,
    pub on_apply_cmd_3: &'a mut LocalHistogram,
}

impl<E: KvEngine> CoprocessorHost<E> {
    pub fn new<C: CasualRouter<E> + Clone + Send + 'static>(
        ch: C,
        cfg: Config,
    ) -> CoprocessorHost<E> {
        let mut registry = Registry::default();
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(SizeCheckObserver::new(ch.clone())),
        );
        registry.register_split_check_observer(
            200,
            BoxSplitCheckObserver::new(KeysCheckObserver::new(ch)),
        );
        // TableCheckObserver has higher priority than SizeCheckObserver.
        registry.register_split_check_observer(100, BoxSplitCheckObserver::new(HalfCheckObserver));
        registry.register_split_check_observer(
            400,
            BoxSplitCheckObserver::new(TableCheckObserver::default()),
        );
        CoprocessorHost { registry, cfg }
    }

    pub fn pre_propose_barrier(&self, region: &Region) -> Result<()> {
        try_loop_ob!(region, &self.registry.query_observers, pre_propose_barrier,)
    }

    /// Call all propose hooks until bypass is set to true.
    pub fn pre_propose(&self, region: &Region, req: &mut RaftCmdRequest) -> Result<()> {
        if !req.has_admin_request() {
            let query = req.mut_requests();
            let mut vec_query = mem::take(query).into();
            let result = try_loop_ob!(
                region,
                &self.registry.query_observers,
                pre_propose_query,
                &mut vec_query,
            );
            *query = vec_query.into();
            result
        } else {
            let admin = req.mut_admin_request();
            try_loop_ob!(
                region,
                &self.registry.admin_observers,
                pre_propose_admin,
                admin
            )
        }
    }

    /// Call all pre apply hook until bypass is set to true.
    pub fn pre_apply(&self, region: &Region, req: &RaftCmdRequest) {
        if !req.has_admin_request() {
            let query = req.get_requests();
            loop_ob!(
                region,
                &self.registry.query_observers,
                pre_apply_query,
                query,
            );
        } else {
            let admin = req.get_admin_request();
            loop_ob!(
                region,
                &self.registry.admin_observers,
                pre_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply(&self, region: &Region, cmd: &mut Cmd) {
        if !cmd.response.has_admin_response() {
            loop_ob!(
                region,
                &self.registry.query_observers,
                post_apply_query,
                cmd,
            );
        } else {
            let admin = cmd.response.mut_admin_response();
            loop_ob!(
                region,
                &self.registry.admin_observers,
                post_apply_admin,
                admin
            );
        }
    }

    pub fn post_apply_plain_kvs_from_snapshot(
        &self,
        region: &Region,
        cf: CfName,
        kv_pairs: &[(Vec<u8>, Vec<u8>)],
    ) {
        loop_ob!(
            region,
            &self.registry.apply_snapshot_observers,
            apply_plain_kvs,
            cf,
            kv_pairs
        );
    }

    pub fn post_apply_sst_from_snapshot(&self, region: &Region, cf: CfName, path: &str) {
        loop_ob!(
            region,
            &self.registry.apply_snapshot_observers,
            apply_sst,
            cf,
            path
        );
    }

    pub fn new_split_checker_host<'a>(
        &'a self,
        region: &Region,
        engine: &E,
        auto_split: bool,
        policy: CheckPolicy,
    ) -> SplitCheckerHost<'a, E> {
        let mut host = SplitCheckerHost::new(auto_split, &self.cfg);
        loop_ob!(
            region,
            &self.registry.split_check_observers,
            add_checker,
            &mut host,
            engine,
            policy
        );
        host
    }

    pub fn on_prepropose_compute_hash(&self, req: &mut ComputeHashRequest) {
        for observer in &self.registry.consistency_check_observers {
            let observer = observer.observer.inner();
            if observer.update_context(req.mut_context()) {
                break;
            }
        }
    }

    pub fn on_compute_hash(
        &self,
        region: &Region,
        context: &[u8],
        snap: E::Snapshot,
    ) -> Result<Vec<(Vec<u8>, u32)>> {
        let mut hashes = Vec::new();
        let (mut reader, context_len) = (context, context.len());
        for observer in &self.registry.consistency_check_observers {
            let observer = observer.observer.inner();
            let old_len = reader.len();
            let hash = match box_try!(observer.compute_hash(region, &mut reader, &snap)) {
                Some(hash) => hash,
                None => break,
            };
            let new_len = reader.len();
            let ctx = context[context_len - old_len..context_len - new_len].to_vec();
            hashes.push((ctx, hash));
        }
        Ok(hashes)
    }

    pub fn on_role_change(&self, region: &Region, role: StateRole) {
        loop_ob!(region, &self.registry.role_observers, on_role_change, role);
    }

    pub fn on_region_changed(&self, region: &Region, event: RegionChangeEvent, role: StateRole) {
        loop_ob!(
            region,
            &self.registry.region_change_observers,
            on_region_changed,
            event,
            role
        );
    }

    pub fn on_region_split(&self, old_region_id: u64, new_region_id: u64) {
        for ob in &self.registry.region_split_observers {
            ob.observer
                .inner()
                .on_region_split(old_region_id, new_region_id);
        }
    }

    pub fn on_region_merge(&self, source_region_id: u64, target_region_id: u64) {
        for ob in &self.registry.region_merge_observers {
            ob.observer
                .inner()
                .on_region_merge(source_region_id, target_region_id);
        }
    }

    pub fn prepare_for_apply(&self, observe_cmd: Option<&ObserveCmd>, region_id: u64) {
        for ob in &self.registry.global_cmd_observers {
            ob.observer
                .inner()
                .on_prepare_for_apply(ObserveID::zero(), region_id);
        }

        if let Some(observe_cmd) = observe_cmd {
            let observe_id = observe_cmd.id;
            for cmd_ob in &self.registry.cmd_observers {
                cmd_ob
                    .observer
                    .inner()
                    .on_prepare_for_apply(observe_id, region_id);
            }
        }
    }

    pub fn on_apply_cmd(
        &self,
        observe_cmd: Option<&ObserveCmd>,
        region_id: u64,
        cmd: &Cmd,
        _t: &Option<tikv_util::time::Instant>,
        _metrics: Option<&mut CoprocessorHostMetrics>,
    ) {
        for ob in &self.registry.global_cmd_observers {
            ob.observer
                .inner()
                .on_apply_cmd(ObserveID::zero(), region_id, cmd);
        }

        // if let (Some(t), Some(metrics)) = (t, metrics.as_ref()) {
        //     metrics.on_apply_cmd_1.observe(t.saturating_elapsed_secs());
        // }

        if let Some(observe_cmd) = observe_cmd {
            let observe_id = observe_cmd.id;
            assert!(
                !self.registry.cmd_observers.is_empty(),
                "CmdObserver is not registered"
            );
            for i in 0..self.registry.cmd_observers.len() - 1 {
                self.registry
                    .cmd_observers
                    .get(i)
                    .unwrap()
                    .observer
                    .inner()
                    .on_apply_cmd(observe_id, region_id, cmd)
            }
            // if let (Some(t), Some(metrics)) = (t, metrics.as_ref()) {
            //     metrics.on_apply_cmd_2.observe(t.saturating_elapsed_secs());
            // }
            self.registry
                .cmd_observers
                .last()
                .unwrap()
                .observer
                .inner()
                .on_apply_cmd(observe_id, region_id, cmd);
            // if let (Some(t), Some(metrics)) = (t, metrics.as_ref()) {
            //     metrics.on_apply_cmd_3.observe(t.saturating_elapsed_secs());
            // }
        }
    }

    pub fn on_flush_apply(&self, engine: Option<E>) {
        // for ob in &self.registry.global_cmd_observers {
        //     ob.observer.inner().on_flush_apply(engine.clone());
        // }

        if self.registry.cmd_observers.is_empty() {
            return;
        }

        for i in 0..self.registry.cmd_observers.len() - 1 {
            self.registry
                .cmd_observers
                .get(i)
                .unwrap()
                .observer
                .inner()
                .on_flush_apply(engine.clone())
        }
        self.registry
            .cmd_observers
            .last()
            .unwrap()
            .observer
            .inner()
            .on_flush_apply(engine)
    }

    pub fn on_step_read_index(&self, msg: &mut eraftpb::Message) {
        for step_ob in &self.registry.read_index_observers {
            step_ob.observer.inner().on_step(msg);
        }
    }

    pub fn shutdown(&self) {
        for entry in &self.registry.admin_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.query_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.split_check_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.cmd_observers {
            entry.observer.inner().stop();
        }
        for entry in &self.registry.global_cmd_observers {
            entry.observer.inner().stop();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::coprocessor::*;
    use std::sync::atomic::*;
    use std::sync::Arc;

    use engine_panic::PanicEngine;
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{
        AdminRequest, AdminResponse, RaftCmdRequest, RaftCmdResponse, Request,
    };

    #[derive(Clone, Default)]
    struct TestCoprocessor {
        bypass: Arc<AtomicBool>,
        called: Arc<AtomicUsize>,
        return_err: Arc<AtomicBool>,
    }

    impl Coprocessor for TestCoprocessor {}

    impl AdminObserver for TestCoprocessor {
        fn pre_propose_admin(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut AdminRequest,
        ) -> Result<()> {
            self.called.fetch_add(1, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &AdminRequest) {
            self.called.fetch_add(2, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_admin(&self, ctx: &mut ObserverContext<'_>, _: &mut AdminResponse) {
            self.called.fetch_add(3, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl QueryObserver for TestCoprocessor {
        fn pre_propose_query(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: &mut Vec<Request>,
        ) -> Result<()> {
            self.called.fetch_add(4, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
            if self.return_err.load(Ordering::SeqCst) {
                return Err(box_err!("error"));
            }
            Ok(())
        }

        fn pre_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &[Request]) {
            self.called.fetch_add(5, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn post_apply_query(&self, ctx: &mut ObserverContext<'_>, _: &mut Cmd) {
            self.called.fetch_add(6, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RoleObserver for TestCoprocessor {
        fn on_role_change(&self, ctx: &mut ObserverContext<'_>, _: StateRole) {
            self.called.fetch_add(7, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RegionChangeObserver for TestCoprocessor {
        fn on_region_changed(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: RegionChangeEvent,
            _: StateRole,
        ) {
            self.called.fetch_add(8, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl RegionSplitObserver for TestCoprocessor {}

    impl RegionMergeObserver for TestCoprocessor {}

    impl ApplySnapshotObserver for TestCoprocessor {
        fn apply_plain_kvs(
            &self,
            ctx: &mut ObserverContext<'_>,
            _: CfName,
            _: &[(Vec<u8>, Vec<u8>)],
        ) {
            self.called.fetch_add(9, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }

        fn apply_sst(&self, ctx: &mut ObserverContext<'_>, _: CfName, _: &str) {
            self.called.fetch_add(10, Ordering::SeqCst);
            ctx.bypass = self.bypass.load(Ordering::SeqCst);
        }
    }

    impl CmdObserver<PanicEngine> for TestCoprocessor {
        fn on_prepare_for_apply(&self, _: ObserveID, _: u64) {
            self.called.fetch_add(11, Ordering::SeqCst);
        }
        fn on_apply_cmd(&self, _: ObserveID, _: u64, _: &Cmd) {
            self.called.fetch_add(12, Ordering::SeqCst);
        }
        fn on_flush_apply(&self, _: Option<PanicEngine>) {
            self.called.fetch_add(13, Ordering::SeqCst);
        }
    }

    macro_rules! assert_all {
        ($target:expr, $expect:expr) => {{
            for (c, e) in ($target).iter().zip($expect) {
                assert_eq!(c.load(Ordering::SeqCst), *e);
            }
        }};
    }

    macro_rules! set_all {
        ($target:expr, $v:expr) => {{
            for v in $target {
                v.store($v, Ordering::SeqCst);
            }
        }};
    }

    #[test]
    fn test_trigger_right_hook() {
        let mut host = CoprocessorHost::<PanicEngine>::default();
        let ob = TestCoprocessor::default();
        host.registry
            .register_admin_observer(1, BoxAdminObserver::new(ob.clone()));
        host.registry
            .register_query_observer(1, BoxQueryObserver::new(ob.clone()));
        host.registry
            .register_apply_snapshot_observer(1, BoxApplySnapshotObserver::new(ob.clone()));
        host.registry
            .register_role_observer(1, BoxRoleObserver::new(ob.clone()));
        host.registry
            .register_region_change_observer(1, BoxRegionChangeObserver::new(ob.clone()));
        host.registry
            .register_cmd_observer(1, BoxCmdObserver::new(ob.clone()));
        let region = Region::default();
        let mut admin_req = RaftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        host.pre_propose(&region, &mut admin_req).unwrap();
        assert_all!(&[&ob.called], &[1]);
        host.pre_apply(&region, &admin_req);
        assert_all!(&[&ob.called], &[3]);
        let mut admin_resp = RaftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        host.post_apply(&region, &mut Cmd::new(0, admin_req, admin_resp));
        assert_all!(&[&ob.called], &[6]);

        let mut query_req = RaftCmdRequest::default();
        query_req.set_requests(vec![Request::default()].into());
        host.pre_propose(&region, &mut query_req).unwrap();
        assert_all!(&[&ob.called], &[10]);
        host.pre_apply(&region, &query_req);
        assert_all!(&[&ob.called], &[15]);
        let query_resp = RaftCmdResponse::default();
        host.post_apply(&region, &mut Cmd::new(0, query_req, query_resp));
        assert_all!(&[&ob.called], &[21]);

        host.on_role_change(&region, StateRole::Leader);
        assert_all!(&[&ob.called], &[28]);

        host.on_region_changed(&region, RegionChangeEvent::Create, StateRole::Follower);
        assert_all!(&[&ob.called], &[36]);

        host.post_apply_plain_kvs_from_snapshot(&region, "default", &[]);
        assert_all!(&[&ob.called], &[45]);
        host.post_apply_sst_from_snapshot(&region, "default", "");
        assert_all!(&[&ob.called], &[55]);
        let observe_id = ObserveID::new();
        let observe_cmd = ObserveCmd { id: observe_id };
        host.prepare_for_apply(Some(&observe_cmd), 0);
        assert_all!(&[&ob.called], &[66]);
        let t = None;
        host.on_apply_cmd(
            Some(&observe_cmd),
            0,
            &Cmd::new(0, RaftCmdRequest::default(), RaftCmdResponse::default()),
            &t,
            None,
        );
        assert_all!(&[&ob.called], &[78]);
        host.on_flush_apply(Some(PanicEngine));
        assert_all!(&[&ob.called], &[91]);
    }

    #[test]
    fn test_order() {
        let mut host = CoprocessorHost::<PanicEngine>::default();

        let ob1 = TestCoprocessor::default();
        host.registry
            .register_admin_observer(3, BoxAdminObserver::new(ob1.clone()));
        host.registry
            .register_query_observer(3, BoxQueryObserver::new(ob1.clone()));
        let ob2 = TestCoprocessor::default();
        host.registry
            .register_admin_observer(2, BoxAdminObserver::new(ob2.clone()));
        host.registry
            .register_query_observer(2, BoxQueryObserver::new(ob2.clone()));

        let region = Region::default();
        let mut admin_req = RaftCmdRequest::default();
        admin_req.set_admin_request(AdminRequest::default());
        let mut admin_resp = RaftCmdResponse::default();
        admin_resp.set_admin_response(AdminResponse::default());
        let query_req = RaftCmdRequest::default();
        let query_resp = RaftCmdResponse::default();

        let cases = vec![(0, admin_req, admin_resp), (3, query_req, query_resp)];

        for (base_score, mut req, resp) in cases {
            set_all!(&[&ob1.return_err, &ob2.return_err], false);
            set_all!(&[&ob1.called, &ob2.called], 0);
            set_all!(&[&ob1.bypass, &ob2.bypass], true);

            host.pre_propose(&region, &mut req).unwrap();

            // less means more.
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score + 1]);

            host.pre_apply(&region, &req);
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score * 2 + 3]);

            host.post_apply(&region, &mut Cmd::new(0, req.clone(), resp.clone()));
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score * 3 + 6]);

            set_all!(&[&ob2.bypass], false);
            set_all!(&[&ob2.called], 0);

            host.pre_propose(&region, &mut req).unwrap();

            assert_all!(
                &[&ob1.called, &ob2.called],
                &[base_score + 1, base_score + 1]
            );

            set_all!(&[&ob1.called, &ob2.called], 0);

            // when return error, following coprocessor should not be run.
            set_all!(&[&ob2.return_err], true);
            host.pre_propose(&region, &mut req).unwrap_err();
            assert_all!(&[&ob1.called, &ob2.called], &[0, base_score + 1]);
        }
    }
}
