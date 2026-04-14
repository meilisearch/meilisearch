use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use rayon::{BroadcastContext, ThreadPool, ThreadPoolBuilder};
use thiserror::Error;

/// A rayon ThreadPool wrapper that can catch panics in the pool
/// and modifies the install function accordingly.
#[derive(Debug)]
pub struct ThreadPoolNoAbort {
    thread_pool: ThreadPool,
    /// The number of active operations.
    active_operations: AtomicUsize,
    /// Set to true if the thread pool caught a panic.
    pool_caught_panic: Arc<AtomicBool>,
}

impl ThreadPoolNoAbort {
    pub fn install<OP, R>(&self, op: OP) -> Result<R, CaughtPanic>
    where
        OP: FnOnce() -> R + Send,
        R: Send,
    {
        self.active_operations.fetch_add(1, Ordering::Relaxed);
        let output = self.thread_pool.install(op);
        self.active_operations.fetch_sub(1, Ordering::Relaxed);
        // While reseting the pool panic catcher we return an error if we caught one.
        if self.pool_caught_panic.swap(false, Ordering::SeqCst) {
            Err(CaughtPanic)
        } else {
            Ok(output)
        }
    }

    pub fn broadcast<OP, R>(&self, op: OP) -> Result<Vec<R>, CaughtPanic>
    where
        OP: Fn(BroadcastContext<'_>) -> R + Sync,
        R: Send,
    {
        self.active_operations.fetch_add(1, Ordering::Relaxed);
        let output = self.thread_pool.broadcast(op);
        self.active_operations.fetch_sub(1, Ordering::Relaxed);
        // While reseting the pool panic catcher we return an error if we caught one.
        if self.pool_caught_panic.swap(false, Ordering::SeqCst) {
            Err(CaughtPanic)
        } else {
            Ok(output)
        }
    }

    pub fn current_num_threads(&self) -> usize {
        self.thread_pool.current_num_threads()
    }

    /// The number of active operations.
    pub fn active_operations(&self) -> usize {
        self.active_operations.load(Ordering::Relaxed)
    }
}

#[derive(Error, Debug)]
#[error("A panic occurred. Read the logs to find more information about it")]
pub struct CaughtPanic;

#[derive(Default)]
pub struct ThreadPoolNoAbortBuilder(ThreadPoolBuilder);

impl ThreadPoolNoAbortBuilder {
    pub fn new() -> ThreadPoolNoAbortBuilder {
        ThreadPoolNoAbortBuilder::default()
    }

    pub fn new_for_indexing() -> ThreadPoolNoAbortBuilder {
        ThreadPoolNoAbortBuilder::default().thread_name(|index| format!("indexing-thread:{index}"))
    }

    pub fn thread_name<F>(mut self, closure: F) -> Self
    where
        F: FnMut(usize) -> String + 'static,
    {
        self.0 = self.0.thread_name(closure);
        self
    }

    pub fn num_threads(mut self, num_threads: usize) -> ThreadPoolNoAbortBuilder {
        self.0 = self.0.num_threads(num_threads);
        self
    }

    pub fn build(mut self) -> Result<ThreadPoolNoAbort, rayon::ThreadPoolBuildError> {
        let pool_caught_panic = Arc::new(AtomicBool::new(false));
        self.0 = self.0.panic_handler({
            let caught_panic = pool_caught_panic.clone();
            move |_result| caught_panic.store(true, Ordering::SeqCst)
        });
        Ok(ThreadPoolNoAbort {
            thread_pool: self.0.build()?,
            active_operations: AtomicUsize::new(0),
            pool_caught_panic,
        })
    }
}
