use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};
use thiserror::Error;

/// A rayon ThreadPool wrapper that can catch panics in the pool
/// and modifies the install function accordingly.
#[derive(Debug)]
pub struct ThreadPoolNoAbort {
    thread_pool: ThreadPool,
    /// Set to true if the thread pool catched a panic.
    pool_catched_panic: Arc<AtomicBool>,
}

impl ThreadPoolNoAbort {
    pub fn install<OP, R>(&self, op: OP) -> Result<R, PanicCatched>
    where
        OP: FnOnce() -> R + Send,
        R: Send,
    {
        let output = self.thread_pool.install(op);
        // While reseting the pool panic catcher we return an error if we catched one.
        if self.pool_catched_panic.swap(false, Ordering::SeqCst) {
            Err(PanicCatched)
        } else {
            Ok(output)
        }
    }

    pub fn current_num_threads(&self) -> usize {
        self.thread_pool.current_num_threads()
    }
}

#[derive(Error, Debug)]
#[error("A panic occured. Read the logs to find more information about it")]
pub struct PanicCatched;

#[derive(Default)]
pub struct ThreadPoolNoAbortBuilder(ThreadPoolBuilder);

impl ThreadPoolNoAbortBuilder {
    pub fn new() -> ThreadPoolNoAbortBuilder {
        ThreadPoolNoAbortBuilder::default()
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
        let pool_catched_panic = Arc::new(AtomicBool::new(false));
        self.0 = self.0.panic_handler({
            let catched_panic = pool_catched_panic.clone();
            move |_result| catched_panic.store(true, Ordering::SeqCst)
        });
        Ok(ThreadPoolNoAbort { thread_pool: self.0.build()?, pool_catched_panic })
    }
}
