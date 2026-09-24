//! A dedicated, long-lived thread pool for the blocking work of HTTP route handlers.
//!
//! actix-web runs each HTTP worker on its own tokio current-thread runtime. actix-server
//! builds those runtimes itself, with tokio's default blocking-pool settings, and does not
//! let us change them. With those defaults a blocking thread exits after only 10 seconds
//! of idleness, so a steady trickle of requests keeps creating and destroying OS threads:
//! thousands per day on a small but busy instance.
//!
//! Frequent thread exits hurt us twice:
//!
//! - Every exit runs the global allocator's thread teardown (`_mi_thread_done` for
//!   mimalloc), which abandons the thread's heap pages to be reclaimed by other threads.
//!   This is the most delicate part of the allocator, and we have observed an exiting
//!   `tokio-rt-worker` spinning there forever at 100% CPU
//!   (<https://github.com/meilisearch/meilisearch/issues/6642>).
//! - When a blocking thread times out, tokio joins the *previously* exited blocking thread
//!   from it. So once a single thread never finishes exiting, the next thread to time out
//!   blocks forever in `join`, then the next one blocks joining that one, and so on. Every
//!   idle timeout leaves one more thread behind, and thread count and memory grow without
//!   bound until the process is restarted.
//!
//! Running the blocking work on a pool whose threads stay alive for a long time turns
//! thread exits from a constant event into a rare one, which removes the churn and stops
//! the cascade from growing even if a thread ever gets stuck on exit.

use std::sync::LazyLock;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

/// Name of the threads of the blocking pool, as shown in `top -H` or `/proc/<pid>/task/*/comm`.
///
/// Must stay under 16 bytes, the Linux limit for thread names.
pub const THREAD_NAME: &str = "meili-blocking";

/// How long an idle blocking thread is kept before it exits.
///
/// tokio defaults to 10 seconds, which makes threads exit constantly under steady traffic.
/// An hour keeps them around across any realistic gap between requests while still
/// letting the pool shrink after a burst.
const THREAD_KEEP_ALIVE: Duration = Duration::from_secs(60 * 60);

/// Upper bound on the number of blocking threads, matching tokio's default.
const MAX_BLOCKING_THREADS: usize = 512;

static BLOCKING_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        // The async worker only drives the rare futures spawned from blocking closures,
        // for example from `Drop` implementations that call `tokio::spawn`.
        .worker_threads(1)
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .thread_keep_alive(THREAD_KEEP_ALIVE)
        .thread_name(THREAD_NAME)
        .enable_all()
        .build()
        .expect("failed to build the blocking thread pool runtime")
});

/// Runs `f` on the long-lived blocking thread pool.
///
/// Drop-in replacement for [`tokio::task::spawn_blocking`]: it returns the same
/// [`JoinHandle`], so awaiting it yields the same `Result<R, JoinError>`, including when
/// `f` panics.
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    BLOCKING_RUNTIME.spawn_blocking(f)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use super::{spawn_blocking, THREAD_NAME};

    fn current_thread_name() -> String {
        std::thread::current().name().unwrap_or_default().to_owned()
    }

    #[actix_rt::test]
    async fn runs_on_the_dedicated_pool() {
        let result = spawn_blocking(|| (current_thread_name(), 40 + 2)).await.unwrap();
        assert_eq!(result, (THREAD_NAME.to_owned(), 42));
    }

    #[actix_rt::test]
    async fn panics_are_reported_as_join_errors() {
        let error = spawn_blocking(|| panic!("boom")).await.unwrap_err();
        assert!(error.is_panic());
    }

    #[actix_rt::test]
    async fn tokio_spawn_from_blocking_closure_works() {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        spawn_blocking(move || {
            // Same pattern as `search_queue::Permit::drop`.
            std::mem::drop(tokio::spawn(async move { sender.send(7).unwrap() }));
        })
        .await
        .unwrap();
        assert_eq!(receiver.await.unwrap(), 7);
    }

    /// The whole point of this pool: threads stay alive past tokio's 10 seconds default
    /// keep-alive, so the same OS threads are reused instead of being recreated.
    #[actix_rt::test]
    async fn idle_threads_are_kept_past_tokio_default_keep_alive() {
        let thread_id = || spawn_blocking(|| std::thread::current().id());

        let mut before = HashSet::new();
        for _ in 0..4 {
            before.insert(thread_id().await.unwrap());
        }

        tokio::time::sleep(Duration::from_secs(11)).await;

        let after = thread_id().await.unwrap();
        assert!(
            before.contains(&after),
            "a new blocking thread was created after 11s idle, so idle threads are exiting"
        );
    }
}
