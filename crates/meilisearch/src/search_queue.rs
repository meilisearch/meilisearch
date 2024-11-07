//! This file implements a queue of searches to process and the ability to control how many searches can be run in parallel.
//! We need this because we don't want to process more search requests than we have cores.
//! That slows down everything and consumes RAM for no reason.
//! The steps to do a search are to get the `SearchQueue` data structure and try to get a search permit.
//! This can fail if the queue is full, and we need to drop your search request to register a new one.
//!
//! ### How to do a search request
//!
//! In order to do a search request you should try to get a search permit.
//! Retrieve the `SearchQueue` structure from actix-web (`search_queue: Data<SearchQueue>`)
//! and right before processing the search, calls the `SearchQueue::try_get_search_permit` method: `search_queue.try_get_search_permit().await?;`
//!
//! What is going to happen at this point is that you're going to send a oneshot::Sender over an async mpsc channel.
//! Then, the queue/scheduler is going to either:
//! - Drop your oneshot channel => that means there are too many searches going on, and yours won't be executed.
//!                                You should exit and free all the RAM you use ASAP.
//! - Sends you a Permit => that will unlock the method, and you will be able to process your search.
//!                         And should drop the Permit only once you have freed all the RAM consumed by the method.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, oneshot};

use crate::error::MeilisearchHttpError;

#[derive(Debug)]
pub struct SearchQueue {
    sender: mpsc::Sender<oneshot::Sender<Permit>>,
    capacity: usize,
    /// If we have waited longer than this to get a permit, we should abort the search request entirely.
    /// The client probably already closed the connection, but we have no way to find out.
    time_to_abort: Duration,
    searches_running: Arc<AtomicUsize>,
    searches_waiting_to_be_processed: Arc<AtomicUsize>,
}

/// You should only run search requests while holding this permit.
/// Once it's dropped, a new search request will be able to process.
/// You should always try to drop the permit yourself calling the `drop` async method on it.
#[derive(Debug)]
pub struct Permit {
    sender: mpsc::Sender<()>,
}

impl Permit {
    /// Drop the permit giving back on permit to the search queue.
    pub async fn drop(self) {
        // if the channel is closed then the whole instance is down
        let _ = self.sender.send(()).await;
    }
}

impl Drop for Permit {
    /// The implicit drop implementation can still be called in multiple cases:
    /// - We forgot to call the explicit one somewhere => this should be fixed on our side asap
    /// - The future is cancelled while running and the permit dropped with it
    fn drop(&mut self) {
        let sender = self.sender.clone();
        // if the channel is closed then the whole instance is down
        std::mem::drop(tokio::spawn(async move { sender.send(()).await }));
    }
}

impl SearchQueue {
    pub fn new(capacity: usize, paralellism: NonZeroUsize) -> Self {
        // Search requests are going to wait until we're available anyway,
        // so let's not allocate any RAM and keep a capacity of 1.
        let (sender, receiver) = mpsc::channel(1);

        let instance = Self {
            sender,
            capacity,
            time_to_abort: Duration::from_secs(60),
            searches_running: Default::default(),
            searches_waiting_to_be_processed: Default::default(),
        };

        tokio::task::spawn(Self::run(
            capacity,
            paralellism,
            receiver,
            Arc::clone(&instance.searches_running),
            Arc::clone(&instance.searches_waiting_to_be_processed),
        ));

        instance
    }

    pub fn with_time_to_abort(self, time_to_abort: Duration) -> Self {
        Self { time_to_abort, ..self }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn searches_running(&self) -> usize {
        self.searches_running.load(Ordering::Relaxed)
    }

    pub fn searches_waiting(&self) -> usize {
        self.searches_waiting_to_be_processed.load(Ordering::Relaxed)
    }

    /// This function is the main loop, it's in charge on scheduling which search request should execute first and
    /// how many should executes at the same time.
    ///
    /// It **must never** panic or exit.
    async fn run(
        capacity: usize,
        parallelism: NonZeroUsize,
        mut receive_new_searches: mpsc::Receiver<oneshot::Sender<Permit>>,
        metric_searches_running: Arc<AtomicUsize>,
        metric_searches_waiting: Arc<AtomicUsize>,
    ) {
        let mut queue: Vec<oneshot::Sender<Permit>> = Default::default();
        let mut rng: StdRng = StdRng::from_entropy();
        let mut searches_running: usize = 0;
        // By having a capacity of parallelism we ensures that every time a search finish it can release its RAM asap
        let (sender, mut search_finished) = mpsc::channel(parallelism.into());

        loop {
            tokio::select! {
                // biased select because we wants to free up space before trying to register new tasks
                biased;
                _ = search_finished.recv() => {
                    searches_running = searches_running.saturating_sub(1);
                    if !queue.is_empty() {
                        // Can't panic: the queue wasn't empty thus the range isn't empty.
                        let remove = rng.gen_range(0..queue.len());
                        let channel = queue.swap_remove(remove);
                        let _ = channel.send(Permit { sender: sender.clone() });
                    }
                },

                search_request = receive_new_searches.recv() => {
                    let search_request = match search_request {
                        Some(search_request) => search_request,
                        // This should never happen while actix-web is running, but it's not a reason to crash
                        // and it can generate a lot of noise in the tests.
                        None => continue,
                    };

                    if searches_running < usize::from(parallelism) && queue.is_empty() {
                        searches_running += 1;
                        // if the search requests die it's not a hard error on our side
                        let _ = search_request.send(Permit { sender: sender.clone() });
                        continue;
                    } else if capacity == 0 {
                        // in the very specific case where we have a capacity of zero
                        // we must refuse the request straight away without going through
                        // the queue stuff.
                        drop(search_request);
                        continue;

                    } else if queue.len() >= capacity {
                        let remove = rng.gen_range(0..queue.len());
                        let thing = queue.swap_remove(remove); // this will drop the channel and notify the search that it won't be processed
                        drop(thing);
                    }
                    queue.push(search_request);
                },
            }

            metric_searches_running.store(searches_running, Ordering::Relaxed);
            metric_searches_waiting.store(queue.len(), Ordering::Relaxed);
        }
    }

    /// Returns a search `Permit`.
    /// It should be dropped as soon as you've freed all the RAM associated with the search request being processed.
    pub async fn try_get_search_permit(&self) -> Result<Permit, MeilisearchHttpError> {
        let now = std::time::Instant::now();
        let (sender, receiver) = oneshot::channel();
        self.sender.send(sender).await.map_err(|_| MeilisearchHttpError::SearchLimiterIsDown)?;
        let permit = receiver
            .await
            .map_err(|_| MeilisearchHttpError::TooManySearchRequests(self.capacity))?;

        // If we've been for more than one minute to get a search permit, it's better to simply
        // abort the search request than spending time processing something were the client
        // most certainly exited or got a timeout a long time ago.
        // We may find a better solution in https://github.com/actix/actix-web/issues/3462.
        if now.elapsed() > self.time_to_abort {
            permit.drop().await;
            Err(MeilisearchHttpError::TooManySearchRequests(self.capacity))
        } else {
            Ok(permit)
        }
    }

    /// Returns `Ok(())` if everything seems normal.
    /// Returns `Err(MeilisearchHttpError::SearchLimiterIsDown)` if the search limiter seems down.
    pub fn health(&self) -> Result<(), MeilisearchHttpError> {
        if self.sender.is_closed() {
            Err(MeilisearchHttpError::SearchLimiterIsDown)
        } else {
            Ok(())
        }
    }
}
