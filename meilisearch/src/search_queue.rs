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

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, oneshot};

use crate::error::MeilisearchHttpError;

#[derive(Debug)]
pub struct SearchQueue {
    sender: mpsc::Sender<oneshot::Sender<Permit>>,
    capacity: usize,
}

/// You should only run search requests while holding this permit.
/// Once it's dropped, a new search request will be able to process.
#[derive(Debug)]
pub struct Permit {
    sender: mpsc::Sender<()>,
}

impl Drop for Permit {
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

        tokio::task::spawn(Self::run(capacity, paralellism, receiver));
        Self { sender, capacity }
    }

    /// This function is the main loop, it's in charge on scheduling which search request should execute first and
    /// how many should executes at the same time.
    ///
    /// It **must never** panic or exit.
    async fn run(
        capacity: usize,
        parallelism: NonZeroUsize,
        mut receive_new_searches: mpsc::Receiver<oneshot::Sender<Permit>>,
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
        }
    }

    /// Returns a search `Permit`.
    /// It should be dropped as soon as you've freed all the RAM associated with the search request being processed.
    pub async fn try_get_search_permit(&self) -> Result<Permit, MeilisearchHttpError> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(sender).await.map_err(|_| MeilisearchHttpError::SearchLimiterIsDown)?;
        receiver.await.map_err(|_| MeilisearchHttpError::TooManySearchRequests(self.capacity))
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
