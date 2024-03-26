use std::time::Duration;

use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::sync::{mpsc, oneshot};

use crate::error::MeilisearchHttpError;

#[derive(Debug)]
pub struct SearchQueue {
    sender: mpsc::Sender<oneshot::Sender<Permit>>,
    capacity: usize,
}

#[derive(Debug)]
pub struct Permit {
    sender: mpsc::Sender<()>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        // if the channel is closed then the whole instance is down
        let _ = futures::executor::block_on(self.sender.send(()));
    }
}

impl SearchQueue {
    pub fn new(capacity: usize, paralellism: usize) -> Self {
        // We can make the search requests wait until we're available.
        // they're going to wait anyway right after, so let's not allocate any
        // RAM by keeping a capacity of 1.
        let (sender, receiver) = mpsc::channel(1);
        tokio::task::spawn(Self::run(capacity, paralellism, receiver));
        Self { sender, capacity }
    }

    async fn run(
        capacity: usize,
        parallelism: usize,
        mut receive_new_searches: mpsc::Receiver<oneshot::Sender<Permit>>,
    ) {
        let mut queue: Vec<oneshot::Sender<Permit>> = Default::default();
        let mut rng: StdRng = StdRng::from_entropy();
        let mut searches_running = 0;
        // by having a capacity of parallelism we ensures that every time a search finish it can release its RAM asap
        let (sender, mut search_finished) = mpsc::channel(parallelism);

        loop {
            tokio::select! {
                search_request = receive_new_searches.recv() => {
                    let search_request = search_request.unwrap();
                    println!("queue contains {} elements and already running {}", queue.len(), searches_running);
                    if searches_running < parallelism && queue.is_empty() {
                        println!("We can process the search straight away");
                        searches_running += 1;
                        // if the search requests die it's not a hard error on our side
                        let _ = search_request.send(Permit { sender: sender.clone() });
                        continue;
                    }
                    if queue.len() >= capacity {
                        println!("we're above capacity, dropping a random request");
                        let remove = rng.gen_range(0..queue.len());
                        let thing = queue.swap_remove(remove); // this will drop the channel and notify the search that it won't be processed
                        drop(thing);
                    }
                    println!("pushed a new search request to the queue {}", queue.len());
                    queue.push(search_request);
                },
                _ = search_finished.recv() => {
                    searches_running = searches_running.saturating_sub(1);
                    if !queue.is_empty() {
                        println!("processed an element in the queue");
                        let remove = rng.gen_range(0..queue.len());
                        let channel = queue.swap_remove(remove);
                        let _ = channel.send(Permit { sender: sender.clone() });
                    }
                },
            }
        }
    }

    pub async fn register_search(&self) -> Result<Permit, MeilisearchHttpError> {
        let (sender, receiver) = oneshot::channel();
        self.sender.send(sender).await.map_err(|_| MeilisearchHttpError::SearchLimiterIsDown)?;
        receiver.await.map_err(|_| {
            MeilisearchHttpError::TooManySearchRequests(self.capacity, Duration::from_secs(10))
        })
    }
}
