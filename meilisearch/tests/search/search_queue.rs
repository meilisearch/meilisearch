use std::{sync::Arc, time::Duration};

use meili_snap::snapshot;
use meilisearch::search_queue::SearchQueue;

#[actix_rt::test]
async fn search_queue_register() {
    let queue = SearchQueue::new(4, 2);

    // First, use all the cores
    let permit1 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
    let _permit2 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // If we free one spot we should be able to register one new search
    drop(permit1);

    let permit3 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // And again
    drop(permit3);

    let _permit4 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}

#[actix_rt::test]
async fn search_queue_wait_till_cores_available() {
    let queue = Arc::new(SearchQueue::new(4, 1));

    // First, use all the cores
    let permit1 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    let ret = tokio::time::timeout(Duration::from_secs(1), queue.register_search()).await;
    assert!(ret.is_err(), "The capacity is full, we should not get a permit");

    let q = queue.clone();
    let task = tokio::task::spawn(async move { q.register_search().await });

    // after dropping a permit the previous task should be able to finish
    drop(permit1);
    let _permit2 = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}

#[actix_rt::test]
async fn search_queue_refuse_search_requests() {
    let queue = Arc::new(SearchQueue::new(1, 1));

    // First, use the whole capacity of the
    let _permit1 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    let q = queue.clone();
    let permit2 = tokio::task::spawn(async move { q.register_search().await });

    // Here the queue is full. By registering two new search requests the permit 2 and 3 should be thrown out
    let q = queue.clone();
    let _permit3 = tokio::task::spawn(async move { q.register_search().await });

    let permit2 = tokio::time::timeout(Duration::from_secs(1), permit2)
        .await
        .expect("I should get a result straight away")
        .unwrap(); // task should end successfully

    snapshot!(permit2.unwrap_err(), @"Too many search requests running at the same time: 1. Retry after 10s.");
}

#[actix_rt::test]
async fn search_request_crashes_while_holding_permits() {
    let queue = Arc::new(SearchQueue::new(1, 1));

    let (send, recv) = tokio::sync::oneshot::channel();

    // This first request take a cpu
    let q = queue.clone();
    tokio::task::spawn(async move {
        let _permit = q.register_search().await.unwrap();
        recv.await.unwrap();
        panic!("oops an unexpected crash happened")
    });

    // This second request waits in the queue till the first request finishes
    let q = queue.clone();
    let task = tokio::task::spawn(async move {
        let _permit = q.register_search().await.unwrap();
    });

    // By sending something in the channel the request holding a CPU will panic and should lose its permit
    send.send(()).unwrap();

    // Then the second request should be able to process and finishes correctly without panic
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // I should even be able to take second permit here
    let _permit1 = tokio::time::timeout(Duration::from_secs(1), queue.register_search())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}
