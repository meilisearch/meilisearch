use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use actix_web::ResponseError;
use meili_snap::snapshot;
use meilisearch::search_queue::SearchQueue;

#[actix_rt::test]
async fn search_queue_register() {
    let queue = SearchQueue::new(4, NonZeroUsize::new(2).unwrap());

    // First, use all the cores
    let permit1 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
    let _permit2 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // If we free one spot we should be able to register one new search
    drop(permit1);

    let permit3 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // And again
    drop(permit3);

    let _permit4 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}

#[actix_rt::test]
async fn search_queue_register_with_explicit_drop() {
    let queue = SearchQueue::new(4, NonZeroUsize::new(2).unwrap());

    // First, use all the cores
    let permit1 = queue.try_get_search_permit().await.unwrap();
    let _permit2 = queue.try_get_search_permit().await.unwrap();

    // If we free one spot we should be able to register one new search
    permit1.drop().await;

    let permit3 = queue.try_get_search_permit().await.unwrap();

    // And again
    permit3.drop().await;

    let _permit4 = queue.try_get_search_permit().await.unwrap();
}

#[actix_rt::test]
async fn search_queue_register_with_time_to_abort() {
    let queue = Arc::new(
        SearchQueue::new(1, NonZeroUsize::new(1).unwrap())
            .with_time_to_abort(Duration::from_secs(1)),
    );

    // First, use all the cores
    let permit1 = queue.try_get_search_permit().await.unwrap();
    let q = queue.clone();
    let permit2 = tokio::task::spawn(async move { q.try_get_search_permit().await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    permit1.drop().await;
    let ret = permit2.await.unwrap();

    snapshot!(ret.unwrap_err(), @"Too many search requests running at the same time: 1. Retry after 10s.");
}

#[actix_rt::test]
async fn wait_till_cores_are_available() {
    let queue = Arc::new(SearchQueue::new(4, NonZeroUsize::new(1).unwrap()));

    // First, use all the cores
    let permit1 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    let ret = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit()).await;
    assert!(ret.is_err(), "The capacity is full, we should not get a permit");

    let q = queue.clone();
    let task = tokio::task::spawn(async move { q.try_get_search_permit().await });

    // after dropping a permit the previous task should be able to finish
    drop(permit1);
    let _permit2 = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}

#[actix_rt::test]
async fn refuse_search_requests_when_queue_is_full() {
    let queue = Arc::new(SearchQueue::new(1, NonZeroUsize::new(1).unwrap()));

    // First, use the whole capacity of the
    let _permit1 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    let q = queue.clone();
    let permit2 = tokio::task::spawn(async move { q.try_get_search_permit().await });

    // Here the queue is full. By registering two new search requests the permit 2 and 3 should be thrown out
    let q = queue.clone();
    let _permit3 = tokio::task::spawn(async move { q.try_get_search_permit().await });

    let permit2 = tokio::time::timeout(Duration::from_secs(1), permit2)
        .await
        .expect("I should get a result straight away")
        .unwrap(); // task should end successfully

    let err = meilisearch_types::error::ResponseError::from(permit2.unwrap_err());
    let http_response = err.error_response();
    let mut headers: Vec<_> = http_response
        .headers()
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_string()))
        .collect();
    headers.sort();
    snapshot!(format!("{headers:?}"), @r###"[("content-type", "application/json"), ("retry-after", "10")]"###);

    let err = serde_json::to_string_pretty(&err).unwrap();
    snapshot!(err, @r###"
    {
      "message": "Too many search requests running at the same time: 1. Retry after 10s.",
      "code": "too_many_search_requests",
      "type": "system",
      "link": "https://docs.meilisearch.com/errors#too_many_search_requests"
    }
    "###);
}

#[actix_rt::test]
async fn search_request_crashes_while_holding_permits() {
    let queue = Arc::new(SearchQueue::new(1, NonZeroUsize::new(1).unwrap()));

    let (send, recv) = tokio::sync::oneshot::channel();

    // This first request take a cpu
    let q = queue.clone();
    tokio::task::spawn(async move {
        let _permit = q.try_get_search_permit().await.unwrap();
        recv.await.unwrap();
        panic!("oops an unexpected crash happened")
    });

    // This second request waits in the queue till the first request finishes
    let q = queue.clone();
    let task = tokio::task::spawn(async move {
        let _permit = q.try_get_search_permit().await.unwrap();
    });

    // By sending something in the channel the request holding a CPU will panic and should lose its permit
    send.send(()).unwrap();

    // Then the second request should be able to process and finishes correctly without panic
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // I should even be able to take second permit here
    let _permit1 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}

#[actix_rt::test]
async fn works_with_capacity_of_zero() {
    let queue = Arc::new(SearchQueue::new(0, NonZeroUsize::new(1).unwrap()));

    // First, use the whole capacity of the
    let permit1 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();

    // then we should get an error if we try to register a second search request.
    let permit2 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a result straight away");

    let err = meilisearch_types::error::ResponseError::from(permit2.unwrap_err());
    let http_response = err.error_response();
    let mut headers: Vec<_> = http_response
        .headers()
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_string()))
        .collect();
    headers.sort();
    snapshot!(format!("{headers:?}"), @r###"[("content-type", "application/json"), ("retry-after", "10")]"###);

    let err = serde_json::to_string_pretty(&err).unwrap();
    snapshot!(err, @r###"
    {
      "message": "Too many search requests running at the same time: 0. Retry after 10s.",
      "code": "too_many_search_requests",
      "type": "system",
      "link": "https://docs.meilisearch.com/errors#too_many_search_requests"
    }
    "###);

    drop(permit1);
    // After dropping the first permit we should be able to get a new permit
    let _permit3 = tokio::time::timeout(Duration::from_secs(1), queue.try_get_search_permit())
        .await
        .expect("I should get a permit straight away")
        .unwrap();
}
