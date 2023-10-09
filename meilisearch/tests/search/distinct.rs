use meili_snap::snapshot;
use once_cell::sync::Lazy;
use serde_json::{json, Value};

use crate::common::Server;

pub(self) static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {"productId": 1, "shopId": 1},
        {"productId": 2, "shopId": 1},
        {"productId": 3, "shopId": 2},
        {"productId": 4, "shopId": 2},
        {"productId": 5, "shopId": 3},
        {"productId": 6, "shopId": 3},
        {"productId": 7, "shopId": 4},
        {"productId": 8, "shopId": 4},
        {"productId": 9, "shopId": 5},
        {"productId": 10, "shopId": 5}
    ])
});

pub(self) static DOCUMENT_PRIMARY_KEY: &str = "productId";
pub(self) static DOCUMENT_DISTINCT_KEY: &str = "shopId";

/// testing: https://github.com/meilisearch/meilisearch/issues/4078
#[actix_rt::test]
async fn distinct_search_with_offset_no_ranking() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, Some(DOCUMENT_PRIMARY_KEY)).await;
    index.update_distinct_attribute(json!(DOCUMENT_DISTINCT_KEY)).await;
    index.wait_task(1).await;

    fn get_hits(response: Value) -> Vec<i64> {
        let hits_array = response["hits"].as_array().unwrap();
        hits_array.iter().map(|h| h[DOCUMENT_DISTINCT_KEY].as_i64().unwrap()).collect::<Vec<_>>()
    }

    let (response, code) = index.search_post(json!({"limit": 2, "offset": 0})).await;
    let hits = get_hits(response);
    snapshot!(code, @"200 OK");
    snapshot!(hits.len(), @"2");
    snapshot!(format!("{:?}", hits), @"[1, 2]");

    let (response, code) = index.search_post(json!({"limit": 2, "offset": 2})).await;
    let hits = get_hits(response);
    snapshot!(code, @"200 OK");
    snapshot!(hits.len(), @"2");
    snapshot!(format!("{:?}", hits), @"[3, 4]");

    let (response, code) = index.search_post(json!({"limit": 10, "offset": 4})).await;
    let hits = get_hits(response);
    snapshot!(code, @"200 OK");
    snapshot!(hits.len(), @"1");
    snapshot!(format!("{:?}", hits), @"[5]");

    let (response, code) = index.search_post(json!({"limit": 10, "offset": 5})).await;
    let hits = get_hits(response);
    snapshot!(code, @"200 OK");
    snapshot!(hits.len(), @"0");
}
