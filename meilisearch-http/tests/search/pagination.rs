use crate::common::Server;
use crate::search::DOCUMENTS;
use serde_json::json;

#[actix_rt::test]
async fn default_search_should_return_estimated_total_hit() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert!(response.get("estimatedTotalHits").is_some());
            assert!(response.get("limit").is_some());
            assert!(response.get("offset").is_some());

            // these fields shouldn't be present
            assert!(response.get("totalHits").is_none());
            assert!(response.get("page").is_none());
            assert!(response.get("totalPages").is_none());
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"page": 1}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            assert!(response.get("totalHits").is_some());
            assert_eq!(response["page"], 1);
            assert_eq!(response["totalPages"], 1);

            // these fields shouldn't be present
            assert!(response.get("estimatedTotalHits").is_none());
            assert!(response.get("limit").is_none());
            assert!(response.get("offset").is_none());
        })
        .await;
}

#[actix_rt::test]
async fn page_zero_should_not_return_any_result() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"page": 0}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 0);
            assert!(response.get("totalHits").is_some());
            assert_eq!(response["page"], 0);
            assert_eq!(response["totalPages"], 1);
        })
        .await;
}

#[actix_rt::test]
async fn hits_per_page_1() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"hitsPerPage": 1}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            assert_eq!(response["totalHits"], 5);
            assert_eq!(response["page"], 1);
            assert_eq!(response["totalPages"], 5);
        })
        .await;
}

#[actix_rt::test]
async fn hits_per_page_0_should_not_return_any_result() {
    let server = Server::new().await;
    let index = server.index("basic");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    index
        .search(json!({"hitsPerPage": 0}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 0);
            assert_eq!(response["totalHits"], 5);
            assert_eq!(response["page"], 1);
            assert_eq!(response["totalPages"], 0);
        })
        .await;
}
