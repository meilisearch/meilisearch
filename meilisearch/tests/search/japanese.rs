use crate::common::Server;
use serde_json::json;

async fn setup() -> Server {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        { "id": 0, "title": "東京医科歯科大学" },
        { "id": 1, "title": "東京のお寿司。" },
        { "id": 2, "title": "東京オペラシティ" },
        { "id": 3, "title": "東京スカイツリー" },
        { "id": 4, "title": "東京時代まつり" },
        { "id": 5, "title": "アッー!!" }
    ]);

    index.add_documents(documents, None).await;
    index.wait_task(0).await;

    server
}

async fn test_search(server: &Server, query: &str, expected_hits: usize) {
    let index = server.index("test");
    index
        .search(json!({ "q": query }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), expected_hits);
        })
        .await;
}

#[actix_rt::test]
async fn prototype_test_search_japanese_empty() {
    // Index check for "ッー" (ref: https://github.com/meilisearch/meilisearch/pull/3588#issuecomment-1529169147)
    let server = setup().await;
    test_search(&server, "", 6).await;
}

#[actix_rt::test]
async fn prototype_test_search_japanese_tokyo() {
    // Test for kanji
    // If unforce Japanese, the number of hits will be 1
    let server = setup().await;
    test_search(&server, "東京", 5).await;
}

#[actix_rt::test]
async fn prototype_test_search_japanese_katakana() {
    // Test for katakana
    // Test that it is possible to search by katakana or hiragana by wana_kana
    let server = setup().await;
    test_search(&server, "オペラ", 1).await;
    test_search(&server, "おぺら", 1).await;
}

#[actix_rt::test]
async fn prototype_test_search_japanese_hiragana() {
    // Test for hiragana
    // Test that it is possible to search by katakana or hiragana by wana_kana
    let server = setup().await;
    test_search(&server, "まつり", 1).await;
    test_search(&server, "マツリ", 1).await;
}
