use crate::common::Server;
use serde_json::json;

/// Regression test for https://github.com/meilisearch/meilisearch/issues/1495
/// The crux of the bug is in changing order of fields in serialized document
/// after changing index's `searchableAttributes` setting.
#[actix_rt::test]
async fn update_searchable_attributes() {
    let server = Server::new().await;
    let index_uid = "test";
    let index = server.index(index_uid);
    let (_, code) = index.create(None).await;
    assert_eq!(code, 202);
    index.wait_task(0).await;

    let doc = json!([
        {
            "id": 1,
            "title": "Apocalypse Now",
            "overview": "At the height of the Vietnam war, Captain Benjamin Willard is sent on a dangerous mission that, officially, \"does not exist, nor will it ever exist.\" His goal is to locate - and eliminate - a mysterious Green Beret Colonel named Walter Kurtz, who has been leading his personal army on illegal guerrilla missions into enemy territory.",
            "genres": [
                "Drama",
                "War"
            ],
            "poster": "https://image.tmdb.org/t/p/w500/gQB8Y5RCMkv2zwzFHbUJX3kAhvA.jpg",
            "release_date": 303523200
        }
    ]);

    index.add_documents(doc, None).await;
    index.wait_task(1).await;

    let (resp, code) = index.search_post(json!({"q": "apocalypse"})).await;
    assert_eq!(code, 200);
    let doc = &resp["hits"].as_array().unwrap()[0];
    let keys = doc.as_object().unwrap().keys().collect::<Vec<_>>();
    assert_eq!(doc["title"], "Apocalypse Now");
    assert_eq!(
        keys,
        vec![
            "id",
            "title",
            "overview",
            "genres",
            "poster",
            "release_date"
        ]
    );

    let settings = json!({
        "searchableAttributes": [
            "title",
            "description"
        ],
    });
    let (_, code) = index.update_settings(settings).await;
    assert_eq!(code, 202);
    index.wait_task(2).await;

    let (resp, code) = index.search_post(json!({"q": "apocalypse"})).await;
    assert_eq!(code, 200);
    let doc = &resp["hits"].as_array().unwrap()[0];
    let keys = doc.as_object().unwrap().keys().collect::<Vec<_>>();
    assert_eq!(doc["title"], "Apocalypse Now");
    assert_eq!(
        keys,
        vec![
            "id",
            "title",
            "overview",
            "genres",
            "poster",
            "release_date"
        ]
    );
}
