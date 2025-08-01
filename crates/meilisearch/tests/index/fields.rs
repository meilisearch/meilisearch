use crate::common::{Server, shared_does_not_exists_index};
use crate::json;

#[actix_rt::test]
async fn error_get_fields_unexisting_index() {
    let index = shared_does_not_exists_index().await;
    let (response, code) = index.fields().await;
    assert_eq!(code, 404, "{response}");
    let expected = json!({
        "message": "Index `DOES_NOT_EXISTS` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });
    assert_eq!(response, expected);
}

#[actix_rt::test]
async fn get_fields_empty_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.fields().await;
    assert_eq!(code, 200, "{response}");
    assert_eq!(response["total"], json!(0));
    assert!(response["results"].as_array().unwrap().is_empty());
}

#[actix_rt::test]
async fn get_fields_with_documents_and_search() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // create index and add a few documents
    index.create(None).await; // 202 accepted
    let docs = json!([
        { "id": 1, "title": "Titanic", "genre": "drama" },
        { "id": 2, "title": "Shazam!", "color": "blue" }
    ]);
    let (task, code) = index.add_documents(docs, Some("id")).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    // fetch fields without any param
    let (response, code) = index.fields().await;
    assert_eq!(code, 200);
    assert!(response["total"].as_u64().unwrap() >= 3);

    // ensure `id` appears in results
    let names: Vec<_> = response["results"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"id"));

    // search parameter should filter
    let url = format!("/indexes/{}/fields?search=ti*", index.uid);
    let (search_resp, code) = server.service.get(url).await;
    assert_eq!(code, 200);
    let sr_names: Vec<_> = search_resp["results"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
    assert_eq!(sr_names, vec!["title"]);
} 

#[actix_rt::test]
async fn fields_after_uploading_recipe_and_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // create index with primary key `id`
    let (task, code) = index.create(Some("id")).await;
    assert_eq!(code, 202, "{task}");
    server.wait_task(task.uid()).await.succeeded();

    // upload recipe document
    let doc = json!([
        {
            "id": 1,
            "title": "Spaghetti Carbonara",
            "description": "A classic Italian pasta dish with eggs, cheese, pancetta, and black pepper.",
            "difficulty": "medium",
            "preparation_time_minutes": 15,
            "cooking_time_minutes": 20,
            "total_time_minutes": 35,
            "servings": 4,
            "calories_per_serving": 450,
            "cuisine": {
                "type": "Italian",
                "region": "Rome",
                "is_authentic": true
            },
            "ingredients": [{"name": "spaghetti"}],
            "instructions": [{"step": 1, "description": "Bring water to boil"}],
            "tags": ["pasta", "italian"],
            "nutrition": {"calories": 450, "protein": 20, "carbs": 45, "fat": 20},
            "is_vegetarian": false,
            "is_vegan": false,
            "is_gluten_free": false,
            "is_dairy_free": false,
            "created_at": "2023-01-10T14:30:00Z",
            "updated_at": "2023-06-15T09:15:30Z",
            "image_url": "https://example.com/images/spaghetti-carbonara.jpg"
        }
    ]);
    let (task, code) = index.add_documents(doc, Some("id")).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // upload settings
    let settings = json!({
        "displayedAttributes": [
            "id", "title", "description", "difficulty",
            "preparation_time_minutes", "cooking_time_minutes", "total_time_minutes",
            "servings", "calories_per_serving", "cuisine", "tags", "nutrition",
            "is_vegetarian", "is_vegan", "is_gluten_free", "is_dairy_free", "image_url"
        ],
        "searchableAttributes": [
            "title", "tags", "description", "ingredients.name",
            "cuisine.type", "cuisine.region", "instructions.description"
        ],
        "filterableAttributes": [
            "difficulty", "preparation_time_minutes", "cooking_time_minutes",
            "total_time_minutes", "servings", "calories_per_serving", "cuisine.type",
            "cuisine.region", "cuisine.is_authentic", "ingredients.name", "tags",
            "nutrition.calories", "nutrition.protein", "nutrition.carbs", "nutrition.fat",
            "nutrition.fiber", "is_vegetarian", "is_vegan", "is_gluten_free", "is_dairy_free",
            "reviews.rating"
        ],
        "sortableAttributes": [
            "preparation_time_minutes", "cooking_time_minutes", "total_time_minutes",
            "calories_per_serving", "servings", "created_at", "nutrition.calories",
            "nutrition.protein", "nutrition.carbs", "nutrition.fat"
        ]
    });
    let (task, code) = index.update_settings(settings).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // fetch fields - large limit to get all
    let url = format!("/indexes/{}/fields?limit=500", index.uid);
    let (resp, code) = server.service.get(url).await;
    assert_eq!(code, 200);

    // helper closure to find field by name
    let find = |name: &str| -> Option<&serde_json::Value> {
        resp["results"].as_array().unwrap().iter().find(|f| f["name"] == name)
    };

    // title: displayed & searchable true, filterable false
    let title = find("title").expect("title field");
    assert!(title["displayed"]["enabled"].as_bool().unwrap());
    assert!(title["searchable"]["enabled"].as_bool().unwrap());
    assert!(!title["filterable"]["enabled"].as_bool().unwrap());

    // cuisine.type should be filterable & searchable true
    let cuisine_type = find("cuisine.type").expect("cuisine.type field");
    assert!(cuisine_type["filterable"]["enabled"].as_bool().unwrap());
    assert!(cuisine_type["searchable"]["enabled"].as_bool().unwrap());

    // nutrition.calories sortable, so filterable enabled but displayed maybe true by list.
    let calories = find("nutrition.calories").expect("nutrition.calories field");
    assert!(calories["filterable"]["enabled"].as_bool().unwrap());

    // check total fields count > 20
    assert!(resp["total"].as_u64().unwrap() > 20);
} 