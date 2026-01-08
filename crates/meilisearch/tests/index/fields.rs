use crate::common::index::{ListFieldsFilterPayload, ListFieldsPayload};
use crate::common::{shared_does_not_exists_index, Server};
use crate::json;

#[actix_rt::test]
async fn error_get_fields_unexisting_index() {
    let index = shared_does_not_exists_index().await;
    let (response, code) = index.fields(&ListFieldsPayload::default()).await;
    insta::assert_json_snapshot!((code.as_u16(), response));
}

#[actix_rt::test]
async fn get_fields_empty_index() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(None).await;
    insta::assert_snapshot!(code);
    server.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.fields(&ListFieldsPayload::default()).await;
    insta::assert_json_snapshot!((code.as_u16(), response));
}

#[actix_rt::test]
async fn get_fields_with_documents_and_filters() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // create index and add a few documents
    index.create(None).await; // 202 accepted
    let docs = json!([
        { "id": 1, "title": "Titanic", "genre": "drama" },
        { "id": 2, "title": "Shazam!", "color": "blue" }
    ]);
    let (task, code) = index.add_documents(docs, Some("id")).await;
    insta::assert_json_snapshot!((code.as_u16(), &task), {
        "[1].taskUid" => "[uid]",
        "[1].enqueuedAt" => "[date]",
        "[1].indexUid" => "[uid]",
    });
    server.wait_task(task.uid()).await.succeeded();

    // fetch fields without any param
    let (response, code) = index.fields(&ListFieldsPayload::default()).await;

    insta::assert_json_snapshot!((code.as_u16(), response), {
        "[1]" => insta::sorted_redaction(),
    });

    // search parameter should filter
    let (response, code) = index
        .fields(&ListFieldsPayload {
            filter: Some(ListFieldsFilterPayload { starts_with: Some("ti"), ..Default::default() }),
            ..Default::default()
        })
        .await;

    insta::assert_json_snapshot!((code.as_u16(), response), {
        "[1]" => insta::sorted_redaction(),
    });
}

#[actix_rt::test]
async fn fields_after_uploading_recipe_and_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // create index with primary key `id`
    let (task, code) = index.create(Some("id")).await;
    insta::assert_json_snapshot!((code.as_u16(), &task), {
        "[1].taskUid" => "[uid]",
        "[1].enqueuedAt" => "[date]",
        "[1].indexUid" => "[uid]",
    });
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
    insta::assert_json_snapshot!((code.as_u16(), &task), {
        "[1].taskUid" => "[uid]",
        "[1].enqueuedAt" => "[date]",
        "[1].indexUid" => "[uid]",
    });
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
    insta::assert_json_snapshot!((code.as_u16(), &task), {
        "[1].taskUid" => "[uid]",
        "[1].enqueuedAt" => "[date]",
        "[1].indexUid" => "[uid]",
    });
    server.wait_task(task.uid()).await.succeeded();

    // fetch fields - large limit to get all
    let (resp, code) = index.fields(&ListFieldsPayload { limit: 500, ..Default::default() }).await;
    insta::assert_json_snapshot!((code.as_u16(), resp), {
        "[1]" => insta::sorted_redaction(),
    });
}
