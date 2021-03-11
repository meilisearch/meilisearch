use crate::common::Server;
use serde_json::json;

#[actix_rt::test]
async fn get_settings_unexisting_index() {
    let server = Server::new().await;
    let (_response, code) = server.index("test").settings().await;
    assert_eq!(code, 400)
}

#[actix_rt::test]
async fn get_settings() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    let settings = response.as_object().unwrap();
    assert_eq!(settings.keys().len(), 4);
    assert_eq!(settings["displayedAttributes"], json!(["*"]));
    assert_eq!(settings["searchableAttributes"], json!(["*"]));
    assert_eq!(settings["facetedAttributes"], json!({}));
    assert_eq!(settings["rankingRules"], json!(["typo", "words", "proximity", "attribute", "wordsPosition", "exactness"]));
}

#[actix_rt::test]
async fn update_settings_unknown_field() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.update_settings(json!({"foo": 12})).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn test_partial_update() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, _code) = index.update_settings(json!({"displayedAttributes": ["foo"]})).await;
    println!("response: {}", response);
    index.wait_update_id(0).await;
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"],json!(["foo"]));
    assert_eq!(response["searchableAttributes"],json!(["*"]));

    index.update_settings(json!({"searchableAttributes": ["bar"]})).await;
    index.wait_update_id(1).await;

    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"],json!(["foo"]));
    assert_eq!(response["searchableAttributes"],json!(["bar"]));
}

#[actix_rt::test]
async fn delete_settings_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.delete_settings().await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn reset_all_settings() {
    let server = Server::new().await;
    let index = server.index("test");
    index.update_settings(json!({"displayedAttributes": ["foo"], "searchableAttributes": ["bar"]})).await;
    index.wait_update_id(0).await;
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"],json!(["foo"]));
    assert_eq!(response["searchableAttributes"],json!(["bar"]));

    index.delete_settings().await;
    index.wait_update_id(1).await;

    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"],json!(["*"]));
    assert_eq!(response["searchableAttributes"],json!(["*"]));
}

#[actix_rt::test]
async fn update_setting_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.update_settings(json!({})).await;
    assert_eq!(code, 200);
    let (_response, code) = index.get().await;
    assert_eq!(code, 200);
}

#[actix_rt::test]
async fn update_setting_unexisting_index_invalid_uid() {
    let server = Server::new().await;
    let index = server.index("test##!  ");
    let (_response, code) = index.update_settings(json!({})).await;
    println!("response: {}", _response);
    assert_eq!(code, 400);
}

macro_rules! test_setting_routes {
    ($($setting:ident), *) => {
        $(
            mod $setting {
                use crate::common::Server;

                #[actix_rt::test]
                async fn get_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_response, code) = server.service.get(url).await;
                    assert_eq!(code, 400);
                }

                #[actix_rt::test]
                async fn update_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_response, code) = server.service.post(url, serde_json::Value::Null).await;
                    assert_eq!(code, 200);
                    let (_response, code) = server.index("test").get().await;
                    assert_eq!(code, 200);
                }

                #[actix_rt::test]
                async fn delete_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_response, code) = server.service.delete(url).await;
                    assert_eq!(code, 400);
                }
            }
        )*
    };
}

test_setting_routes!(
    attributes_for_faceting,
    displayed_attributes,
    searchable_attributes);
