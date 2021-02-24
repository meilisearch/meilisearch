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
    assert_eq!(settings.keys().len(), 3);
    assert_eq!(settings["displayedAttributes"], json!(["*"]));
    assert_eq!(settings["searchableAttributes"], json!(["*"]));
    assert_eq!(settings["facetedAttributes"], json!({}));
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
                #[ignore]
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
