use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn set_and_reset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index
        .update_settings(json!({
            "non_separator_tokens": ["#", "&"],
            "separator_tokens": ["&sep", "<br/>"],
            "dictionary": ["J.R.R.", "J. R. R."],
        }))
        .await;
    index.wait_task(0).await;

    let (response, _) = index.settings().await;
    assert_eq!(response["non_separator_tokens"], json!(["#", "&"]));
    assert_eq!(response["separator_tokens"], json!(["&sep", "<br/>"]));
    assert_eq!(response["dictionary"], json!(["J.R.R.", "J. R. R."]));

    index
        .update_settings(json!({
            "non_separator_tokens": null,
            "separator_tokens": null,
            "dictionary": null,
        }))
        .await;

    index.wait_task(1).await;

    let (response, _) = index.settings().await;
    assert_eq!(response["non_separator_tokens"], json!(null));
    assert_eq!(response["separator_tokens"], json!(null));
    assert_eq!(response["dictionary"], json!(null));
}
