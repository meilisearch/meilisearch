use meili_snap::{json_string, snapshot};
use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn set_and_reset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_response, _code) = index
        .update_settings(json!({
            "nonSeparatorTokens": ["#", "&"],
            "separatorTokens": ["&sep", "<br/>"],
            "dictionary": ["J.R.R.", "J. R. R."],
        }))
        .await;
    index.wait_task(0).await;

    let (response, _) = index.settings().await;
    snapshot!(json_string!(response["nonSeparatorTokens"]), @r###"
    [
      "#",
      "&"
    ]
    "###);
    snapshot!(json_string!(response["separatorTokens"]), @r###"
    [
      "&sep",
      "<br/>"
    ]
    "###);
    snapshot!(json_string!(response["dictionary"]), @r###"
    [
      "J. R. R.",
      "J.R.R."
    ]
    "###);

    index
        .update_settings(json!({
            "nonSeparatorTokens": null,
            "separatorTokens": null,
            "dictionary": null,
        }))
        .await;

    index.wait_task(1).await;

    let (response, _) = index.settings().await;
    snapshot!(json_string!(response["nonSeparatorTokens"]), @"[]");
    snapshot!(json_string!(response["separatorTokens"]), @"[]");
    snapshot!(json_string!(response["dictionary"]), @"[]");
}
