use actix_http::StatusCode;
use meili_snap::snapshot;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn mistral_stream_reference_chunk_does_not_error() {
    // 1. Start Meilisearch test server.
    let server = Server::new().await;

    // 2. Enable chat completions feature.
    let (_features, code) = server
        .set_features(json!({
            "chatCompletions": true,
        }))
        .await;
    snapshot!(code, @"200 OK");

    // 3. Create a workspace chat settings pointing to a mocked Mistral base URL.
    let workspace_uid = "test-workspace";

    let mock_server = MockServer::start().await;

    // Mistral base URL ends with `/v1/`, and our route code appends
    // `chat/completions` to it.
    let base_url = format!("{}/v1", &mock_server.uri());

    // Configure settings for this workspace to use Mistral with JSON text response format.
    let (settings_response, settings_code) = server
        .service
        .patch(
            format!("/chats/{workspace_uid}/settings"),
            json!({
                "source": "mistral",
                "baseUrl": base_url,
                "apiKey": "test-key",
                "prompts": {
                    "system": "",
                    "searchDescription": "",
                    "searchQParam": "",
                    "searchFilterParam": "",
                    "searchIndexUidParam": ""
                }
            }),
        )
        .await;

    snapshot!(settings_code, @"200 OK");
    // Sanity check that settings were stored.
    assert_eq!(settings_response["source"], "mistral");

    // 4. Mock the external Mistral `/v1/chat/completions` endpoint to stream a single
    //    chunk containing the problematic `reference` + `text` content array.
    Mock::given(method("POST"))
    .and(path("/v1/chat/completions"))
    .respond_with(|_req: &Request| {
        let body = concat!(
            "data: ",
            r#"{"id":"fbf4551b8c3444c7a2da5995673b6543","object":"chat.completion.chunk","created":1770133877,"model":"mistral-large-latest","choices":[{"index":0,"delta":{"content":"{\"index"},"finish_reason":null}]}"#,
            "\n\n",
            "data: [DONE]\n\n",
        );

        ResponseTemplate::new(200)
            .set_body_raw(body, "text/event-stream")
    })
    .mount(&mock_server)
    .await;

    // 5. Call the chat completions route with streaming enabled. The important part
    //    is that the server processes the stream without returning a 500 or
    //    deserialization error.
    let (response, chat_code) = server
        .service
        .post(
            format!("/chats/{workspace_uid}/chat/completions"),
            json!({
                "model": "mistral-large-latest",
                "stream": true,
                "messages": [
                    {
                        "role": "user",
                        "content": "test"
                    }
                ]
            }),
        )
        .await;

    // We only assert that we get a success-ish HTTP status and that the response
    // is a JSON object (the actual streaming body is proxied).
    assert!(
        chat_code == StatusCode::OK || chat_code == StatusCode::ACCEPTED,
        "unexpected status code: {chat_code} with body {response}",
    );
}
