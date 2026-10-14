use meili_snap::{json_string, snapshot};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::common::{Server, Value};
use crate::json;

/// Builds the SSE body of a streamed chat completion: one chunk per delta, then the
/// empty chunk carrying the finish reason, as OpenAI streams it.
fn sse(deltas: &[Value], finish_reason: &str) -> String {
    let chunk = |delta: &Value, finish_reason: Option<&str>| {
        let chunk = json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "created": 0,
            "model": "mock",
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish_reason }],
        });
        format!("data: {}\n\n", serde_json::to_string(&chunk).unwrap())
    };
    let mut body: String = deltas.iter().map(|delta| chunk(delta, None)).collect();
    body.push_str(&chunk(&json!({}), Some(finish_reason)));
    body.push_str("data: [DONE]\n\n");
    body
}

fn tool_call_deltas(index: u32, id: &str, name: &str, arguments: &str) -> Vec<Value> {
    vec![
        json!({"tool_calls": [{"index": index, "id": id, "type": "function", "function": {"name": name, "arguments": ""}}]}),
        json!({"tool_calls": [{"index": index, "function": {"arguments": arguments}}]}),
    ]
}

/// Mounts an LLM that answers the first request with the given deltas ending in tool calls,
/// and every following request with a plain text answer.
async fn mount_llm(mock_server: &MockServer, mut first_turn: Vec<Value>) {
    first_turn.insert(0, json!({"role": "assistant", "content": ""}));
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(sse(&first_turn, "tool_calls"), "text/event-stream"),
        )
        .up_to_n_times(1)
        .mount(mock_server)
        .await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            sse(
                &[
                    json!({"role": "assistant", "content": ""}),
                    json!({"content": "Here is the answer."}),
                ],
                "stop",
            ),
            "text/event-stream",
        ))
        .mount(mock_server)
        .await;
}

/// Runs one streamed chat completion against the mocked LLM and returns the messages
/// Meilisearch sent to the LLM on the second request.
async fn second_turn_messages(mock_server: &MockServer) -> Value {
    // The chat completions route needs an API key to search on behalf of the user.
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let (_, code) = server.set_features(json!({"chatCompletions": true})).await;
    snapshot!(code, @"200 OK");

    let index = server.index("pages");
    let (task, _) = index
        .add_documents(
            json!([
                {"id": 1, "title": "Scholarships in Baden-Württemberg"},
                {"id": 2, "title": "How to apply for a Stipendium"},
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, _) = index
        .update_settings_chat(json!({
            "description": "Pages of a website about scholarships",
            "documentTemplate": "{{ doc.title }}",
            "documentTemplateMaxBytes": 400,
            "searchParameters": { "limit": 3 },
        }))
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (_, code) = server
        .service
        .patch(
            "/chats/scholarships/settings",
            json!({
                "source": "openAi",
                "baseUrl": mock_server.uri(),
                "apiKey": "mock-key",
                "prompts": { "system": "Answer with the search function." },
            }),
        )
        .await;
    snapshot!(code, @"200 OK");

    let (_, code) = server
        .service
        .post(
            "/chats/scholarships/chat/completions",
            json!({
                "model": "gpt-mock",
                "stream": true,
                "messages": [{"role": "user", "content": "Any scholarships?"}],
            }),
        )
        .await;
    snapshot!(code, @"200 OK");

    let requests = mock_server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        2,
        "the LLM must be called once for the tool calls and once for the answer"
    );
    let request: Value = requests[1].body_json::<serde_json::Value>().unwrap().into();
    request["messages"].clone().into()
}

#[actix_rt::test]
async fn parallel_tool_use_runs_every_wrapped_search() {
    let mock_server = MockServer::start().await;
    // OpenAI models sometimes bundle several calls into one `multi_tool_use.parallel` call.
    let arguments = json!({
        "tool_uses": [
            {"recipient_name": "functions._meiliSearchInIndex", "parameters": {"index_uid": "pages", "q": "scholarships", "filter": ""}},
            {"recipient_name": "functions._meiliSearchInIndex", "parameters": {"index_uid": "pages", "q": "Stipendium", "filter": ""}},
        ]
    });
    let deltas = tool_call_deltas(0, "call_1", "multi_tool_use.parallel", &arguments.to_string());
    mount_llm(&mock_server, deltas).await;

    let messages = second_turn_messages(&mock_server).await;
    snapshot!(json_string!(messages), @r#"
    [
      {
        "role": "developer",
        "content": "Answer with the search function."
      },
      {
        "role": "user",
        "content": "Any scholarships?"
      },
      {
        "role": "assistant",
        "tool_calls": [
          {
            "id": "call_1_0",
            "type": "function",
            "function": {
              "name": "_meiliSearchInIndex",
              "arguments": "{\"index_uid\":\"pages\",\"q\":\"scholarships\",\"filter\":\"\"}"
            }
          },
          {
            "id": "call_1_1",
            "type": "function",
            "function": {
              "name": "_meiliSearchInIndex",
              "arguments": "{\"index_uid\":\"pages\",\"q\":\"Stipendium\",\"filter\":\"\"}"
            }
          }
        ]
      },
      {
        "role": "tool",
        "content": "Scholarships in Baden-Württemberg",
        "tool_call_id": "call_1_0"
      },
      {
        "role": "tool",
        "content": "How to apply for a Stipendium",
        "tool_call_id": "call_1_1"
      }
    ]
    "#);
}

#[actix_rt::test]
async fn unknown_function_calls_are_answered() {
    let mock_server = MockServer::start().await;
    let deltas = tool_call_deltas(0, "call_1", "fetch_weather", r#"{"city":"Paris"}"#);
    mount_llm(&mock_server, deltas).await;

    let messages = second_turn_messages(&mock_server).await;
    snapshot!(json_string!(messages), @r#"
    [
      {
        "role": "developer",
        "content": "Answer with the search function."
      },
      {
        "role": "user",
        "content": "Any scholarships?"
      },
      {
        "role": "assistant",
        "tool_calls": [
          {
            "id": "call_1",
            "type": "function",
            "function": {
              "name": "fetch_weather",
              "arguments": "{\"city\":\"Paris\"}"
            }
          }
        ]
      },
      {
        "role": "tool",
        "content": "fetch_weather is not a function you can call, use _meiliSearchInIndex instead.",
        "tool_call_id": "call_1"
      }
    ]
    "#);
}
