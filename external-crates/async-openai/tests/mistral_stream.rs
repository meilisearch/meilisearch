use async_openai::types::CreateChatCompletionStreamResponse;

#[test]
fn mistral_reference_and_text_parts_deserialize() {
    // This payload mirrors the chunks returned by Mistral when using
    // JSON / JSON-Schema response formats, where `delta.content` is
    // an array containing a `reference` part followed by a `text` part.
    let json = r#"{
      "id":"fbf4551b8c3444c7a2da5995673b6543",
      "object":"chat.completion.chunk",
      "created":1770133877,
      "model":"mistral-large-latest",
      "choices":[{
        "index":0,
        "delta":{
          "content":[
            {"type":"reference","reference_ids":[]},
            {"type":"text","text":"{\"index"}
          ]
        },
        "finish_reason":null
      }]
    }"#;

    let parsed: CreateChatCompletionStreamResponse = serde_json::from_str(json).unwrap();
    let delta = &parsed.choices[0].delta;
    assert_eq!(delta.content.as_deref(), Some("{\"index"));
}
