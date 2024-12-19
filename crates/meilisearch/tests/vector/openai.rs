use std::collections::BTreeMap;
use std::io::Write;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use meili_snap::{json_string, snapshot};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::common::{GetAllDocumentsOptions, Value};
use crate::json;
use crate::vector::get_server_vector;

#[derive(serde::Deserialize)]
struct OpenAiResponses(BTreeMap<String, OpenAiResponse>);

#[derive(serde::Deserialize)]
struct OpenAiResponse {
    large: Option<Vec<f32>>,
    small: Option<Vec<f32>>,
    ada: Option<Vec<f32>>,
    large_512: Option<Vec<f32>>,
}

#[derive(serde::Deserialize)]
struct OpenAiTokenizedResponses {
    tokens: Vec<u64>,
    embedding: Vec<f32>,
}

impl OpenAiResponses {
    fn get(&self, text: &str, model_dimensions: ModelDimensions) -> Option<&[f32]> {
        let entry = self.0.get(text)?;
        match model_dimensions {
            ModelDimensions::Large => entry.large.as_deref(),
            ModelDimensions::Small => entry.small.as_deref(),
            ModelDimensions::Ada => entry.ada.as_deref(),
            ModelDimensions::Large512 => entry.large_512.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelDimensions {
    Large,
    Small,
    Ada,
    Large512,
}

impl ModelDimensions {
    fn add_to_settings(&self, settings: &mut Value) {
        settings["model"] = serde_json::json!(self.model());
        if let ModelDimensions::Large512 = self {
            settings["dimensions"] = serde_json::json!(512);
        }
    }

    fn model(&self) -> &'static str {
        match self {
            ModelDimensions::Large | ModelDimensions::Large512 => "text-embedding-3-large",
            ModelDimensions::Small => "text-embedding-3-small",
            ModelDimensions::Ada => "text-embedding-ada-002",
        }
    }

    fn from_request(request: &serde_json::Value) -> Self {
        let has_dimensions_512 = if let Some(dimensions) = request.get("dimensions") {
            if dimensions != 512 {
                panic!("unsupported dimensions values")
            }
            true
        } else {
            false
        };
        let serde_json::Value::String(model) = &request["model"] else {
            panic!("unsupported non string model")
        };
        match (model.as_str(), has_dimensions_512) {
            ("text-embedding-3-large", true) => Self::Large512,
            (_, true) => panic!("unsupported dimensions with non-large model"),
            ("text-embedding-3-large", false) => Self::Large,
            ("text-embedding-3-small", false) => Self::Small,
            ("text-embedding-ada-002", false) => Self::Ada,
            (_, false) => panic!("unsupported model"),
        }
    }
}

fn openai_responses() -> &'static OpenAiResponses {
    static OPENAI_RESPONSES: OnceLock<OpenAiResponses> = OnceLock::new();
    OPENAI_RESPONSES.get_or_init(|| {
        // json file that was compressed with gzip
        // decompress with `gzip --keep -d openai_responses.json.gz`
        // recompress with `gzip --keep -c openai_responses.json > openai_responses.json.gz`
        let compressed_responses = include_bytes!("openai_responses.json.gz");
        let mut responses = Vec::new();
        let mut decoder = flate2::write::GzDecoder::new(&mut responses);

        decoder.write_all(compressed_responses).unwrap();
        drop(decoder);
        serde_json::from_slice(&responses).unwrap()
    })
}

fn openai_tokenized_responses() -> &'static OpenAiTokenizedResponses {
    static OPENAI_TOKENIZED_RESPONSES: OnceLock<OpenAiTokenizedResponses> = OnceLock::new();
    OPENAI_TOKENIZED_RESPONSES.get_or_init(|| {
        // json file that was compressed with gzip
        // decompress with `gzip --keep -d openai_tokenized_responses.json.gz`
        // recompress with `gzip --keep -c openai_tokenized_responses.json > openai_tokenized_responses.json.gz`
        let compressed_responses = include_bytes!("openai_tokenized_responses.json.gz");
        let mut responses = Vec::new();
        let mut decoder = flate2::write::GzDecoder::new(&mut responses);

        decoder.write_all(compressed_responses).unwrap();
        drop(decoder);
        serde_json::from_slice(&responses).unwrap()
    })
}

fn long_text() -> &'static str {
    static LONG_TEXT: OnceLock<String> = OnceLock::new();
    LONG_TEXT.get_or_init(|| {
        // decompress with `gzip --keep -d intel_gen.txt.gz`
        // recompress with `gzip --keep -c intel_gen.txt > intel_gen.txt.gz`
        let compressed_long_text = include_bytes!("intel_gen.txt.gz");
        let mut long_text = Vec::new();
        let mut decoder = flate2::write::GzDecoder::new(&mut long_text);

        decoder.write_all(compressed_long_text).unwrap();
        drop(decoder);
        let long_text = std::str::from_utf8(&long_text).unwrap();

        long_text.repeat(3)
    })
}

async fn create_mock_tokenized() -> (MockServer, Value) {
    create_mock_with_template("{{doc.text}}", ModelDimensions::Large, false, false).await
}

async fn create_mock_with_template(
    document_template: &str,
    model_dimensions: ModelDimensions,
    fallible: bool,
    slow: bool,
) -> (MockServer, Value) {
    let mock_server = MockServer::start().await;
    const API_KEY: &str = "my-api-key";
    const API_KEY_BEARER: &str = "Bearer my-api-key";

    let attempt = AtomicU32::new(0);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            // 0. wait for a long time
            if slow {
              std::thread::sleep(std::time::Duration::from_secs(1));
            }
            // 1. maybe return 500
            if fallible {
             let attempt = attempt.fetch_add(1, Ordering::Relaxed);
             let failed = matches!(attempt % 4, 0 | 1 | 3);
             if failed {
                return ResponseTemplate::new(503).set_body_json(json!({
                    "error": {
                        "message": "come back later",
                        "type": "come_back_later"
                    }
                }))
             }
            }
            // 2. check API key
            match req.headers.get("Authorization") {
                Some(api_key) if api_key == API_KEY_BEARER => {
                    {}
                }
                Some(api_key) => {
                    let api_key = api_key.to_str().unwrap();
                    return ResponseTemplate::new(401).set_body_json(
                        json!(
                            {
                                "error": {
                                    "message": format!("Incorrect API key provided: {api_key}. You can find your API key at https://platform.openai.com/account/api-keys."),
                                    "type": "invalid_request_error",
                                    "param": serde_json::Value::Null,
                                    "code": "invalid_api_key"
                                }
                            }
                        ),
                    )
                }
                None => {
                    return ResponseTemplate::new(401).set_body_json(
                        json!(
                            {
                                "error": {
                                    "message": "You didn't provide an API key. You need to provide your API key in an Authorization header using Bearer auth (i.e. Authorization: Bearer YOUR_KEY), or as the password field (with blank username) if you're accessing the API from your browser and are prompted for a username and password. You can obtain an API key from https://platform.openai.com/account/api-keys.",
                                    "type": "invalid_request_error",
                                    "param": serde_json::Value::Null,
                                    "code": serde_json::Value::Null
                                }
                            }
                        ),
                    )
                }
            }
            // 3. parse text inputs
            let query: serde_json::Value = match req.body_json() {
                Ok(query) => query,
                Err(_error) => return ResponseTemplate::new(400).set_body_json(
                    json!(
                        {
                            "error": {
                                "message": "We could not parse the JSON body of your request. (HINT: This likely means you aren't using your HTTP library correctly. The OpenAI API expects a JSON payload, but what was sent was not valid JSON. If you have trouble figuring out how to fix this, please contact us through our help center at help.openai.com.)",
                                "type": "invalid_request_error",
                                "param": serde_json::Value::Null,
                                "code": serde_json::Value::Null
                            }
                        }
                    )
                )
            };
            let query_model_dimensions = ModelDimensions::from_request(&query);
            if query_model_dimensions != model_dimensions {
                panic!("Expected {model_dimensions:?}, got {query_model_dimensions:?}")
            }

            // 4. for each text, find embedding in responses
            let serde_json::Value::Array(inputs) = &query["input"] else {
                panic!("Unexpected `input` value")
            };

            let openai_tokenized_responses = openai_tokenized_responses();
            let embeddings = if inputs == openai_tokenized_responses.tokens.as_slice() {
                vec![openai_tokenized_responses.embedding.clone()]
            } else {
                let mut embeddings = Vec::new();
                for input in inputs {
                    let serde_json::Value::String(input) = input else {
                        return ResponseTemplate::new(400).set_body_json(json!({
                            "error": {
                                "message": "Unexpected `input` value",
                                "type": "test_response",
                                "query": query
                            }
                        }))
                    };

                    if input == long_text() {
                        return ResponseTemplate::new(400).set_body_json(json!(
                            {
                                "error": {
                                  "message": "This model's maximum context length is 8192 tokens, however you requested 10554 tokens (10554 in your prompt; 0 for the completion). Please reduce your prompt; or completion length.",
                                  "type": "invalid_request_error",
                                  "param": null,
                                  "code": null,
                                }
                            }
                        ));
                    }

                    let Some(embedding) = openai_responses().get(input, model_dimensions) else {
                        return ResponseTemplate::new(404).set_body_json(json!(
                            {
                                "error": {
                                    "message": "Could not find embedding for text",
                                    "text": input,
                                    "model_dimensions": format!("{model_dimensions:?}"),
                                    "type": "add_to_openai_responses_json_please",
                                    "query": query,
                                }
                            }
                        ))
                    };

                    embeddings.push(embedding.to_vec());
                }
                embeddings
            };


            let data : Vec<_> = embeddings.into_iter().enumerate().map(|(index, embedding)| json!({
                "object": "embedding",
                "index": index,
                "embedding": embedding,
            })).collect();

            // 5. produce output from embeddings
            ResponseTemplate::new(200).set_body_json(json!({
                "object": "list",
                "data": data,
                "model": model_dimensions.model(),
                "usage": {
                    "prompt_tokens": "[prompt_tokens]",
                    "total_tokens": "[total_tokens]"
                }
            }))
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let mut embedder_settings = json!({
        "source": "openAi",
        "url": url,
        "apiKey": API_KEY,
        "documentTemplate": document_template,
        "documentTemplateMaxBytes": 8000000,
    });

    model_dimensions.add_to_settings(&mut embedder_settings);

    (mock_server, embedder_settings)
}

const DOGGO_TEMPLATE: &str = r#"{%- if doc.gender == "F" -%}Une chienne nommée {{doc.name}}, née en {{doc.birthyear}}
        {%- else -%}
        Un chien nommé {{doc.name}}, né en {{doc.birthyear}}
        {%- endif %}, de race {{doc.breed}}."#;

async fn create_mock() -> (MockServer, Value) {
    create_mock_with_template(DOGGO_TEMPLATE, ModelDimensions::Large, false, false).await
}

async fn create_mock_dimensions() -> (MockServer, Value) {
    create_mock_with_template(DOGGO_TEMPLATE, ModelDimensions::Large512, false, false).await
}

async fn create_mock_small_embedding_model() -> (MockServer, Value) {
    create_mock_with_template(DOGGO_TEMPLATE, ModelDimensions::Small, false, false).await
}

async fn create_mock_legacy_embedding_model() -> (MockServer, Value) {
    create_mock_with_template(DOGGO_TEMPLATE, ModelDimensions::Ada, false, false).await
}

async fn create_fallible_mock() -> (MockServer, Value) {
    create_mock_with_template(DOGGO_TEMPLATE, ModelDimensions::Large, true, false).await
}

async fn create_slow_mock() -> (MockServer, Value) {
    create_mock_with_template(DOGGO_TEMPLATE, ModelDimensions::Large, true, true).await
}

// basic test "it works"
#[actix_rt::test]
async fn it_works() {
    let (_mock, setting) = create_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents, {".results.*._vectors.default.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "Intel",
          "gender": "M",
          "birthyear": 2011,
          "breed": "Beagle",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 2,
          "name": "Vénus",
          "gender": "F",
          "birthyear": 2003,
          "breed": "Jack Russel Terrier",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 3,
          "name": "Max",
          "gender": "M",
          "birthyear": 1995,
          "breed": "Labrador Retriever",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"},
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);
}

// tokenize long text

// basic test "it works"
#[actix_rt::test]
async fn tokenize_long_text() {
    let (_mock, setting) = create_mock_tokenized().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "text": long_text()}
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "showRankingScore": true,
            "attributesToRetrieve": ["id"],
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 0,
        "_rankingScore": 0.07944583892822266
      }
    ]
    "###);
}

// "wrong parameters"

#[actix_rt::test]
async fn bad_api_key() {
    let (_mock, mut setting) = create_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;

    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // wrong API key
    setting["apiKey"] = "doggo".into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;

    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "default": {
            "source": "openAi",
            "model": "text-embedding-3-large",
            "apiKey": "XXX...",
            "documentTemplate": "{%- if doc.gender == \"F\" -%}Une chienne nommée {{doc.name}}, née en {{doc.birthyear}}\n        {%- else -%}\n        Un chien nommé {{doc.name}}, né en {{doc.birthyear}}\n        {%- endif %}, de race {{doc.breed}}.",
            "documentTemplateMaxBytes": 8000000,
            "url": "[url]"
          }
        }
      },
      "error": {
        "message": "Index `doggo`: While embedding documents for embedder `default`: user error: could not authenticate against OpenAI server\n  - server replied with `{\"error\":{\"message\":\"Incorrect API key provided: Bearer doggo. You can find your API key at https://platform.openai.com/account/api-keys.\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":\"invalid_api_key\"}}`\n  - Hint: Check the `apiKey` parameter in the embedder configuration, and the `MEILI_OPENAI_API_KEY` and `OPENAI_API_KEY` environment variables",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // no API key
    setting.as_object_mut().unwrap().remove("apiKey");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "default": {
            "source": "openAi",
            "model": "text-embedding-3-large",
            "documentTemplate": "{%- if doc.gender == \"F\" -%}Une chienne nommée {{doc.name}}, née en {{doc.birthyear}}\n        {%- else -%}\n        Un chien nommé {{doc.name}}, né en {{doc.birthyear}}\n        {%- endif %}, de race {{doc.breed}}.",
            "documentTemplateMaxBytes": 8000000,
            "url": "[url]"
          }
        }
      },
      "error": {
        "message": "Index `doggo`: While embedding documents for embedder `default`: user error: could not authenticate against OpenAI server\n  - server replied with `{\"error\":{\"message\":\"You didn't provide an API key. You need to provide your API key in an Authorization header using Bearer auth (i.e. Authorization: Bearer YOUR_KEY), or as the password field (with blank username) if you're accessing the API from your browser and are prompted for a username and password. You can obtain an API key from https://platform.openai.com/account/api-keys.\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":null}}`\n  - Hint: Check the `apiKey` parameter in the embedder configuration, and the `MEILI_OPENAI_API_KEY` and `OPENAI_API_KEY` environment variables",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // not a string API key
    setting["apiKey"] = 42.into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.embedders.default.apiKey`: expected a string, but found a positive integer: `42`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
}

// one test with wrong model
#[actix_rt::test]
async fn bad_model() {
    let (_mock, mut setting) = create_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;

    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // wrong model
    setting["model"] = "doggo".into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");

    snapshot!(response, @r###"
    {
      "message": "`.embedders.default.model`: Invalid model `doggo` for OpenAI. Supported models: [\"text-embedding-ada-002\", \"text-embedding-3-small\", \"text-embedding-3-large\"]",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    // not a string model
    setting["model"] = 42.into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.embedders.default.model`: expected a string, but found a positive integer: `42`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
}

#[actix_rt::test]
async fn bad_dimensions() {
    let (_mock, mut setting) = create_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;

    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // null dimensions
    setting["dimensions"] = 0.into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");

    snapshot!(response, @r###"
    {
      "message": "`.embedders.default.dimensions`: `dimensions` cannot be zero",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    // negative dimensions
    setting["dimensions"] = (-42).into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.embedders.default.dimensions`: expected a positive integer, but found a negative integer: `-42`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    // huge dimensions
    setting["dimensions"] = (42_000_000).into();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.default.dimensions`: Model `text-embedding-3-large` does not support overriding its dimensions to a value higher than 3072. Found 42000000",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
}

// one test with changed dimensions
#[actix_rt::test]
async fn smaller_dimensions() {
    let (_mock, setting) = create_mock_dimensions().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents, {".results.*._vectors.default.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "Intel",
          "gender": "M",
          "birthyear": 2011,
          "breed": "Beagle",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 2,
          "name": "Vénus",
          "gender": "F",
          "birthyear": 2003,
          "breed": "Jack Russel Terrier",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 3,
          "name": "Max",
          "gender": "M",
          "birthyear": 1995,
          "breed": "Labrador Retriever",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);
}

// one test with different models
#[actix_rt::test]
async fn small_embedding_model() {
    let (_mock, setting) = create_mock_small_embedding_model().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents, {".results.*._vectors.default.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "Intel",
          "gender": "M",
          "birthyear": 2011,
          "breed": "Beagle",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 2,
          "name": "Vénus",
          "gender": "F",
          "birthyear": 2003,
          "breed": "Jack Russel Terrier",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 3,
          "name": "Max",
          "gender": "M",
          "birthyear": 1995,
          "breed": "Labrador Retriever",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);
}

#[actix_rt::test]
async fn legacy_embedding_model() {
    let (_mock, setting) = create_mock_legacy_embedding_model().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents, {".results.*._vectors.default.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "Intel",
          "gender": "M",
          "birthyear": 2011,
          "breed": "Beagle",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 2,
          "name": "Vénus",
          "gender": "F",
          "birthyear": 2003,
          "breed": "Jack Russel Terrier",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 3,
          "name": "Max",
          "gender": "M",
          "birthyear": 1995,
          "breed": "Labrador Retriever",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      }
    ]
    "###);
}

// test with a server that responds 500 on 3 out of 4 calls
#[actix_rt::test]
async fn it_still_works() {
    let (_mock, setting) = create_fallible_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
      {"id": 1, "name": "Intel", "gender": "M", "birthyear": 2011, "breed": "Beagle"},
      {"id": 2, "name": "Vénus", "gender": "F", "birthyear": 2003, "breed": "Jack Russel Terrier"},
      {"id": 3, "name": "Max", "gender": "M", "birthyear": 1995, "breed": "Labrador Retriever"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents, {".results.*._vectors.default.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "Intel",
          "gender": "M",
          "birthyear": 2011,
          "breed": "Beagle",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 2,
          "name": "Vénus",
          "gender": "F",
          "birthyear": 2003,
          "breed": "Jack Russel Terrier",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        },
        {
          "id": 3,
          "name": "Max",
          "gender": "M",
          "birthyear": 1995,
          "breed": "Labrador Retriever",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      },
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      },
      {
        "id": 1,
        "name": "Intel",
        "gender": "M",
        "birthyear": 2011,
        "breed": "Beagle"
      },
      {
        "id": 3,
        "name": "Max",
        "gender": "M",
        "birthyear": 1995,
        "breed": "Labrador Retriever"
      },
      {
        "id": 2,
        "name": "Vénus",
        "gender": "F",
        "birthyear": 2003,
        "breed": "Jack Russel Terrier"
      }
    ]
    "###);
}

// test with a server that responds 500 on 3 out of 4 calls
#[actix_rt::test]
async fn timeout() {
    let (_mock, setting) = create_slow_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "default": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir", "gender": "M", "birthyear": 2023, "breed": "Patou"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents, {".results.*._vectors.default.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "default": {
              "embeddings": "[vector]",
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 0.99, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["semanticHitCount"]), @"0");
    snapshot!(json_string!(response["hits"]), @"[]");

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 0.99, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["semanticHitCount"]), @"1");
    snapshot!(json_string!(response["hits"]), @r###"
    [
      {
        "id": 0,
        "name": "kefir",
        "gender": "M",
        "birthyear": 2023,
        "breed": "Patou"
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 0.99, "embedder": "default"}
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response["semanticHitCount"]), @"0");
    snapshot!(json_string!(response["hits"]), @"[]");
}

// test with a server that wrongly responds 400
