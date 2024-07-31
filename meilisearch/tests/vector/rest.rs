use std::sync::atomic::{AtomicUsize, Ordering};

use meili_snap::{json_string, snapshot};
use reqwest::IntoUrl;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::common::Value;
use crate::json;
use crate::vector::{get_server_vector, GetAllDocumentsOptions};

async fn create_mock() -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    let counter = AtomicUsize::new(0);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |_req: &Request| {
            let counter = counter.fetch_add(1, Ordering::Relaxed);
            ResponseTemplate::new(200).set_body_json(json!({ "data": vec![counter; 3] }))
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "dimensions": 3,
        "request": "{{text}}",
        "response": {
          "data": "{{embedding}}"
        }
    });

    (mock_server, embedder_settings)
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct MultipleRequest {
    input: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MultipleResponse {
    output: Vec<SingleResponse>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SingleResponse {
    text: String,
    embedding: Vec<f32>,
}

async fn create_mock_multiple() -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    let counter = AtomicUsize::new(0);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let req: MultipleRequest = match req.body_json() {
                Ok(req) => req,
                Err(error) => {
                    return ResponseTemplate::new(400).set_body_json(json!({
                      "error": format!("Invalid request: {error}")
                    }));
                }
            };

            let output = req
                .input
                .into_iter()
                .map(|text| SingleResponse {
                    text,
                    embedding: vec![counter.fetch_add(1, Ordering::Relaxed) as f32; 3],
                })
                .collect();

            let response = MultipleResponse { output };

            ResponseTemplate::new(200).set_body_json(response)
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "dimensions": 3,
        "request": {
          "input": ["{{text}}", "{{..}}"]
        },
        "response": {
          "output": [
            {
              "embedding": "{{embedding}}"
            },
            "{{..}}"
          ]
        }
    });

    (mock_server, embedder_settings)
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct SingleRequest {
    input: String,
}

async fn create_mock_single_response_in_array() -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    let counter = AtomicUsize::new(0);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let req: SingleRequest = match req.body_json() {
                Ok(req) => req,
                Err(error) => {
                    return ResponseTemplate::new(400).set_body_json(json!({
                      "error": format!("Invalid request: {error}")
                    }));
                }
            };

            let output = vec![SingleResponse {
                text: req.input,
                embedding: vec![counter.fetch_add(1, Ordering::Relaxed) as f32; 3],
            }];

            let response = MultipleResponse { output };

            ResponseTemplate::new(200).set_body_json(response)
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "dimensions": 3,
        "request": {
          "input": "{{text}}"
        },
        "response": {
          "output": [
            {
              "embedding": "{{embedding}}"
            }
          ]
        }
    });

    (mock_server, embedder_settings)
}

async fn create_mock_raw_with_custom_header() -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    let counter = AtomicUsize::new(0);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            match req.headers.get("my-nonstandard-auth") {
                Some(x) if x == "bearer of the ring" => {}
                Some(x) => {
                    return ResponseTemplate::new(401).set_body_json(
                        json!({"error": format!("thou shall not pass, {}", x.to_str().unwrap())}),
                    )
                }
                None => {
                    return ResponseTemplate::new(401)
                        .set_body_json(json!({"error": "missing header 'my-nonstandard-auth'"}))
                }
            }

            let _req: String = match req.body_json() {
                Ok(req) => req,
                Err(error) => {
                    return ResponseTemplate::new(400).set_body_json(json!({
                      "error": format!("Invalid request: {error}")
                    }));
                }
            };

            let output = vec![counter.fetch_add(1, Ordering::Relaxed) as f32; 3];

            ResponseTemplate::new(200).set_body_json(output)
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "request": "{{text}}",
        "response": "{{embedding}}",
        "headers": {"my-nonstandard-auth": "bearer of the ring"}
    });

    (mock_server, embedder_settings)
}

async fn create_mock_raw() -> (MockServer, Value) {
    let mock_server = MockServer::start().await;

    let counter = AtomicUsize::new(0);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |req: &Request| {
            let _req: String = match req.body_json() {
                Ok(req) => req,
                Err(error) => {
                    return ResponseTemplate::new(400).set_body_json(json!({
                      "error": format!("Invalid request: {error}")
                    }));
                }
            };

            let output = vec![counter.fetch_add(1, Ordering::Relaxed) as f32; 3];

            ResponseTemplate::new(200).set_body_json(output)
        })
        .mount(&mock_server)
        .await;
    let url = mock_server.uri();

    let embedder_settings = json!({
        "source": "rest",
        "url": url,
        "dimensions": 3,
        "request": "{{text}}",
        "response": "{{embedding}}"
    });

    (mock_server, embedder_settings)
}

pub async fn post<T: IntoUrl>(url: T) -> reqwest::Result<reqwest::Response> {
    reqwest::Client::builder().build()?.post(url).send().await
}

#[actix_rt::test]
async fn dummy_testing_the_mock() {
    let (mock, _setting) = create_mock().await;
    let body = post(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @r###"{"data":[0,0,0]}"###);
    let body = post(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @r###"{"data":[1,1,1]}"###);
    let body = post(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @r###"{"data":[2,2,2]}"###);
    let body = post(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @r###"{"data":[3,3,3]}"###);
    let body = post(&mock.uri()).await.unwrap().text().await.unwrap();
    snapshot!(body, @r###"{"data":[4,4,4]}"###);
}

#[actix_rt::test]
async fn bad_request() {
    let (mock, _setting) = create_mock().await;

    let server = get_server_vector().await;
    let index = server.index("doggo");

    // No placeholder string appear in the template
    let (response, code) = index
      .update_settings(json!({
        "embedders": {
            "rest": json!({ "source": "rest", "url": mock.uri(), "request": "54", "response": "{{embedding}}" }),
        },
      }))
      .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
  {
    "message": "Error while generating embeddings: user error: in `request`: \"{{text}}\" not found",
    "code": "vector_embedding_error",
    "type": "invalid_request",
    "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
  }
  "###);

    // A repeat string appears inside a repeated value
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": [
                  {
                    "input": [
                      "{{text}}",
                      "{{..}}"
                    ]
                  },
                  "{{..}}"
                ]
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.input.input`: \"{{..}}\" appears nested inside of a value that is itself repeated",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeat string appears outside of an array
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": {
                  "input": "{{text}}",
                  "repeat": "{{..}}"
                }
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.input.repeat`: \"{{..}}\" appears outside of an array",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeat string appears in an array, but not in the second position
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": [
                  "{{..}}",
                  "{{text}}"
                ]
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.input`: \"{{..}}\" expected at position #1, but found at position #0",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": [
                  "{{text}}",
                  "42",
                  "{{..}}",
                ]
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.input`: \"{{..}}\" expected at position #1, but found at position #2",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeated value lacks a placeholder
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": [
                  "42",
                  "{{..}}",
                ]
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.input[0]`: Expected \"{{text}}\" inside of the repeated value",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // Multiple repeat strings appear in the template
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": [
                  "{{text}}",
                  "{{..}}",
                ],
                "data": [
                  "42",
                  "{{..}}",
                ],
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.data`: Found \"{{..}}\", but it was already present in `request.input`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // Multiple placeholder strings appear in the template
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": "{{text}}",
                "data": "{{text}}",
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.data`: Found \"{{text}}\", but it was already present in `request.input`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request":
              {"repeated": [{
                "input": "{{text}}",
                "data": [42, "{{text}}"],
              }, "{{..}}"]}, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.repeated.data[1]`: Found \"{{text}}\", but it was already present in `request.repeated.input`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A placeholder appears both inside a repeated value and outside of it
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": {
                "input": ["{{text}}", "{{..}}"],
                "data": "{{text}}",
              }, "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request.data`: Found \"{{text}}\", but it was already present in `request.input[0]` (repeated)",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);
}

#[actix_rt::test]
async fn bad_response() {
    let (mock, _setting) = create_mock().await;

    let server = get_server_vector().await;
    let index = server.index("doggo");

    // No placeholder string appear in the template
    let (response, code) = index
      .update_settings(json!({
        "embedders": {
            "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": "42" }),
        },
      }))
      .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response`: \"{{embedding}}\" not found",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeat string appears inside a repeated value
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": [
                  {
                    "output": [
                      "{{embedding}}",
                      "{{..}}"
                    ]
                  },
                  "{{..}}"
                ]
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.output.output`: \"{{..}}\" appears nested inside of a value that is itself repeated",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeat string appears outside of an array
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": {
                  "output": "{{embedding}}",
                  "repeat": "{{..}}"
                }
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.output.repeat`: \"{{..}}\" appears outside of an array",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeat string appears in an array, but not in the second position
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": [
                  "{{..}}",
                  "{{embedding}}"
                ]
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.output`: \"{{..}}\" expected at position #1, but found at position #0",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": [
                  "{{embedding}}",
                  "42",
                  "{{..}}",
                ]
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.output`: \"{{..}}\" expected at position #1, but found at position #2",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A repeated value lacks a placeholder
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": [
                  "42",
                  "{{..}}",
                ]
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.output[0]`: Expected \"{{embedding}}\" inside of the repeated value",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // Multiple repeat strings appear in the template
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": [
                  "{{embedding}}",
                  "{{..}}",
                ],
                "data": [
                  "42",
                  "{{..}}",
                ],
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.data`: Found \"{{..}}\", but it was already present in `response.output`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // Multiple placeholder strings appear in the template
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": [{"type": "data", "data": "{{embedding}}"}],
                "data": "{{embedding}}",
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.data`: Found \"{{embedding}}\", but it was already present in `response.output[0].data`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response":
              {"repeated": [{
                "output": "{{embedding}}",
                "data": [42, "{{embedding}}"],
              }, "{{..}}"]}, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.repeated.data[1]`: Found \"{{embedding}}\", but it was already present in `response.repeated.output`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // A placeholder appears both inside a repeated value and outside of it
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "output": ["{{embedding}}", "{{..}}"],
                "data": "{{embedding}}",
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response.data`: Found \"{{embedding}}\", but it was already present in `response.output[0]` (repeated)",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // request sends a single text but response expects multiple embeddings
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "data": ["{{embedding}}", "{{..}}"],
              }, "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response`: `response` has multiple embeddings, but `request` has only one text to embed",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // request sends multiple texts but response expects a single embedding
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": {
                "data": "{{embedding}}",
              }, "request": {"data": ["{{text}}", "{{..}}"]} }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response`: `response` has a single embedding, but `request` has multiple texts to embed",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);
}

#[actix_rt::test]
async fn bad_settings() {
    let (mock, _setting) = create_mock().await;

    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "request": 42, "response": 42 }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `request`: \"{{text}}\" not found",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": "kefir", "request": 42, "response": 42 }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.rest.url`: could not parse `kefir`: relative URL without a base",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "response": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.rest`: Missing field `request` (note: this field is mandatory for source rest)",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}" }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.rest`: Missing field `response` (note: this field is mandatory for source rest)",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": 42 }),
          },
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: in `response`: \"{{embedding}}\" not found",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": "{{text}}",
            "response": "{{embedding}}"
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with runtime error: error extracting embeddings from the response:\n  - in `response`, while extracting a single \"{{embedding}}\", expected `response` to be an array of numbers, but failed to parse server response:\n  - invalid type: map, expected a sequence",
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

    // Validate an embedder with a bad dimension of 2 instead of 3
    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": { "data": "{{embedding}}" }, "dimensions": 2 }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);

    let (response, code) = index.add_documents(json!( { "id": 1, "name": "kefir" }), None).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "While embedding documents for embedder `rest`: runtime error: was expecting embeddings of dimension `2`, got embeddings of dimensions `3`",
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
}

#[actix_rt::test]
async fn add_vector_and_user_provided() {
    let (_mock, setting) = create_mock().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir"},
      {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
      {"id": 2, "name": "intel"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
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
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 2,
          "name": "intel",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
}

#[actix_rt::test]
async fn server_returns_bad_request() {
    let (mock, _setting) = create_mock_multiple().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": "{{embedding}}" }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": "{{text}}",
            "response": "{{embedding}}"
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with user error: sent a bad request to embedding server\n  - Hint: check that the `request` in the embedder configuration matches the remote server's API\n  - server replied with `{\"error\":\"Invalid request: invalid type: string \\\"test\\\", expected struct MultipleRequest at line 1 column 6\"}`",
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

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": "{{embedding}}", "dimensions": 3 }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "dimensions": 3,
            "url": "[url]",
            "request": "{{text}}",
            "response": "{{embedding}}"
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.add_documents(json!( { "id": 1, "name": "kefir" }), None).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "While embedding documents for embedder `rest`: user error: sent a bad request to embedding server\n  - Hint: check that the `request` in the embedder configuration matches the remote server's API\n  - server replied with `{\"error\":\"Invalid request: invalid type: string \\\" id: 1\\\\n name: kefir\\\\n\\\", expected struct MultipleRequest at line 1 column 24\"}`",
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
}

#[actix_rt::test]
async fn server_returns_bad_response() {
    let (mock, _setting) = create_mock_multiple().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(),
              "request": {
                "input": ["{{text}}", "{{..}}"]
              },
              "response": ["{{embedding}}", "{{..}}"] }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": {
              "input": [
                "{{text}}",
                "{{..}}"
              ]
            },
            "response": [
              "{{embedding}}",
              "{{..}}"
            ]
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with runtime error: error extracting embeddings from the response:\n  - in `response`, while extracting the array of \"{{embedding}}\"s, configuration expects `response` to be an array with at least 1 item(s) but server sent an object with 1 field(s)",
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

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(),
              "request": {
                "input": ["{{text}}", "{{..}}"]
              },
              "response": {
                "output": ["{{embedding}}", "{{..}}"]
              } }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": {
              "input": [
                "{{text}}",
                "{{..}}"
              ]
            },
            "response": {
              "output": [
                "{{embedding}}",
                "{{..}}"
              ]
            }
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with runtime error: error extracting embeddings from the response:\n  - in `response`, while extracting item #0 from the array of \"{{embedding}}\"s, expected `response` to be an array of numbers, but failed to parse server response:\n  - invalid type: map, expected a sequence",
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

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(),
              "request": {
                "input": ["{{text}}"]
              },
              "response": {
                "output": "{{embedding}}"
              } }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": {
              "input": [
                "{{text}}"
              ]
            },
            "response": {
              "output": "{{embedding}}"
            }
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with runtime error: error extracting embeddings from the response:\n  - in `response.output`, while extracting a single \"{{embedding}}\", expected `output` to be an array of numbers, but failed to parse server response:\n  - invalid type: map, expected f32",
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

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(),
              "request": {
                "input": ["{{text}}", "{{..}}"]
              },
              "response": {
                "output": [{ "embedding":
              {
                "data": "{{embedding}}"
              }
               }, "{{..}}"]
              } }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": {
              "input": [
                "{{text}}",
                "{{..}}"
              ]
            },
            "response": {
              "output": [
                {
                  "embedding": {
                    "data": "{{embedding}}"
                  }
                },
                "{{..}}"
              ]
            }
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with runtime error: error extracting embeddings from the response:\n  - in `response.embedding`, while extracting item #0 from the array of \"{{embedding}}\"s, configuration expects `embedding` to be an object with key `data` but server sent an array of size 3",
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

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": json!({ "source": "rest", "url": mock.uri(),
              "request": {
                "input": ["{{text}}"]
              },
              "response": {
                "output": [
                  { "embeddings":
                    {
                      "data": "{{embedding}}"
                    }
                  }
                ]
              } }),
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": {
              "input": [
                "{{text}}"
              ]
            },
            "response": {
              "output": [
                {
                  "embeddings": {
                    "data": "{{embedding}}"
                  }
                }
              ]
            }
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with runtime error: error extracting embeddings from the response:\n  - in `response.output[0]`, while extracting a single \"{{embedding}}\", configuration expects key \"embeddings\", which is missing in response\n  - Hint: item #0 inside `output` has key `embedding`, did you mean `response.output[0].embedding` in embedder configuration?",
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
}

#[actix_rt::test]
async fn server_returns_multiple() {
    let (_mock, setting) = create_mock_multiple().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir"},
      {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
      {"id": 2, "name": "intel"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
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
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 2,
          "name": "intel",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
}

#[actix_rt::test]
async fn server_single_input_returns_in_array() {
    let (_mock, setting) = create_mock_single_response_in_array().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir"},
      {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
      {"id": 2, "name": "intel"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
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
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 2,
          "name": "intel",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
}

#[actix_rt::test]
async fn server_raw() {
    let (_mock, setting) = create_mock_raw().await;
    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);
    let documents = json!([
      {"id": 0, "name": "kefir"},
      {"id": 1, "name": "echo", "_vectors": { "rest": [1, 1, 1] }},
      {"id": 2, "name": "intel"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
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
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  0.0,
                  0.0,
                  0.0
                ]
              ],
              "regenerate": true
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 2,
          "name": "intel",
          "_vectors": {
            "rest": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  1.0
                ]
              ],
              "regenerate": true
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
}

#[actix_rt::test]
async fn server_custom_header() {
    let (mock, setting) = create_mock_raw_with_custom_header().await;

    let server = get_server_vector().await;
    let index = server.index("doggo");

    let (response, code) = index
  .update_settings(json!({
    "embedders": {
        "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": "{{embedding}}" }),
    },
  }))
  .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": "{{text}}",
            "response": "{{embedding}}"
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with user error: could not authenticate against embedding server\n  - server replied with `{\"error\":\"missing header 'my-nonstandard-auth'\"}`\n  - Hint: Check the `apiKey` parameter in the embedder configuration",
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

    let (response, code) = index
.update_settings(json!({
  "embedders": {
      "rest": json!({ "source": "rest", "url": mock.uri(), "request": "{{text}}", "response": "{{embedding}}", "headers": {"my-nonstandard-auth": "Balrog"} }),
  },
}))
.await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": "{{text}}",
            "response": "{{embedding}}",
            "headers": {
              "my-nonstandard-auth": "Balrog"
            }
          }
        }
      },
      "error": {
        "message": "Error while generating embeddings: runtime error: could not determine model dimensions:\n  - test embedding failed with user error: could not authenticate against embedding server\n  - server replied with `{\"error\":\"thou shall not pass, Balrog\"}`\n  - Hint: Check the `apiKey` parameter in the embedder configuration",
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

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "rest": setting,
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "rest": {
            "source": "rest",
            "url": "[url]",
            "request": "{{text}}",
            "response": "{{embedding}}",
            "headers": {
              "my-nonstandard-auth": "bearer of the ring"
            }
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}
