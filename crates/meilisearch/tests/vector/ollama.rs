//! Tests ollama embedders with the server at the location described by `MEILI_TEST_OLLAMA_SERVER` environment variable.

use std::env::VarError;

use meili_snap::{json_string, snapshot};

use crate::common::{GetAllDocumentsOptions, Value};
use crate::json;
use crate::vector::get_server_vector;

pub enum Endpoint {
    /// Deprecated, undocumented endpoint
    Embeddings,
    /// Current endpoint
    Embed,
}

impl Endpoint {
    fn suffix(&self) -> &'static str {
        match self {
            Endpoint::Embeddings => "/api/embeddings",
            Endpoint::Embed => "/api/embed",
        }
    }
}

pub enum Model {
    Nomic,
    AllMinilm,
}

impl Model {
    fn name(&self) -> &'static str {
        match self {
            Model::Nomic => "nomic-embed-text",
            Model::AllMinilm => "all-minilm",
        }
    }
}

const DOGGO_TEMPLATE: &str = r#"{%- if doc.gender == "F" -%}Une chienne nommée {{doc.name}}, née en {{doc.birthyear}}
        {%- else -%}
        Un chien nommé {{doc.name}}, né en {{doc.birthyear}}
        {%- endif %}, de race {{doc.breed}}."#;

fn create_ollama_config_with_template(
    document_template: &str,
    model: Model,
    endpoint: Endpoint,
) -> Option<Value> {
    let ollama_base_url = match std::env::var("MEILI_TEST_OLLAMA_SERVER") {
        Ok(ollama_base_url) => ollama_base_url,
        Err(VarError::NotPresent) => return None,
        Err(VarError::NotUnicode(s)) => panic!(
            "`MEILI_TEST_OLLAMA_SERVER` was not properly utf-8, `{:?}`",
            s.as_encoded_bytes()
        ),
    };

    Some(json!({
        "source": "ollama",
        "url": format!("{ollama_base_url}{}", endpoint.suffix()),
        "documentTemplate": document_template,
        "documentTemplateMaxBytes": 8000000,
        "model": model.name()
    }))
}

#[actix_rt::test]
async fn test_both_apis() {
    let Some(embed_settings) =
        create_ollama_config_with_template(DOGGO_TEMPLATE, Model::AllMinilm, Endpoint::Embed)
    else {
        panic!("Missing `MEILI_TEST_OLLAMA_SERVER` environment variable, skipping `test_both_apis` test.");
    };

    let Some(embeddings_settings) =
        create_ollama_config_with_template(DOGGO_TEMPLATE, Model::AllMinilm, Endpoint::Embeddings)
    else {
        return;
    };

    let Some(nomic_embed_settings) =
        create_ollama_config_with_template(DOGGO_TEMPLATE, Model::Nomic, Endpoint::Embed)
    else {
        return;
    };

    let Some(nomic_embeddings_settings) =
        create_ollama_config_with_template(DOGGO_TEMPLATE, Model::Nomic, Endpoint::Embeddings)
    else {
        return;
    };

    let server = get_server_vector().await;

    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "embed": embed_settings,
              "embeddings": embeddings_settings,
              "nomic_embed": nomic_embed_settings,
              "nomic_embeddings": nomic_embeddings_settings,
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
    snapshot!(json_string!(documents, {".results.*._vectors.*.embeddings" => "[vector]"}), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "gender": "M",
          "birthyear": 2023,
          "breed": "Patou",
          "_vectors": {
            "embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "embeddings": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embeddings": {
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
            "embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "embeddings": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embeddings": {
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
            "embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "embeddings": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embeddings": {
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
            "embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "embeddings": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embed": {
              "embeddings": "[vector]",
              "regenerate": true
            },
            "nomic_embeddings": {
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
            "hybrid": {"semanticRatio": 1.0, "embedder": "embed"},
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
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "embeddings"},
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
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "embed"}
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
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "embeddings"}
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
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "embed"}
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
            "hybrid": {"semanticRatio": 1.0, "embedder": "embeddings"}
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
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "nomic_embed"},
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
            "q": "chien de chasse",
            "hybrid": {"semanticRatio": 1.0, "embedder": "nomic_embeddings"},
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
            "hybrid": {"semanticRatio": 1.0, "embedder": "nomic_embed"}
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
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "petit chien",
            "hybrid": {"semanticRatio": 1.0, "embedder": "nomic_embeddings"}
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
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "nomic_embed"}
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
      }
    ]
    "###);

    let (response, code) = index
        .search_post(json!({
            "q": "grand chien de berger des montagnes",
            "hybrid": {"semanticRatio": 1.0, "embedder": "nomic_embeddings"}
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
      }
    ]
    "###);
}
