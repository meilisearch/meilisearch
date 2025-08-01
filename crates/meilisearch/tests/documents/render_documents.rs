use crate::common::{shared_index_for_fragments, Server};
use crate::json;
use meili_snap::{json_string, snapshot};

#[actix_rt::test]
async fn empty_id() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index.render(json! {{ "template": { "id": "" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "The template ID is empty.\n  Hint: Valid prefixes are `embedders` or `chatCompletions`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn wrong_id_prefix() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index.render(json! {{ "template": { "id": "wrong.disregarded" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Template ID must start with `embedders` or `chatCompletions`, but found `{wrong}`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn missing_embedder() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index.render(json! {{ "template": { "id": "embedders" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Template ID configured with `embedders` but no embedder name provided.\n  Hint: Available embedders are `rest`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn wrong_embedder() {
    let index = shared_index_for_fragments().await;

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.wrong.disregarded" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Embedder `{wrong}` does not exist.\n  Hint: Available embedders are `rest`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn missing_template_kind() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index.render(json! {{ "template": { "id": "embedders.rest" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Template ID configured with `embedders.{rest}` but no template kind provided.\n  Hint: Available fragments are `indexingFragments.basic`, `indexingFragments.withBreed`, `searchFragments.justBreed`, `searchFragments.justName`, `searchFragments.query`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn wrong_template_kind() {
    let index = shared_index_for_fragments().await;

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.rest.wrong.disregarded" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Wrong template `{wrong}` after embedder `{rest}`.\n  Hint: Available fragments are `indexingFragments.basic`, `indexingFragments.withBreed`, `searchFragments.justBreed`, `searchFragments.justName`, `searchFragments.query`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn document_template_on_fragmented_index() {
    let index = shared_index_for_fragments().await;

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.rest.documentTemplate" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Requested document template for embedder `{rest}` but it uses fragments.\n  Hint: Use `indexingFragments` or `searchFragments` instead.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn missing_fragment_name() {
    let index = shared_index_for_fragments().await;

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.rest.indexingFragments" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Indexing fragment name was not provided.\n  Hint: Available indexing fragments for embedder `{rest}` are `basic`, `withBreed`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.rest.searchFragments" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Search fragment name was not provided.\n  Hint: Available search fragments for embedder `{rest}` are `justBreed`, `justName`, `query`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn wrong_fragment_name() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{ "template": { "id": "embedders.rest.indexingFragments.wrong" }}})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Indexing fragment `{wrong}` does not exist for embedder `{rest}`.\n  Hint: Available indexing fragments are `basic`, `withBreed`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.rest.searchFragments.wrong" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Search fragment `{wrong}` does not exist for embedder `{rest}`.\n  Hint: Available search fragments are `justBreed`, `justName`, `query`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn leftover_tokens() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(
            json! {{ "template": { "id": "embedders.rest.indexingFragments.withBreed.leftover" }}},
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Leftover token `{leftover}` after parsing template ID",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);

    let (value, code) = index
        .render(json! {{"template": { "id": "embedders.rest.searchFragments.justBreed.leftover" }}})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Leftover token `{leftover}` after parsing template ID",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);

    let (value, code) = index
        .render(json! {{"template": { "id": "chatCompletions.documentTemplate.leftover" }}})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Leftover token `{leftover}` after parsing template ID",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn fragment_retrieval() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{ "template": { "id": "embedders.rest.indexingFragments.withBreed" }}})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a {{ doc.breed }}",
      "rendered": null
    }
    "#);

    let (value, code) = index
        .render(json! {{ "template": { "id": "embedders.rest.searchFragments.justBreed" }}})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "It's a {{ media.breed }}",
      "rendered": null
    }
    "#);
}

#[actix_rt::test]
async fn missing_chat_completions_template() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index.render(json! {{ "template": { "id": "chatCompletions" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Missing chat completion template ID. The only available template is `documentTemplate`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn wrong_chat_completions_template() {
    let index = shared_index_for_fragments().await;

    let (value, code) =
        index.render(json! {{ "template": { "id": "chatCompletions.wrong" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Unknown chat completion template ID `{wrong}`. The only available template is `documentTemplate`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn chat_completions_template_retrieval() {
    let index = shared_index_for_fragments().await;

    let (value, code) =
        index.render(json! {{ "template": { "id": "chatCompletions.documentTemplate" }}}).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
      "rendered": null
    }
    "#);
}

#[actix_rt::test]
async fn retrieve_document_template() {
    let server = Server::new_shared();
    let index = server.unique_index();

    let (response, code) = index
        .update_settings(json!(
        {
            "embedders": {
                "doggo_embedder": {
                    "source": "huggingFace",
                    "model": "sentence-transformers/all-MiniLM-L6-v2",
                    "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                    "documentTemplate": "This is a document template {{doc.doggo}}",
                }
            }
        }
        ))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response["taskUid"].as_u64().unwrap()).await;

    let (value, code) = index
        .render(json! {{ "template": { "id": "embedders.doggo_embedder.documentTemplate" }}})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "This is a document template {{doc.doggo}}",
      "rendered": null
    }
    "#);
}

#[actix_rt::test]
async fn render_document_kefir() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.indexingFragments.basic" },
            "input": { "documentId": "0" },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a dog",
      "rendered": "kefir is a dog"
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.indexingFragments.withBreed" },
            "input": { "documentId": "0" },
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(value, { ".message" => "[ignored]" }), @r#"
    {
      "message": "[ignored]",
      "code": "template_rendering_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#template_rendering_error"
    }
    "#);
}

#[actix_rt::test]
async fn render_inline_document_iko() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.indexingFragments.basic" },
            "input": { "inline": { "doc": { "name": "iko", "breed": "jack russell" } } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a dog",
      "rendered": "iko is a dog"
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.indexingFragments.withBreed" },
            "input": { "inline": { "doc": { "name": "iko", "breed": "jack russell" } } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a {{ doc.breed }}",
      "rendered": "iko is a jack russell"
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.searchFragments.justBreed" },
            "input": { "inline": { "media": { "name": "iko", "breed": "jack russell" } } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "It's a {{ media.breed }}",
      "rendered": "It's a jack russell"
    }
    "#);
}

#[actix_rt::test]
async fn chat_completions() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "id": "chatCompletions.documentTemplate" },
            "input": { "documentId": "0" },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
      "rendered": "id: 0\nname: kefir\n"
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": { "id": "chatCompletions.documentTemplate" },
            "input": { "inline": { "doc": { "name": "iko", "breed": "jack russell" } } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
      "rendered": "name: iko\nbreed: jack russell\n"
    }
    "#);
}

#[actix_rt::test]
async fn both_document_id_and_inline() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "inline": "{{ doc.name }} compared to {{ media.name }}" },
            "input": { "documentId": "0", "inline": { "media": { "name": "iko" } } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} compared to {{ media.name }}",
      "rendered": "kefir compared to iko"
    }
    "#);
}

#[actix_rt::test]
async fn multiple_templates_or_docs() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "id": "whatever", "inline": "whatever" }
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Cannot provide both an inline template and a template ID.",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": { "inline": "whatever" },
            "input": { "documentId": "0", "inline": { "doc": { "name": "iko" } } }
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "A document id was provided but adding it to the input would overwrite the `doc` field that you already defined inline.",
      "code": "invalid_render_input",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_input"
    }
    "#);
}

#[actix_rt::test]
async fn document_not_found() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.indexingFragments.basic" },
            "input": { "documentId": "9999" }
        }})
        .await;
    snapshot!(code, @"404 Not Found");
    snapshot!(value, @r#"
    {
      "message": "Document with ID `9999` not found.",
      "code": "render_document_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#render_document_not_found"
    }
    "#);
}

#[actix_rt::test]
async fn bad_template() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "inline": "{{ doc.name" },
            "input": { "documentId": "0" }
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Error parsing template: error while parsing template: liquid:  --> 1:4\n  |\n1 | {{ doc.name\n  |    ^---\n  |\n  = expected Literal\n",
      "code": "template_parsing_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#template_parsing_error"
    }
    "#);
}

#[actix_rt::test]
async fn inline_nested() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": { "inline": "{{ doc.name }} is a {{ doc.breed.name }} ({{ doc.breed.kind }})" },
            "input": { "inline": { "doc": { "name": "iko", "breed": { "name": "jack russell", "kind": "terrier" } } } }
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a {{ doc.breed.name }} ({{ doc.breed.kind }})",
      "rendered": "iko is a jack russell (terrier)"
    }
    "#);
}

#[actix_rt::test]
async fn embedder_document_template() {
    let (_mock, setting) = crate::vector::rest::create_mock().await;
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
            "embedders": {
                "rest": setting,
            },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();
    let documents = json!([
      {"id": 0, "name": "kefir"},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let (value, code) = index
        .render(json! {{
            "template": { "id": "embedders.rest.documentTemplate" },
            "input": { "documentId": "0" }
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{doc.name}}",
      "rendered": "kefir"
    }
    "#);

    let (value, code) =
        index.render(json! {{ "template": { "id": "embedders.rest.wrong.disregarded" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Wrong template `{wrong}` after embedder `{rest}`.\n  Hint: Available template: `documentTemplate`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);
}

#[actix_rt::test]
async fn ugly_embedder_and_fragment_names() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (_response, code) = server.set_features(json!({"multimodal": true})).await;
    snapshot!(code, @"200 OK");

    // Set up a mock server for the embedder
    let mock_server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
            "data": [0.1, 0.2, 0.3]
        })))
        .mount(&mock_server)
        .await;

    // Create an embedder with an ugly name containing quotes and special characters
    let (response, code) = index
        .update_settings(json!({
            "embedders": {
                "Open AI \"3.1\"": {
                    "source": "rest",
                    "url": mock_server.uri(),
                    "dimensions": 3,
                    "request": "{{fragment}}",
                    "response": {
                        "data": "{{embedding}}"
                    },
                    "indexingFragments": {
                        "ugly fragment \"name\".": {"value": "{{ doc.name }} processed by AI"}
                    },
                    "searchFragments": {
                        "search with [brackets]": {"value": "It's a {{ media.breed }}"}
                    }
                },
            },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // Test retrieving indexing fragment template with ugly name
    let (value, code) = index
        .render(json! {{
            "template": { "id": r#"embedders."Open AI \"3.1\"".indexingFragments."ugly fragment \"name\".""# },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} processed by AI",
      "rendered": null
    }
    "#);

    // Test retrieving search fragment template with ugly name
    let (value, code) = index
        .render(json! {{
            "template": { "id": r#"embedders."Open AI \"3.1\"".searchFragments."search with [brackets]""# },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "It's a {{ media.breed }}",
      "rendered": null
    }
    "#);

    // Test quoting normal parts of the template ID
    let (value, code) = index
        .render(json! {{
            "template": { "id": r#""embedders"."Open AI \"3.1\""."indexingFragments"."ugly fragment \"name\".""# }
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} processed by AI",
      "rendered": null
    }
    "#);
}
