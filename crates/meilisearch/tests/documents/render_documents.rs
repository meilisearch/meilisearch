use meili_snap::{json_string, snapshot};

use crate::common::{shared_server_and_index_for_fragments, Server};
use crate::json;

#[actix_rt::test]
async fn wrong_params() {
    let server = Server::new().await;

    // missing experimental feature
    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "documentTemplate", "indexUid": "test", "embedder": "rest" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "calling the /render-template route requires enabling the `render_route` experimental feature. See https://github.com/orgs/meilisearch/discussions/888",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

    let (value, code) = server.render_template(json! {{}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "Missing field `template`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    let (value, code) = server.render_template(json! {{"template":{}}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "Missing field `kind` inside `.template`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server.render_template(json! {{"template":{"kind": "bad"}}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "Unknown value `bad` at `.template.kind`: expected one of `documentTemplate`, `chatDocumentTemplate`, `indexingFragment`, `searchFragment`, `inlineDocumentTemplate`, `inlineFragment`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) =
        server.render_template(json! {{"template":{"kind": "documentTemplate"}}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `index_uid` missing for kind `documentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server
        .render_template(json! {{"template":{"kind": "documentTemplate", "embedder": "test"}}})
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `index_uid` missing for kind `documentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server.render_template(json! {{"template":{"kind": "documentTemplate", "indexUid": "test", "embedder": "test", "fragment": "bad"}}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `fragment` disallowed for kind `documentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server.render_template(json! {{"template":{"kind": "documentTemplate", "indexUid": "test", "embedder": "test", "inline": "bad"}}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `inline` disallowed for kind `documentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) =
        server.render_template(json! {{"template":{"kind": "inlineDocumentTemplate"}}}).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `inline` missing for kind `inlineDocumentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server
        .render_template(
            json! {{"template":{"kind": "inlineDocumentTemplate", "indexUid": "test"}}},
        )
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `index_uid` disallowed for kind `inlineDocumentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server
        .render_template(
            json! {{"template":{"kind": "inlineDocumentTemplate", "embedder": "test"}}},
        )
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `embedder` disallowed for kind `inlineDocumentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) = server
        .render_template(
            json! {{"template":{"kind": "inlineDocumentTemplate", "fragment": "test"}}},
        )
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: parameter `fragment` disallowed for kind `inlineDocumentTemplate`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);
}

#[actix_rt::test]
async fn wrong_embedder() {
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "documentTemplate", "indexUid": index.uid, "embedder": "wrong" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: cannot find embedder `wrong` in index `[uuid]`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);
}

#[actix_rt::test]
async fn document_template_on_fragmented_embedder() {
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "documentTemplate", "indexUid": index.uid, "embedder": "rest" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: embedder `rest` in index `[uuid]` does not use a document template.",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);
}

#[actix_rt::test]
async fn fragment_on_document_template_embedder() {
    let server = Server::new().await;

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

    let index = server.unique_index();
    let (_response, _code) = server.set_features(json!({"multimodal": true})).await;

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
    server.wait_task(response).await.succeeded();

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "doggo_embedder", "fragment": "bad" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: embedder `doggo_embedder` in index `[uuid]` does not use fragments.",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);
}

#[actix_rt::test]
async fn wrong_fragment_name() {
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "bad" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: cannot find indexing fragment `bad` for embedder `rest` in index `[uuid]`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "searchFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "bad" }}}).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: cannot find search fragment `bad` for embedder `rest` in index `[uuid]`",
      "code": "invalid_render_template",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template"
    }
    "###);
}

#[actix_rt::test]
async fn fragment_retrieval() {
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "withBreed" }}}).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a {{ doc.breed }}",
      "rendered": null
    }
    "#);

    let (value, code) =
        server.render_template(json! {{ "template": { "kind": "searchFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "justBreed" }}}).await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "It's a {{ media.breed }}",
      "rendered": null
    }
    "#);
}

#[actix_rt::test]
async fn chat_completions_template_retrieval() {
    let server = Server::new().await;
    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task).await.succeeded();

    let (_response, code) =
        server.set_features(json!({"chatCompletions": true, "renderRoute": true})).await;
    snapshot!(code, @"200 OK");

    let (value, code) = server
        .render_template(
            json! {{ "template": { "kind": "chatDocumentTemplate", "indexUid": index.uid}}},
        )
        .await;
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
    let server = Server::new().await;

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

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
    server.wait_task(response).await.succeeded();

    let (value, code) = server
        .render_template(json! {{ "template": { "kind": "documentTemplate", "indexUid": index.uid, "embedder": "doggo_embedder" }}})
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
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment":"basic" },
            "input": { "kind": "indexDocument", "indexUid": index.uid, "id": "0" },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a dog",
      "rendered": "kefir is a dog"
    }
    "#);

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment":"withBreed" },
            "input": { "kind": "indexDocument", "indexUid": index.uid, "id": "0" },
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
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "basic" },
            "input": { "kind": "inlineDocument", "inline": {"name": "iko", "breed": "jack russell" } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a dog",
      "rendered": "iko is a dog"
    }
    "#);

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "withBreed" },
            "input": { "kind": "inlineDocument", "inline": {"name": "iko", "breed": "jack russell" } },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a {{ doc.breed }}",
      "rendered": "iko is a jack russell"
    }
    "#);

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "searchFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "justBreed" },
            "input": { "kind": "inlineSearch", "inline": {"media": {"name":"iko","breed":"jack russell"}, "filter": "ignored", "q": "unused" } },
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
async fn render_doc_not_object() {
    let server = Server::new().await;

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "inlineDocumentTemplate", "inline": "{{ doc }}" },
            "input": { "kind": "inlineDocument", "inline":  "that's not an object, that's a string" },
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching input: parsing inline document: invalid type: string \"that's not an object, that's a string\", expected a map at line 1 column 39\n  - Note: the inline document must be a JSON map",
      "code": "invalid_render_input",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_input"
    }
    "###);

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "inlineDocumentTemplate", "inline": "nothing to render" },
            "input": { "kind": "inlineDocument", "inline":  null },
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching input: parameter `inline` missing for kind `inlineDocument`",
      "code": "invalid_render_input",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_input"
    }
    "###);
}

#[actix_rt::test]
async fn render_search_not_object() {
    let server = Server::new().await;
    let (_response, code) =
        server.set_features(json!({"multimodal": true, "renderRoute": true})).await;
    snapshot!(code, @"200 OK");

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "inlineFragment", "inline": "{{ q }}: {{ media }}" },
            "input": { "kind": "inlineSearch", "inline":  "that's not an object, that's a string" },
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching input: parsing inline search: invalid type: string \"that's not an object, that's a string\", expected a map at line 1 column 39\n  - Note: the inline search query must be a JSON map containing `q` and/or `media`",
      "code": "invalid_render_input",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_input"
    }
    "###);

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "inlineFragment", "inline": "default" },
            "input": { "kind": "inlineSearch", "inline":  null },
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching input: parameter `inline` missing for kind `inlineSearch`",
      "code": "invalid_render_input",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_input"
    }
    "###);
}

#[actix_rt::test]
async fn chat_completions() {
    let server = Server::new().await;

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

    let index = server.unique_index();
    let (task, _code) = index.create(None).await;
    server.wait_task(task).await.succeeded();

    let (task, _code) = index.add_documents(json!([{"id": "0", "name": "kefir"}]), None).await;
    server.wait_task(task).await.succeeded();

    let (_response, _code) = server.set_features(json!({"chatCompletions": true})).await;

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "chatDocumentTemplate", "indexUid": index.uid },
            "input": { "kind": "indexDocument", "id": "0", "indexUid": index.uid },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
      "rendered": "id: 0\nname: kefir\n"
    }
    "#);

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "chatDocumentTemplate", "indexUid": index.uid },
            "input": { "kind": "inlineDocument", "inline": { "name": "iko", "breed": "jack russell" } },
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
async fn document_not_found() {
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "indexingFragment", "indexUid": index.uid, "embedder": "rest", "fragment": "basic" },
            "input": { "kind": "indexDocument", "indexUid": index.uid, "id": "9999" }
        }})
        .await;
    snapshot!(code, @"404 Not Found");
    snapshot!(value, @r###"
    {
      "message": "error while fetching input: document `9999` not found in `[uuid]`",
      "code": "render_document_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#render_document_not_found"
    }
    "###);
}
#[actix_rt::test]
async fn bad_template() {
    let (server, index) = shared_server_and_index_for_fragments().await;

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "inlineDocumentTemplate", "inline": "{{ doc.name" },
            "input": { "kind": "indexDocument", "indexUid": index.uid, "id": "0" }
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r###"
    {
      "message": "error while fetching template: user error: cannot parse template: liquid:  --> 1:4\n  |\n1 | {{ doc.name\n  |    ^---\n  |\n  = expected Literal\n",
      "code": "template_parsing_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#template_parsing_error"
    }
    "###);
}
#[actix_rt::test]
async fn inline_nested() {
    let (server, _index) = shared_server_and_index_for_fragments().await;

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "inlineDocumentTemplate", "inline": "{{ doc.name }} is a {{ doc.breed.name }} ({{ doc.breed.kind }})" },
            "input": { "kind": "inlineDocument", "inline": { "name": "iko", "breed": { "name": "jack russell", "kind": "terrier" } } }
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

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

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
    server.wait_task(value.uid()).await.succeeded();

    let (value, code) = server
        .render_template(json! {{
            "template": { "kind": "documentTemplate", "indexUid": index.uid, "embedder": "rest" },
            "input": { "kind": "indexDocument", "indexUid": index.uid, "id": "0" }
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{doc.name}}",
      "rendered": "kefir"
    }
    "#);
}

#[actix_rt::test]
async fn ugly_embedder_and_fragment_names() {
    let server = Server::new().await;

    let (_response, code) = server.set_features(json!({"renderRoute": true})).await;
    snapshot!(code, @"200 OK");

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
    let (value, code) = server
        .render_template(json! {{
            "template": {
                "kind": "indexingFragment",
                "indexUid": index.uid,
                "embedder": "Open AI \"3.1\"",
                "fragment": "ugly fragment \"name\"."
            },
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
    let (value, code) = server
        .render_template(json! {{
            "template": {
                "kind": "searchFragment",
                "indexUid": index.uid,
                "embedder": "Open AI \"3.1\"",
                "fragment": "search with [brackets]"
            },
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "It's a {{ media.breed }}",
      "rendered": null
    }
    "#);
}
