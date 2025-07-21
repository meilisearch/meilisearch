use crate::common::{shared_index_for_fragments, Server};
use crate::json;
use meili_snap::{snapshot, json_string};

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
      "message": "Template ID must start with `embedders` or `chatCompletions`, but found `wrong`.",
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
      "message": "Embedder `wrong` does not exist.\n  Hint: Available embedders are `rest`.",
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
      "message": "Template ID configured with `embedders.rest` but no template kind provided.\n  Hint: Available fragments are `indexingFragments.basic`, `indexingFragments.withBreed`, `searchFragments.justBreed`, `searchFragments.justName`, `searchFragments.query`.",
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
      "message": "Wrong template `wrong` after embedder `rest`.\n  Hint: Available fragments are `indexingFragments.basic`, `indexingFragments.withBreed`, `searchFragments.justBreed`, `searchFragments.justName`, `searchFragments.query`.",
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
      "message": "Requested document template for embedder `rest` but it uses fragments.\n  Hint: Use `indexingFragments` or `searchFragments` instead.",
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
      "message": "Indexing fragment name was not provided.\n  Hint: Available indexing fragments for embedder `rest` are `basic`, `withBreed`.",
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
      "message": "Search fragment name was not provided.\n  Hint: Available search fragments for embedder `rest` are `justBreed`, `justName`, `query`.",
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
      "message": "Indexing fragment `wrong` does not exist for embedder `rest`.\n  Hint: Available indexing fragments are `basic`, `withBreed`.",
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
      "message": "Search fragment `wrong` does not exist for embedder `rest`.\n  Hint: Available search fragments are `justBreed`, `justName`, `query`.",
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
      "message": "Leftover token `leftover` after parsing template ID",
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
      "message": "Leftover token `leftover` after parsing template ID",
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
      "message": "Leftover token `leftover` after parsing template ID",
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
      "message": "Unknown chat completion template ID `wrong`. The only available template is `documentTemplate`.",
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
