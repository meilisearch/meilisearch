use crate::common::shared_index_for_fragments;
use crate::json;
use meili_snap::snapshot;

#[actix_rt::test]
async fn empty_id() {
    let index = shared_index_for_fragments().await;

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": ""
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "wrong.disregarded"
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders"
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.wrong.disregarded"
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest"
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest.wrong.disregarded"
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest.documentTemplate"
            }
        }})
        .await;
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest.indexingFragments"
            }
        }})
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(value, @r#"
    {
      "message": "Indexing fragment name was not provided.\n  Hint: Available indexing fragments for embedder `rest` are `basic`, `withBreed`.",
      "code": "invalid_render_template_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest.searchFragments"
            }
        }})
        .await;
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
        .render(json! {{
            "template": {
                "id": "embedders.rest.indexingFragments.wrong"
            }
        }})
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

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest.searchFragments.wrong"
            }
        }})
        .await;
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
        .render(json! {{
            "template": {
                "id": "embedders.rest.indexingFragments.withBreed.leftover"
            }
        }})
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
        .render(json! {{
            "template": {
                "id": "embedders.rest.searchFragments.justBreed.leftover"
            }
        }})
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
        .render(json! {{
            "template": {
                "id": "embedders.rest.indexingFragments.withBreed"
            }
        }})
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(value, @r#"
    {
      "template": "{{ doc.name }} is a {{ doc.breed }}",
      "rendered": null
    }
    "#);

    let (value, code) = index
        .render(json! {{
            "template": {
                "id": "embedders.rest.searchFragments.justBreed"
            }
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

// TODO chat completions
