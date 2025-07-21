use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use liquid::ValueView;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidRenderInput, InvalidRenderInputDocumentId, InvalidRenderInputInline,
    InvalidRenderTemplate, InvalidRenderTemplateId, InvalidRenderTemplateInline,
};
use meilisearch_types::error::Code;
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::prompt::{get_document, get_inline_document_fields};
use meilisearch_types::milli::vector::db::IndexEmbeddingConfig;
use meilisearch_types::milli::vector::json_template::{self, JsonTemplate};
use meilisearch_types::milli::vector::EmbedderOptions;
use meilisearch_types::{heed, milli, Index};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::DoubleActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::render_analytics::RenderAggregator;

#[derive(OpenApi)]
#[openapi(
    paths(render_post),
    tags((
        name = "Render documents",
        description = "The /render route allows rendering templates used by Meilisearch.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/render"),   
    )),
)]
pub struct RenderApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(render_post))));
}

/// Render documents with POST
#[utoipa::path(
    post,
    path = "{indexUid}/render",
    tag = "Render documents",
    security(("Bearer" = ["settings.get,documents.get", "*.get", "*"])),
    params(("indexUid" = String, Path, example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = RenderQuery,
    responses(
        (status = 200, description = "The rendered result is returned along with the template", body = RenderResult, content_type = "application/json", example = json!(
            {
                "template": "{{ doc.breed }} called {{ doc.name }}",
                "rendered": "A Jack Russell called Iko"
            }
        )),
        (status = 404, description = "Template or document not found", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Document with ID `9999` not found.",
                "code": "render_document_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#render_document_not_found"
            }
        )),
        (status = 400, description = "Parameters are incorrect", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Indexing fragment `mistake` does not exist for embedder `rest`.\n  Hint: Available indexing fragments are `basic`, `withBreed`.",
                "code": "invalid_render_template_id",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
            }
        )),
    )
)]
pub async fn render_post(
    index_scheduler: GuardedData<
        DoubleActionPolicy<{ actions::SETTINGS_GET }, { actions::DOCUMENTS_GET }>,
        Data<IndexScheduler>,
    >,
    index_uid: web::Path<String>,
    params: AwebJson<RenderQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;
    let index = index_scheduler.index(&index_uid)?;

    let query = params.into_inner();
    debug!(parameters = ?query, "Render document");

    let mut aggregate = RenderAggregator::from_query(&query);

    let result = render(index, query).await;

    if result.is_ok() {
        aggregate.succeed();
    }
    analytics.publish(aggregate, &req);

    let result = result?;

    debug!(returns = ?result, "Render document");
    Ok(HttpResponse::Ok().json(result))
}

#[derive(Clone, Copy)]
enum FragmentKind {
    Indexing,
    Search,
}

impl FragmentKind {
    fn as_str(&self) -> &'static str {
        match self {
            FragmentKind::Indexing => "indexing",
            FragmentKind::Search => "search",
        }
    }

    fn capitalized(&self) -> &'static str {
        match self {
            FragmentKind::Indexing => "Indexing",
            FragmentKind::Search => "Search",
        }
    }
}

enum RenderError {
    MultipleTemplates,
    MissingTemplate,
    EmptyTemplateId,
    UnknownTemplateRoot(String),
    MissingEmbedderName {
        available: Vec<String>,
    },
    EmbedderDoesNotExist {
        embedder: String,
        available: Vec<String>,
    },
    EmbedderUsesFragments {
        embedder: String,
    },
    MissingTemplateAfterEmbedder {
        embedder: String,
        indexing: Vec<String>,
        search: Vec<String>,
    },
    UnknownTemplatePrefix {
        embedder: String,
        found: String,
        indexing: Vec<String>,
        search: Vec<String>,
    },
    ReponseError(ResponseError),
    MissingFragment {
        embedder: String,
        kind: FragmentKind,
        available: Vec<String>,
    },
    FragmentDoesNotExist {
        embedder: String,
        fragment: String,
        kind: FragmentKind,
        available: Vec<String>,
    },
    LeftOverToken(String),
    MissingChatCompletionTemplate,
    UnknownChatCompletionTemplate(String),

    DocumentNotFound(String),
    BothInlineDocAndDocId,
    TemplateParsing(json_template::Error),
    TemplateRendering(json_template::Error),
    CouldNotHandleInput,
}

impl From<heed::Error> for RenderError {
    fn from(error: heed::Error) -> Self {
        RenderError::ReponseError(error.into())
    }
}

impl From<milli::Error> for RenderError {
    fn from(error: milli::Error) -> Self {
        RenderError::ReponseError(error.into())
    }
}

use RenderError::*;

impl From<RenderError> for ResponseError {
    fn from(error: RenderError) -> Self {
        match error {
            MultipleTemplates => ResponseError::from_msg(
                String::from("Cannot provide both an inline template and a template ID."),
                Code::InvalidRenderTemplate,
            ),
            MissingTemplate => ResponseError::from_msg(
                String::from("No template provided. Please provide either an inline template or a template ID."),
                Code::InvalidRenderTemplate,
            ),
            EmptyTemplateId => ResponseError::from_msg(
                String::from("The template ID is empty.\n  Hint: Valid prefixes are `embedders` or `chatCompletions`."),
                Code::InvalidRenderTemplateId,
            ),
            UnknownTemplateRoot(root) => ResponseError::from_msg(
                format!("Template ID must start with `embedders` or `chatCompletions`, but found `{root}`."),
                Code::InvalidRenderTemplateId,
            ),
            MissingEmbedderName { mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("Template ID configured with `embedders` but no embedder name provided.\n  Hint: Available embedders are {}.", 
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            EmbedderDoesNotExist { embedder, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("Embedder `{embedder}` does not exist.\n  Hint: Available embedders are {}.",
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            EmbedderUsesFragments { embedder } => ResponseError::from_msg(
                format!("Requested document template for embedder `{embedder}` but it uses fragments.\n  Hint: Use `indexingFragments` or `searchFragments` instead."),
                Code::InvalidRenderTemplateId,
            ),
            MissingTemplateAfterEmbedder { embedder, mut indexing, mut search } => {
                if indexing.is_empty() && search.is_empty() {
                    ResponseError::from_msg(
                        format!("Missing template id after embedder `{embedder}`.\n  Hint: Available fragments: `documentTemplate`."),
                        Code::InvalidRenderTemplateId,
                    )
                } else {
                    indexing.sort_unstable();
                    search.sort_unstable();
                    ResponseError::from_msg(
                        format!("Template ID configured with `embedders.{embedder}` but no template kind provided.\n  Hint: Available fragments are {}.",
                            indexing.iter().map(|s| format!("`indexingFragments.{s}`")).chain(
                            search.iter().map(|s| format!("`searchFragments.{s}`"))).collect::<Vec<_>>().join(", ")),
                        Code::InvalidRenderTemplateId,
                    )
                }
            },
            UnknownTemplatePrefix { embedder, found, mut indexing, mut search } => {
                if indexing.is_empty() && search.is_empty() {
                    ResponseError::from_msg(
                        format!("Wrong template `{found}` after embedder `{embedder}`.\n  Hint: Available fragments: `documentTemplate`."),
                        Code::InvalidRenderTemplateId,
                    )
                } else {
                    indexing.sort_unstable();
                    search.sort_unstable();
                    ResponseError::from_msg(
                        format!("Wrong template `{found}` after embedder `{embedder}`.\n  Hint: Available fragments are {}.",
                            indexing.iter().map(|s| format!("`indexingFragments.{s}`")).chain(
                            search.iter().map(|s| format!("`searchFragments.{s}`"))).collect::<Vec<_>>().join(", ")),
                        Code::InvalidRenderTemplateId,
                    )
                }
            },
            ReponseError(response_error) => response_error,
            MissingFragment { embedder, kind, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("{} fragment name was not provided.\n  Hint: Available {} fragments for embedder `{embedder}` are {}.",
                        kind.capitalized(),
                        kind.as_str(),
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            FragmentDoesNotExist { embedder, fragment, kind, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("{} fragment `{fragment}` does not exist for embedder `{embedder}`.\n  Hint: Available {} fragments are {}.",
                        kind.capitalized(),
                        kind.as_str(),
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            LeftOverToken(token) => ResponseError::from_msg(
                format!("Leftover token `{token}` after parsing template ID"),
                Code::InvalidRenderTemplateId,
            ),
            MissingChatCompletionTemplate => ResponseError::from_msg(
                String::from("Missing chat completion template ID. The only available template is `documentTemplate`."),
                Code::InvalidRenderTemplateId,
            ),
            UnknownChatCompletionTemplate(id) => ResponseError::from_msg(
                format!("Unknown chat completion template ID `{id}`. The only available template is `documentTemplate`."),
                Code::InvalidRenderTemplateId,
            ),
            DocumentNotFound(doc_id) => ResponseError::from_msg(
                format!("Document with ID `{doc_id}` not found."),
                Code::RenderDocumentNotFound,
            ),
            BothInlineDocAndDocId => ResponseError::from_msg(
                String::from("A document id was provided but adding it to the input would overwrite the `doc` field that you already defined inline."),
                Code::InvalidRenderInput,
            ),
            TemplateParsing(err) => ResponseError::from_msg(
                format!("Error parsing template: {}", err.parsing_error("input")),
                Code::TemplateParsingError,
            ),
            TemplateRendering(err) => ResponseError::from_msg(
                format!("Error rendering template: {}", err.rendering_error("input")),
                Code::TemplateRenderingError,
            ),
            CouldNotHandleInput => ResponseError::from_msg(
                String::from("Could not handle the input provided."),
                Code::InvalidRenderInput,
            ),
        }
    }
}

fn parse_template_id_fragment(
    name: Option<&str>,
    kind: FragmentKind,
    embedding_config: &IndexEmbeddingConfig,
    embedder_name: &str,
) -> Result<serde_json::Value, RenderError> {
    let get_available =
        [EmbedderOptions::indexing_fragments, EmbedderOptions::search_fragments][kind as usize];
    let get_specific =
        [EmbedderOptions::indexing_fragment, EmbedderOptions::search_fragment][kind as usize];

    let fragment_name = name.ok_or_else(|| MissingFragment {
        embedder: embedder_name.to_string(),
        kind,
        available: get_available(&embedding_config.config.embedder_options),
    })?;

    let fragment = get_specific(&embedding_config.config.embedder_options, fragment_name)
        .ok_or_else(|| FragmentDoesNotExist {
            embedder: embedder_name.to_string(),
            fragment: fragment_name.to_string(),
            kind,
            available: get_available(&embedding_config.config.embedder_options),
        })?;

    Ok(fragment.clone())
}

fn parse_template_id(
    index: &Index,
    rtxn: &RoTxn<'_>,
    id: &str,
) -> Result<(serde_json::Value, bool), RenderError> {
    let mut parts = id.split('.');

    let root = parts.next().ok_or(EmptyTemplateId)?;

    let template = match root {
        "embedders" => {
            let index_embedding_configs = index.embedding_configs();
            let embedding_configs = index_embedding_configs.embedding_configs(rtxn)?;
            let get_embedders = || embedding_configs.iter().map(|c| c.name.clone()).collect();

            let embedder =
                parts.next().ok_or_else(|| MissingEmbedderName { available: get_embedders() })?;

            let embedding_config = embedding_configs
                .iter()
                .find(|config| config.name == embedder)
                .ok_or_else(|| EmbedderDoesNotExist {
                    embedder: embedder.to_string(),
                    available: get_embedders(),
                })?;

            let get_indexing = || embedding_config.config.embedder_options.indexing_fragments();
            let get_search = || embedding_config.config.embedder_options.search_fragments();

            let template_kind = parts.next().ok_or_else(|| MissingTemplateAfterEmbedder {
                embedder: embedder.to_string(),
                indexing: get_indexing(),
                search: get_search(),
            })?;
            match template_kind {
                "documentTemplate" | "documenttemplate"
                    if !embedding_config.fragments.as_slice().is_empty() =>
                {
                    return Err(EmbedderUsesFragments { embedder: embedder.to_string() });
                }
                "documentTemplate" | "documenttemplate" => (
                    serde_json::Value::String(embedding_config.config.prompt.template.clone()),
                    true,
                ),
                "indexingFragments" | "indexingfragments" => (
                    parse_template_id_fragment(
                        parts.next(),
                        FragmentKind::Indexing,
                        embedding_config,
                        embedder,
                    )?,
                    false,
                ),
                "searchFragments" | "searchfragments" => (
                    parse_template_id_fragment(
                        parts.next(),
                        FragmentKind::Search,
                        embedding_config,
                        embedder,
                    )?,
                    false,
                ),
                found => {
                    return Err(UnknownTemplatePrefix {
                        embedder: embedder.to_string(),
                        found: found.to_string(),
                        indexing: get_indexing(),
                        search: get_search(),
                    })
                }
            }
        }
        "chatCompletions" | "chatcompletions" => {
            let template_name = parts.next().ok_or(MissingChatCompletionTemplate)?;

            if template_name != "documentTemplate" {
                return Err(UnknownChatCompletionTemplate(template_name.to_string()));
            }

            let chat_config = index.chat_config(rtxn)?;

            (serde_json::Value::String(chat_config.prompt.template.clone()), true)
        }
        "" => return Err(EmptyTemplateId),
        unknown => {
            return Err(UnknownTemplateRoot(unknown.to_string()));
        }
    };

    if let Some(next) = parts.next() {
        return Err(LeftOverToken(next.to_string()));
    }

    Ok(template)
}

async fn render(index: Index, query: RenderQuery) -> Result<RenderResult, RenderError> {
    let rtxn = index.read_txn()?;

    let (template, fields_available) = match (query.template.inline, query.template.id) {
        (Some(inline), None) => (inline, true),
        (None, Some(id)) => parse_template_id(&index, &rtxn, &id)?,
        (Some(_), Some(_)) => return Err(MultipleTemplates),
        (None, None) => return Err(MissingTemplate),
    };

    let fields_already_present = query
        .input
        .as_ref()
        .is_some_and(|i| i.inline.as_ref().is_some_and(|i| i.get("fields").is_some()));
    let fields_probably_used = template.as_str().is_none_or(|s| s.contains("fields"));
    let has_inline_doc = query
        .input
        .as_ref()
        .is_some_and(|i| i.inline.as_ref().is_some_and(|i| i.get("doc").is_some()));
    let has_document_id = query.input.as_ref().is_some_and(|i| i.document_id.is_some());
    let has_doc = has_inline_doc || has_document_id;
    let insert_fields =
        fields_available && has_doc && fields_probably_used && !fields_already_present;
    if has_inline_doc && has_document_id {
        return Err(BothInlineDocAndDocId);
    }

    let mut rendered = Value::Null;
    if let Some(input) = query.input {
        let media = input.inline.unwrap_or_default();
        let mut object = liquid::to_object(&media).unwrap();

        if let Some(doc) = media.get("doc") {
            if insert_fields {
                let fields = get_inline_document_fields(&index, &rtxn, doc)?
                    .map_err(|_| CouldNotHandleInput)?;
                object.insert("fields".into(), fields.to_value());
            }
        }

        if let Some(document_id) = input.document_id {
            let (document, fields) = get_document(&index, &rtxn, &document_id, insert_fields)?
                .ok_or_else(|| DocumentNotFound(document_id))?;

            object.insert("doc".into(), document);
            if let Some(fields) = fields {
                object.insert("fields".into(), fields);
            }
        }

        let json_template = JsonTemplate::new(template.clone()).map_err(TemplateParsing)?;

        rendered = json_template.render(&object).map_err(TemplateRendering)?;
    }

    Ok(RenderResult { template, rendered })
}

#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQuery {
    #[deserr(error = DeserrJsonError<InvalidRenderTemplate>)]
    pub template: RenderQueryTemplate,
    #[deserr(default, error = DeserrJsonError<InvalidRenderInput>)]
    pub input: Option<RenderQueryInput>,
}

#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderTemplate>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryTemplate {
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateId>)]
    pub id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateInline>)]
    pub inline: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderInput>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryInput {
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputDocumentId>)]
    pub document_id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputInline>)]
    pub inline: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct RenderResult {
    template: serde_json::Value,
    rendered: serde_json::Value,
}
