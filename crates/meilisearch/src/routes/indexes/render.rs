use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use itertools::structs;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::{
    InvalidRenderInput, InvalidRenderInputDocumentId, InvalidRenderInputInline,
    InvalidRenderTemplate, InvalidRenderTemplateId, InvalidRenderTemplateInline,
};
use meilisearch_types::error::Code;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::vector::json_template::{self, JsonTemplate};
use meilisearch_types::serde_cs::vec::CS;
use meilisearch_types::{heed, milli, Index};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;
use utoipa::{IntoParams, OpenApi, ToSchema};

use super::ActionPolicy;
use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::DoubleActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::similar_analytics::{SimilarAggregator, SimilarGET, SimilarPOST};
use crate::search::{
    add_search_rules, perform_similar, RankingScoreThresholdSimilar, RetrieveVectors, Route,
    SearchKind, SimilarQuery, SimilarResult, DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};

#[derive(OpenApi)]
#[openapi(
    paths(render_post),
    tags((
        name = "Render templates",
        description = "The /render route allows rendering templates used by Meilisearch.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/render"),   
    )),
)]
pub struct RenderApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(render_post))));
}

/// Render templates with POST
#[utoipa::path(
    post,
    path = "{indexUid}/render",
    tag = "Render templates",
    security(("Bearer" = ["settings.get", "settings.*", "*.get", "*"])),
    params(("indexUid" = String, Path, example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = RenderQuery,
    responses(
        (status = 200, description = "The rendered result is returned", body = RenderResult, content_type = "application/json", example = json!(
            {
                "rendered": "A Jack Russell called Iko"
            }
        )),
        (status = 404, description = "Template or document not found", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.", // TODO
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
        (status = 400, description = "Template couldn't be rendered", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.", // TODO
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
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
    debug!(parameters = ?query, "Render template");

    //let mut aggregate = SimilarAggregator::<SimilarPOST>::from_query(&query);

    let result = render(index, query).await?;

    // if let Ok(similar) = &similar {
    //     aggregate.succeed(similar);
    // }
    // analytics.publish(aggregate, &req);

    debug!(returns = ?result, "Render template");
    Ok(HttpResponse::Ok().json(result))
}

enum FragmentKind {
    Indexing,
    Search,
}

impl FragmentKind {
    fn adjective(&self) -> &'static str {
        match self {
            FragmentKind::Indexing => "indexing",
            FragmentKind::Search => "search",
        }
    }

    fn adjective_capitalized(&self) -> &'static str {
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
        embedder_name: String,
        available: Vec<String>,
    },
    EmbedderUsesFragments {
        embedder_name: String,
    },
    MissingTemplateAfterEmbedder {
        embedder_name: String,
        available_indexing_fragments: Vec<String>,
        available_search_fragments: Vec<String>,
    },
    UnknownTemplatePrefix {
        embedder_name: String,
        found: String,
        available_indexing_fragments: Vec<String>,
        available_search_fragments: Vec<String>,
    },
    ReponseError(ResponseError),
    MissingFragment {
        embedder_name: String,
        kind: FragmentKind,
        available: Vec<String>,
    },
    FragmentDoesNotExist {
        embedder_name: String,
        fragment_name: String,
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
            EmbedderDoesNotExist { embedder_name, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("Embedder `{embedder_name}` does not exist.\n  Hint: Available embedders are {}.",
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            EmbedderUsesFragments { embedder_name } => ResponseError::from_msg(
                format!("Requested document template for embedder `{embedder_name}` but it uses fragments.\n  Hint: Use `indexingFragments` or `searchFragments` instead."),
                Code::InvalidRenderTemplateId,
            ),
            MissingTemplateAfterEmbedder { embedder_name, mut available_indexing_fragments, mut available_search_fragments } => {
                if available_indexing_fragments.is_empty() && available_search_fragments.is_empty() {
                    ResponseError::from_msg(
                        format!("Missing template id after embedder `{embedder_name}`.\n  Hint: Available fragments: `documentTemplate`."),
                        Code::InvalidRenderTemplateId,
                    )
                } else {
                    available_indexing_fragments.sort_unstable();
                    available_search_fragments.sort_unstable();
                    ResponseError::from_msg(
                        format!("Template ID configured with `embedders.{embedder_name}` but no template kind provided.\n  Hint: Available fragments are {}.",
                            available_indexing_fragments.iter().map(|s| format!("`indexingFragments.{s}`")).chain(
                            available_search_fragments.iter().map(|s| format!("`searchFragments.{s}`"))).collect::<Vec<_>>().join(", ")),
                        Code::InvalidRenderTemplateId,
                    )
                }
            },
            UnknownTemplatePrefix { embedder_name, found, mut available_indexing_fragments, mut available_search_fragments } => {
                if available_indexing_fragments.is_empty() && available_search_fragments.is_empty() {
                    ResponseError::from_msg(
                        format!("Wrong template `{found}` after embedder `{embedder_name}`.\n  Hint: Available fragments: `documentTemplate`."),
                        Code::InvalidRenderTemplateId,
                    )
                } else {
                    available_indexing_fragments.sort_unstable();
                    available_search_fragments.sort_unstable();
                    ResponseError::from_msg(
                        format!("Wrong template `{found}` after embedder `{embedder_name}`.\n  Hint: Available fragments are {}.",
                            available_indexing_fragments.iter().map(|s| format!("`indexingFragments.{s}`")).chain(
                            available_search_fragments.iter().map(|s| format!("`searchFragments.{s}`"))).collect::<Vec<_>>().join(", ")),
                        Code::InvalidRenderTemplateId,
                    )
                }
            },
            ReponseError(response_error) => response_error,
            MissingFragment { embedder_name, kind, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("{} fragment name was not provided.\n  Hint: Available {} fragments for embedder `{embedder_name}` are {}.",
                        kind.adjective_capitalized(),
                        kind.adjective(),
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            FragmentDoesNotExist { embedder_name, fragment_name, kind, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("{} fragment `{fragment_name}` does not exist for embedder `{embedder_name}`.\n  Hint: Available {} fragments are {}.",
                        kind.adjective_capitalized(),
                        kind.adjective(),
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
                Code::InvalidRenderTemplate,
            ),
            TemplateRendering(err) => ResponseError::from_msg(
                format!("Error rendering template: {}", err.rendering_error("input")),
                Code::InvalidRenderTemplate,
            ),
        }
    }
}

async fn render(index: Index, query: RenderQuery) -> Result<RenderResult, RenderError> {
    let rtxn = index.read_txn()?;

    let template = match (query.template.inline, query.template.id) {
        (Some(inline), None) => inline,
        (None, Some(id)) => {
            let mut parts = id.split('.');

            let root = parts.next().ok_or(EmptyTemplateId)?;

            let template = match root {
                "embedders" => {
                    let index_embedding_configs = index.embedding_configs();
                    let embedding_configs = index_embedding_configs.embedding_configs(&rtxn)?;

                    let embedder_name = parts.next().ok_or_else(|| MissingEmbedderName {
                        available: embedding_configs.iter().map(|c| c.name.clone()).collect(),
                    })?;

                    let embedding_config = embedding_configs
                        .iter()
                        .find(|config| config.name == embedder_name)
                        .ok_or_else(|| EmbedderDoesNotExist {
                            embedder_name: embedder_name.to_string(),
                            available: embedding_configs.iter().map(|c| c.name.clone()).collect(),
                        })?;

                    let template_kind =
                        parts.next().ok_or_else(|| MissingTemplateAfterEmbedder {
                            embedder_name: embedder_name.to_string(),
                            available_indexing_fragments: embedding_config
                                .config
                                .embedder_options
                                .indexing_fragments(),
                            available_search_fragments: embedding_config
                                .config
                                .embedder_options
                                .search_fragments(),
                        })?;
                    match template_kind {
                        "documentTemplate" | "documenttemplate" => {
                            if !embedding_config.fragments.as_slice().is_empty() {
                                return Err(EmbedderUsesFragments {
                                    embedder_name: embedder_name.to_string(),
                                });
                            }

                            serde_json::Value::String(
                                embedding_config.config.prompt.template.clone(),
                            )
                        }
                        "indexingFragments" | "indexingfragments" => {
                            let fragment_name = parts.next().ok_or_else(|| MissingFragment {
                                embedder_name: embedder_name.to_string(),
                                kind: FragmentKind::Indexing,
                                available: embedding_config
                                    .config
                                    .embedder_options
                                    .indexing_fragments(),
                            })?;

                            let fragment = embedding_config
                                .config
                                .embedder_options
                                .indexing_fragment(fragment_name)
                                .ok_or_else(|| FragmentDoesNotExist {
                                    embedder_name: embedder_name.to_string(),
                                    fragment_name: fragment_name.to_string(),
                                    kind: FragmentKind::Indexing,
                                    available: embedding_config
                                        .config
                                        .embedder_options
                                        .indexing_fragments(),
                                })?;

                            fragment.clone()
                        }
                        "searchFragments" | "searchfragments" => {
                            let fragment_name = parts.next().ok_or_else(|| MissingFragment {
                                embedder_name: embedder_name.to_string(),
                                kind: FragmentKind::Search,
                                available: embedding_config
                                    .config
                                    .embedder_options
                                    .search_fragments(),
                            })?;

                            let fragment = embedding_config
                                .config
                                .embedder_options
                                .search_fragment(fragment_name)
                                .ok_or_else(|| FragmentDoesNotExist {
                                    embedder_name: embedder_name.to_string(),
                                    fragment_name: fragment_name.to_string(),
                                    kind: FragmentKind::Search,
                                    available: embedding_config
                                        .config
                                        .embedder_options
                                        .search_fragments(),
                                })?;

                            fragment.clone()
                        }
                        found => {
                            return Err(UnknownTemplatePrefix {
                                embedder_name: embedder_name.to_string(),
                                found: found.to_string(),
                                available_indexing_fragments: embedding_config
                                    .config
                                    .embedder_options
                                    .indexing_fragments(),
                                available_search_fragments: embedding_config
                                    .config
                                    .embedder_options
                                    .search_fragments(),
                            })
                        }
                    }
                }
                "chatCompletions" | "chatcompletions" => {
                    let template_name = parts.next().ok_or(MissingChatCompletionTemplate)?;

                    if template_name != "documentTemplate" {
                        return Err(UnknownChatCompletionTemplate(template_name.to_string()));
                    }

                    let chat_config = index.chat_config(&rtxn)?;

                    serde_json::Value::String(chat_config.prompt.template.clone())
                }
                "" => return Err(EmptyTemplateId),
                unknown => {
                    return Err(UnknownTemplateRoot(unknown.to_string()));
                }
            };

            if let Some(next) = parts.next() {
                return Err(LeftOverToken(next.to_string()));
            }

            template
        }
        (Some(_), Some(_)) => return Err(MultipleTemplates),
        (None, None) => return Err(MissingTemplate),
    };

    let mut rendered = Value::Null;
    if let Some(input) = query.input {
        let mut media = input.inline.unwrap_or_default();
        if let Some(document_id) = input.document_id {
            let internal_id = index
                .external_documents_ids()
                .get(&rtxn, &document_id)?
                .ok_or_else(|| DocumentNotFound(document_id.to_string()))?;

            let document = index.document(&rtxn, internal_id)?;

            let fields_ids_map = index.fields_ids_map(&rtxn)?;
            let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();
            let document = milli::obkv_to_json(&all_fields, &fields_ids_map, document)?;
            let document = Value::Object(document);

            if media.insert(String::from("doc"), document).is_some() {
                return Err(BothInlineDocAndDocId);
            }
        }

        let json_template = JsonTemplate::new(template.clone()).map_err(TemplateParsing)?;

        rendered = json_template.render_serializable(&media).map_err(TemplateRendering)?;
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
    id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateInline>)]
    inline: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderInput>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryInput {
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputDocumentId>)]
    document_id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputInline>)]
    inline: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct RenderResult {
    template: serde_json::Value,
    rendered: serde_json::Value,
}
