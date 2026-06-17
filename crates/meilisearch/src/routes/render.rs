use std::cell::RefCell;
use std::num::NonZeroUsize;
use std::sync::RwLock;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use bumpalo::Bump;
use bumparaw_collections::RawMap;
use deserr::actix_web::AwebJson;
use index_scheduler::{IndexScheduler, RoFeatures};
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{InvalidRenderInput, InvalidRenderTemplate};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::prompt::{Prompt, PromptData};
use meilisearch_types::milli::update::new::document::DocumentFromDb;
use meilisearch_types::milli::vector::json_template::{self, JsonTemplate};
use meilisearch_types::milli::{FieldIdMapWithMetadata, FieldsIdsMap, GlobalFieldsIdsMap};
use meilisearch_types::{heed, milli, Index};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::Value;
use tracing::debug;
use utoipa::ToSchema;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::DoubleActionPolicy;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::render_analytics::RenderAggregator;

#[routes::routes(
    routes("" => post(render_post)),
    tag = "Template",
    tags((
        name = "Render templates",
        description = "The /render-template route allows rendering templates used by Meilisearch.",
        external_docs(
            url = "https://www.meilisearch.com/docs/reference/api/render-template",
            description = "Render template API reference"
        ),
    )),
)]
pub struct RenderApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(render_post))));
}

/// Render documents with POST
#[routes::path(
    security(("Bearer" = ["settings.get,documents.get", "*.get", "*"])),
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
                "message": "Document with ID `9999` not found in index `movies`.",
                "code": "render_document_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#render_document_not_found"
            }
        )),
        (status = 400, description = "Parameters are incorrect", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "cannot find embedder `default` in index `movies`",
                "code": "invalid_render_template",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#invalid_render_template_id"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        ))
    )
)]
pub async fn render_post(
    index_scheduler: GuardedData<
        DoubleActionPolicy<{ actions::SETTINGS_GET }, { actions::DOCUMENTS_GET }>,
        Data<IndexScheduler>,
    >,
    params: AwebJson<RenderQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let query = params.into_inner();
    debug!(parameters = ?query, "Render document");
    let mut aggregate = RenderAggregator::from_query(&query);
    let features = index_scheduler.features();
    features.check_render_route("calling the /render-template route")?;

    let RenderQuery { template, input } = query;

    let template_index_uid = template.index_uid.as_deref();
    let input_index_uid = input.as_ref().and_then(|input| input.index_uid.as_deref());

    // check index permissions
    {
        match (template_index_uid, input_index_uid) {
            (None, None) => (),
            (None, Some(index_uid)) | (Some(index_uid), None) => {
                if !index_scheduler.filters().is_index_authorized(index_uid) {
                    return Err(AuthenticationError::InvalidToken.into());
                }
            }
            (Some(template_index_uid), Some(input_index_uid))
                if template_index_uid == input_index_uid =>
            {
                // can skip second check
                if !index_scheduler.filters().is_index_authorized(template_index_uid) {
                    return Err(AuthenticationError::InvalidToken.into());
                }
            }
            (Some(template_index_uid), Some(input_index_uid)) => {
                // check both indexes
                if !index_scheduler.filters().is_index_authorized(template_index_uid)
                    || !index_scheduler.filters().is_index_authorized(input_index_uid)
                {
                    return Err(AuthenticationError::InvalidToken.into());
                }
            }
        }
    }

    let result: Result<(RenderingTemplate, Option<Value>), Error> =
        tokio::task::spawn_blocking(move || {
            let template_index_uid = template.index_uid.as_deref();
            let input_index_uid = input.as_ref().and_then(|input| input.index_uid.as_deref());

            let doc_alloc = Bump::new();

            let (template, template_index_rtxn) =
                fetch_template(&index_scheduler, features, &template)?;

            let rendered = if let Some(input) = &input {
                let input_index;
                let input_index_rtxn_fidmap = match (input_index_uid, template_index_uid) {
                    (None, _) => {
                        // close index that will not longer be in used
                        drop(template_index_rtxn);
                        None
                    }
                    (Some(input_index_uid), Some(template_index_uid))
                        if input_index_uid == template_index_uid =>
                    {
                        // unwrap: template_index_uid => template_index_rtxn
                        let (index, rtxn) = template_index_rtxn.unwrap();
                        input_index = index;
                        let fidmap = input_index.fields_ids_map_with_metadata(&rtxn)?;
                        Some((&input_index, rtxn, fidmap))
                    }
                    (Some(index_uid), _) => {
                        // avoid simultaneously opening several indexes
                        drop(template_index_rtxn);
                        input_index = index_scheduler.index(index_uid).map_err(|error| {
                            Error::CannotOpenIndex { error, index: index_uid.to_string() }
                        })?;
                        let input_index_rtxn =
                            input_index.read_txn().map_err(milli::Error::from)?;
                        let fidmap = input_index.fields_ids_map_with_metadata(&input_index_rtxn)?;
                        Some((&input_index, input_index_rtxn, fidmap))
                    }
                };

                let input = fetch_input(
                    input,
                    features,
                    input_index_rtxn_fidmap
                        .as_ref()
                        .map(|(index, rtxn, fidmap)| (*index, rtxn, fidmap.as_fields_ids_map())),
                    &doc_alloc,
                )?;

                let fields_ids_map = input_index_rtxn_fidmap.as_ref().map(|(_, _, fidmap)| fidmap);

                Some(render_template(&template, &input, fields_ids_map, &doc_alloc)?)
            } else {
                None
            };

            Ok((template, rendered))
        })
        .await?;

    if result.is_ok() {
        aggregate.succeed();
    }
    analytics.publish(aggregate, &req);

    let (template, rendered) = result?;

    let template = template.into_value();

    let result = RenderResult { template, rendered };

    debug!(returns = ?result, "Render document");
    Ok(HttpResponse::Ok().json(result))
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("error while fetching template: {0}")]
    Template(#[from] FetchTemplateError),
    #[error("error while fetching input: {0}")]
    Input(#[from] FetchInputError),
    #[error("error while rendering template: {0}")]
    Render(#[from] RenderError),
    #[error("internal error: {0}")]
    Milli(#[from] milli::Error),
    #[error("Cannot open index `{index}`: {error}")]
    CannotOpenIndex { error: index_scheduler::Error, index: String },
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Error::Template(error) => error.error_code(),
            Error::Input(error) => error.error_code(),
            Error::Render(error) => error.error_code(),
            Error::Milli(error) => error.error_code(),
            Error::CannotOpenIndex { error, index: _ } => error.error_code(),
        }
    }
}

fn render_template(
    template: &RenderingTemplate,
    input: &RenderableInput,
    field_id_map: Option<&FieldIdMapWithMetadata>,
    doc_alloc: &Bump,
) -> Result<Value, RenderError> {
    let field_id_map = field_id_map.cloned().unwrap_or_else(FieldIdMapWithMetadata::empty);
    let field_id_map = RwLock::new(field_id_map);
    let field_id_map = RefCell::new(GlobalFieldsIdsMap::new(&field_id_map));

    template.render(input, &field_id_map, doc_alloc)
}

#[derive(Debug, thiserror::Error)]
enum FetchInputError {
    #[error("parameter `{disallowed_param}` disallowed for kind `{kind}`")]
    DisallowedParameterForKind { kind: RenderQueryInputKind, disallowed_param: &'static str },
    #[error("parameter `{missing_param}` missing for kind `{kind}`")]
    MissingParameterForKind { kind: RenderQueryInputKind, missing_param: &'static str },
    #[error("internal error: {0}")]
    Heed(#[from] heed::Error),
    #[error("internal error: {0}")]
    Milli(#[from] Box<milli::Error>),
    #[error("document `{docid}` not found in `{index_uid}`")]
    DocumentNotFound { index_uid: String, docid: String },
    #[error("parsing inline document: {error}\n  - Note: the inline document must be a JSON map")]
    ParseInlineDocument { error: serde_json::Error },
    #[error("parsing inline search: {error}\n  - Note: the inline search query must be a JSON map containing `q` and/or `media`")]
    ParseInlineSearch { error: serde_json::Error },
    #[error("{error}")]
    Features { error: Box<index_scheduler::Error> },
    #[error("`q` is not a string")]
    ParseInlineSearchQ,
}

impl ErrorCode for FetchInputError {
    fn error_code(&self) -> Code {
        match self {
            FetchInputError::DisallowedParameterForKind { .. }
            | FetchInputError::ParseInlineDocument { .. }
            | FetchInputError::ParseInlineSearch { .. }
            | FetchInputError::ParseInlineSearchQ
            | FetchInputError::MissingParameterForKind { .. } => Code::InvalidRenderInput,
            FetchInputError::Heed(_) | FetchInputError::Milli(_) => Code::Internal,
            FetchInputError::DocumentNotFound { .. } => Code::RenderDocumentNotFound,
            FetchInputError::Features { error } => error.error_code(),
        }
    }
}

fn fetch_input<'doc>(
    RenderQueryInput { kind, index_uid, id, inline }: &'doc RenderQueryInput,
    features: RoFeatures,
    index_rtxn_fidmap: Option<(
        &'doc Index,
        &'doc RoTxn<'doc, heed::WithoutTls>,
        &'doc FieldsIdsMap,
    )>,
    doc_alloc: &'doc Bump,
) -> Result<RenderableInput<'doc>, FetchInputError> {
    let kind = *kind;
    Ok(match kind {
        RenderQueryInputKind::IndexDocument => {
            let index_uid =
                index_uid.as_deref().ok_or(FetchInputError::MissingParameterForKind {
                    kind,
                    missing_param: "indexUid",
                })?;

            let id = id
                .as_deref()
                .ok_or(FetchInputError::MissingParameterForKind { kind, missing_param: "id" })?;

            if inline.is_some() {
                return Err(FetchInputError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "inline",
                });
            }

            // unwrap: always some when there is an index_uid
            let (index, rtxn, field_id_map) = index_rtxn_fidmap.unwrap();
            let internal_docid =
                index.external_documents_ids().get(rtxn, id)?.ok_or_else(|| {
                    FetchInputError::DocumentNotFound {
                        index_uid: index_uid.to_string(),
                        docid: id.to_string(),
                    }
                })?;
            // unwrap: DB is corrupted if the external id points to an internal id that is not found in the DB.
            let doc = DocumentFromDb::new(internal_docid, rtxn, index, field_id_map)
                .map_err(|err| FetchInputError::Milli(Box::new(err)))?
                .unwrap();

            RenderableInput::IndexDocument(doc)
        }
        RenderQueryInputKind::InlineDocument => {
            if index_uid.is_some() {
                return Err(FetchInputError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "index_uid",
                });
            }

            if id.is_some() {
                return Err(FetchInputError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "id",
                });
            }

            let inline = inline.as_ref().ok_or(FetchInputError::MissingParameterForKind {
                kind,
                missing_param: "inline",
            })?;

            let doc = to_raw_map(inline, doc_alloc)
                .map_err(|error| FetchInputError::ParseInlineDocument { error })?;

            RenderableInput::InlineDocument(doc)
        }
        RenderQueryInputKind::InlineSearch => {
            features
                .check_multimodal("rendering search")
                .map_err(|error| FetchInputError::Features { error: error.into() })?;

            if index_uid.is_some() {
                return Err(FetchInputError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "index_uid",
                });
            }

            if id.is_some() {
                return Err(FetchInputError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "id",
                });
            }

            let inline = inline.as_ref().ok_or(FetchInputError::MissingParameterForKind {
                kind,
                missing_param: "inline",
            })?;

            let search = to_raw_map(inline, doc_alloc)
                .map_err(|error| FetchInputError::ParseInlineSearch { error })?;

            let q = if let Some(q) = search.get("q") {
                let bumparaw_collections::Value::String(q) =
                    bumparaw_collections::Value::from_raw_value(q, doc_alloc)
                        .map_err(|error| FetchInputError::ParseInlineSearch { error })?
                else {
                    return Err(FetchInputError::ParseInlineSearchQ);
                };
                Some(q)
            } else {
                None
            };

            RenderableInput::Search { q, media: search.get("media") }
        }
    })
}

fn to_raw_map<'doc>(
    inline: &Value,
    doc_alloc: &'doc Bump,
) -> Result<RawMap<'doc>, serde_json::Error> {
    let inline = doc_alloc.alloc_str(&serde_json::to_string(&inline)?);
    let inline: &RawValue = serde_json::from_slice(inline.as_bytes())?;
    RawMap::from_raw_value(inline, doc_alloc)
}

enum RenderableInput<'doc> {
    Search { q: Option<&'doc str>, media: Option<&'doc RawValue> },
    InlineDocument(RawMap<'doc>),
    IndexDocument(DocumentFromDb<'doc, FieldsIdsMap>),
}

#[derive(Debug, thiserror::Error)]
enum FetchTemplateError {
    #[error("parameter `{disallowed_param}` disallowed for kind `{kind}`")]
    DisallowedParameterForKind { kind: RenderQueryTemplateKind, disallowed_param: &'static str },
    #[error("parameter `{missing_param}` missing for kind `{kind}`")]
    MissingParameterForKind { kind: RenderQueryTemplateKind, missing_param: &'static str },
    #[error("cannot find embedder `{embedder}` in index `{index_uid}`")]
    MissingEmbedder { index_uid: String, embedder: String },
    #[error("cannot find {fragment_kind} fragment `{fragment}` for embedder `{embedder}` in index `{index_uid}`")]
    MissingFragment {
        index_uid: String,
        embedder: String,
        fragment: String,
        fragment_kind: FragmentKind,
    },
    #[error("embedder `{embedder}` in index `{index_uid}` does not use fragments.")]
    NotAFragmentEmbedder { index_uid: String, embedder: String },
    #[error("embedder `{embedder}` in index `{index_uid}` does not use a document template.")]
    NotADocumentTemplateEmbedder { index_uid: String, embedder: String },
    #[error("{0}")]
    InlineTemplateParsing(#[from] milli::prompt::error::NewPromptError),
    #[error("inline document templates must be strings")]
    InlineTemplateNotAString,
    #[error("{}", error.parsing_error(""))]
    InlineFragmentParsing { error: json_template::Error },
    #[error("internal error: {0}")]
    Heed(#[from] heed::Error),
    #[error("{error}")]
    Features { error: Box<index_scheduler::Error> },
    #[error("cannot open index `{index}`: {error}")]
    CannotOpenIndex { error: Box<index_scheduler::Error>, index: String },
}

impl ErrorCode for FetchTemplateError {
    fn error_code(&self) -> Code {
        match self {
            FetchTemplateError::DisallowedParameterForKind { .. }
            | FetchTemplateError::MissingParameterForKind { .. }
            | FetchTemplateError::MissingEmbedder { .. }
            | FetchTemplateError::MissingFragment { .. }
            | FetchTemplateError::NotAFragmentEmbedder { .. }
            | FetchTemplateError::InlineTemplateNotAString
            | FetchTemplateError::NotADocumentTemplateEmbedder { .. } => {
                Code::InvalidRenderTemplate
            }
            FetchTemplateError::InlineTemplateParsing(_)
            | FetchTemplateError::InlineFragmentParsing { .. } => Code::TemplateParsingError,
            FetchTemplateError::Heed(_) => Code::Internal,
            FetchTemplateError::Features { error } => error.error_code(),
            FetchTemplateError::CannotOpenIndex { .. } => Code::IndexNotFound,
        }
    }
}

enum RenderQueryTemplateView<'a> {
    DocumentTemplate {
        index_uid: &'a str,
        index: Index,
        rtxn: RoTxn<'static, heed::WithoutTls>,
        embedder: &'a str,
        document_template_max_bytes: Option<NonZeroUsize>,
    },
    ChatDocumentTemplate {
        index: Index,
        rtxn: RoTxn<'static, heed::WithoutTls>,
        document_template_max_bytes: Option<NonZeroUsize>,
    },
    IndexingFragment {
        index_uid: &'a str,
        index: Index,
        rtxn: RoTxn<'static, heed::WithoutTls>,
        embedder: &'a str,
        fragment: &'a str,
    },
    SearchFragment {
        index_uid: &'a str,
        index: Index,
        rtxn: RoTxn<'static, heed::WithoutTls>,
        embedder: &'a str,
        fragment: &'a str,
    },
    InlineDocumentTemplate {
        inline: &'a Value,
        document_template_max_bytes: Option<NonZeroUsize>,
    },
    InlineFragment {
        inline: &'a Value,
    },
}

impl<'a> RenderQueryTemplateView<'a> {
    #[allow(clippy::type_complexity)]
    pub fn fetch(
        self,
    ) -> Result<
        (RenderingTemplate, Option<(Index, RoTxn<'static, heed::WithoutTls>)>),
        FetchTemplateError,
    > {
        use RenderQueryTemplateView::*;
        Ok(match self {
            DocumentTemplate { index_uid, index, rtxn, embedder, document_template_max_bytes } => {
                let configs = index.embedding_configs().embedding_configs(&rtxn)?;
                let config = configs
                    .into_iter()
                    .find(|config| config.name == embedder)
                    .ok_or_else(|| FetchTemplateError::MissingEmbedder {
                        index_uid: index_uid.to_string(),
                        embedder: embedder.to_string(),
                    })?;

                if !config.config.embedder_options.has_document_template() {
                    return Err(FetchTemplateError::NotADocumentTemplateEmbedder {
                        index_uid: index_uid.to_string(),
                        embedder: embedder.to_string(),
                    });
                }

                let mut prompt = config.config.prompt;
                if let Some(document_template_max_bytes) = document_template_max_bytes {
                    prompt.max_bytes = Some(document_template_max_bytes);
                }

                // unwrap: template was validated when sending the settings
                (RenderingTemplate::Template(prompt.try_into().unwrap()), Some((index, rtxn)))
            }
            ChatDocumentTemplate { index, rtxn, document_template_max_bytes } => {
                let chat = index.chat_config(&rtxn)?;

                let mut prompt = chat.prompt;
                if let Some(document_template_max_bytes) = document_template_max_bytes {
                    prompt.max_bytes = Some(document_template_max_bytes);
                }

                // unwrap: template was validated when sending the settings
                (RenderingTemplate::Template(prompt.try_into().unwrap()), Some((index, rtxn)))
            }
            IndexingFragment { index_uid, index, rtxn, embedder, fragment } => {
                let configs = index.embedding_configs().embedding_configs(&rtxn)?;
                let config = configs
                    .into_iter()
                    .find(|config| config.name == embedder)
                    .ok_or_else(|| FetchTemplateError::MissingEmbedder {
                        index_uid: index_uid.to_string(),
                        embedder: embedder.to_string(),
                    })?;

                if !config.config.embedder_options.has_fragments() {
                    return Err(FetchTemplateError::NotAFragmentEmbedder {
                        index_uid: index_uid.to_string(),
                        embedder: embedder.to_string(),
                    });
                }

                let fragment =
                    config.config.embedder_options.indexing_fragment(fragment).ok_or_else(
                        || FetchTemplateError::MissingFragment {
                            index_uid: index_uid.to_string(),
                            embedder: embedder.to_string(),
                            fragment: fragment.to_string(),
                            fragment_kind: FragmentKind::Indexing,
                        },
                    )?;

                (
                    // unwrap: validated in configuration
                    RenderingTemplate::Fragment(JsonTemplate::new(fragment.clone()).unwrap()),
                    Some((index, rtxn)),
                )
            }
            SearchFragment { index_uid, index, rtxn, embedder, fragment } => {
                let configs = index.embedding_configs().embedding_configs(&rtxn)?;
                let config = configs
                    .into_iter()
                    .find(|config| config.name == embedder)
                    .ok_or_else(|| FetchTemplateError::MissingFragment {
                        index_uid: index_uid.to_string(),
                        embedder: embedder.to_string(),
                        fragment: fragment.to_string(),
                        fragment_kind: FragmentKind::Indexing,
                    })?;

                if !config.config.embedder_options.has_fragments() {
                    return Err(FetchTemplateError::NotAFragmentEmbedder {
                        index_uid: index_uid.to_string(),
                        embedder: embedder.to_string(),
                    });
                }

                let fragment =
                    config.config.embedder_options.search_fragment(fragment).ok_or_else(|| {
                        FetchTemplateError::MissingFragment {
                            index_uid: index_uid.to_string(),
                            embedder: embedder.to_string(),
                            fragment: fragment.to_string(),
                            fragment_kind: FragmentKind::Search,
                        }
                    })?;

                (
                    // unwrap: validated in configuration
                    RenderingTemplate::Fragment(JsonTemplate::new(fragment.clone()).unwrap()),
                    Some((index, rtxn)),
                )
            }
            InlineDocumentTemplate { inline, document_template_max_bytes } => {
                let inline = inline.as_str().ok_or(FetchTemplateError::InlineTemplateNotAString)?;

                (
                    RenderingTemplate::Template(Prompt::new(
                        inline.to_owned(),
                        document_template_max_bytes,
                    )?),
                    None,
                )
            }
            InlineFragment { inline } => (
                RenderingTemplate::Fragment(
                    JsonTemplate::new(inline.clone())
                        .map_err(|error| FetchTemplateError::InlineFragmentParsing { error })?,
                ),
                None,
            ),
        })
    }
}

#[allow(clippy::type_complexity)] // the return type is no very beautiful but I don't see any point in hiding it
fn fetch_template<'a>(
    index_scheduler: &'a IndexScheduler,
    features: RoFeatures,
    template: &'a RenderQueryTemplate,
) -> Result<
    (RenderingTemplate, Option<(Index, RoTxn<'static, heed::WithoutTls>)>),
    FetchTemplateError,
> {
    let RenderQueryTemplate {
        kind,
        index_uid,
        embedder,
        fragment,
        inline,
        document_template_max_bytes,
    } = template;
    let kind = *kind;
    let document_template_max_bytes = *document_template_max_bytes;

    let template = match kind {
        RenderQueryTemplateKind::DocumentTemplate => {
            if fragment.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "fragment",
                });
            }
            if inline.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "inline",
                });
            }
            let index_uid =
                index_uid.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "index_uid",
                })?;

            let embedder =
                embedder.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "embedder",
                })?;
            let index = index_scheduler.index(index_uid).map_err(|error| {
                FetchTemplateError::CannotOpenIndex {
                    error: error.into(),
                    index: index_uid.to_string(),
                }
            })?;
            let rtxn = index.static_read_txn()?;

            RenderQueryTemplateView::DocumentTemplate {
                index_uid,
                index,
                rtxn,
                embedder,
                document_template_max_bytes,
            }
        }
        RenderQueryTemplateKind::ChatDocumentTemplate => {
            features
                .check_chat_completions("accessing chat settings")
                .map_err(|error| FetchTemplateError::Features { error: error.into() })?;
            let index_uid =
                index_uid.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "index_uid",
                })?;

            if embedder.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "embedder",
                });
            }
            if fragment.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "fragment",
                });
            }
            if inline.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "inline",
                });
            }

            let index = index_scheduler.index(index_uid).map_err(|error| {
                FetchTemplateError::CannotOpenIndex {
                    error: error.into(),
                    index: index_uid.to_string(),
                }
            })?;
            let rtxn = index.static_read_txn()?;

            RenderQueryTemplateView::ChatDocumentTemplate {
                index,
                rtxn,
                document_template_max_bytes,
            }
        }
        RenderQueryTemplateKind::IndexingFragment => {
            features
                .check_multimodal("accessing fragments")
                .map_err(|error| FetchTemplateError::Features { error: error.into() })?;
            let index_uid =
                index_uid.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "index_uid",
                })?;

            let embedder =
                embedder.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "embedder",
                })?;

            let fragment =
                fragment.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "fragment",
                })?;

            if inline.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "inline",
                });
            }

            if document_template_max_bytes.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "document_template_max_bytes",
                });
            }

            let index = index_scheduler.index(index_uid).map_err(|error| {
                FetchTemplateError::CannotOpenIndex {
                    error: error.into(),
                    index: index_uid.to_string(),
                }
            })?;
            let rtxn = index.static_read_txn()?;

            RenderQueryTemplateView::IndexingFragment { index_uid, index, rtxn, embedder, fragment }
        }
        RenderQueryTemplateKind::SearchFragment => {
            features
                .check_multimodal("accessing fragments")
                .map_err(|error| FetchTemplateError::Features { error: error.into() })?;
            let index_uid =
                index_uid.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "index_uid",
                })?;

            let embedder =
                embedder.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "embedder",
                })?;
            let fragment =
                fragment.as_deref().ok_or(FetchTemplateError::MissingParameterForKind {
                    kind,
                    missing_param: "fragment",
                })?;
            if inline.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "inline",
                });
            }
            if document_template_max_bytes.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "document_template_max_bytes",
                });
            }

            let index = index_scheduler.index(index_uid).map_err(|error| {
                FetchTemplateError::CannotOpenIndex {
                    error: error.into(),
                    index: index_uid.to_string(),
                }
            })?;
            let rtxn = index.static_read_txn()?;

            RenderQueryTemplateView::SearchFragment { index_uid, index, rtxn, embedder, fragment }
        }
        RenderQueryTemplateKind::InlineDocumentTemplate => {
            if index_uid.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "index_uid",
                });
            }
            if embedder.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "embedder",
                });
            }
            if fragment.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "fragment",
                });
            }
            let inline = inline.as_ref().ok_or(FetchTemplateError::MissingParameterForKind {
                kind,
                missing_param: "inline",
            })?;

            RenderQueryTemplateView::InlineDocumentTemplate { inline, document_template_max_bytes }
        }
        RenderQueryTemplateKind::InlineFragment => {
            features
                .check_multimodal("rendering an inline fragment")
                .map_err(|error| FetchTemplateError::Features { error: error.into() })?;
            if index_uid.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "index_uid",
                });
            }
            if embedder.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "embedder",
                });
            }
            if fragment.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "fragment",
                });
            }

            let inline = inline.as_ref().ok_or(FetchTemplateError::MissingParameterForKind {
                kind,
                missing_param: "inline",
            })?;
            if document_template_max_bytes.is_some() {
                return Err(FetchTemplateError::DisallowedParameterForKind {
                    kind,
                    disallowed_param: "document_template_max_bytes",
                });
            }

            RenderQueryTemplateView::InlineFragment { inline }
        }
    };

    template.fetch()
}

enum RenderingTemplate {
    Template(milli::prompt::Prompt),
    Fragment(milli::vector::json_template::JsonTemplate),
}

impl RenderingTemplate {
    // panics if input is IndexDocument and field_id_map is None
    pub fn render<'doc>(
        &self,
        input: &RenderableInput<'doc>,
        field_id_map: &RefCell<GlobalFieldsIdsMap>,
        doc_alloc: &'doc Bump,
    ) -> Result<Value, RenderError> {
        Ok(match (input, self) {
            (RenderableInput::IndexDocument(doc), RenderingTemplate::Template(prompt)) => {
                Value::String(
                    prompt
                        .render_document(None, doc, field_id_map, doc_alloc)
                        .map_err(RenderError::Prompt)?
                        .into(),
                )
            }
            (RenderableInput::Search { .. }, RenderingTemplate::Template(_)) => {
                return Err(RenderError::CannotRenderTemplateForSearch)
            }
            (RenderableInput::Search { q, media }, RenderingTemplate::Fragment(fragment)) => {
                let media = media
                    .map(serde_json::to_value)
                    .transpose()
                    // unwrap: "media" was already parsed as JSON as part of `inline`
                    .unwrap();
                fragment
                    .render_search(*q, media.as_ref())
                    .map_err(|error| RenderError::Fragment { error })?
            }
            (RenderableInput::InlineDocument(doc), RenderingTemplate::Template(prompt)) => {
                Value::String(
                    prompt
                        .render_document(None, doc, field_id_map, doc_alloc)
                        .map_err(RenderError::Prompt)?
                        .into(),
                )
            }
            (RenderableInput::InlineDocument(doc), RenderingTemplate::Fragment(fragment)) => {
                fragment
                    .render_document(doc, doc_alloc)
                    .map_err(|error| RenderError::Fragment { error })?
            }
            (RenderableInput::IndexDocument(doc), RenderingTemplate::Fragment(fragment)) => {
                fragment
                    .render_document(doc, doc_alloc)
                    .map_err(|error| RenderError::Fragment { error })?
            }
        })
    }

    pub fn into_value(self) -> Value {
        match self {
            RenderingTemplate::Template(prompt) => {
                let data: PromptData = prompt.into();
                Value::String(data.template)
            }
            RenderingTemplate::Fragment(json_template) => json_template.into_template(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FragmentKind {
    Indexing,
    Search,
}

impl std::fmt::Display for FragmentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FragmentKind::Indexing => f.write_str("indexing"),
            FragmentKind::Search => f.write_str("search"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum RenderError {
    #[error("{0}")]
    Prompt(#[from] milli::prompt::error::RenderPromptError),
    #[error("{}", error.rendering_error(""))]
    Fragment { error: json_template::Error },
    #[error("cannot render a document template with a search query")]
    CannotRenderTemplateForSearch,
}

impl ErrorCode for RenderError {
    fn error_code(&self) -> Code {
        Code::TemplateRenderingError
    }
}

#[routes::request]
#[derive(Debug, Clone, PartialEq)]
pub struct RenderQuery {
    /// Template/fragment to fetch for rendering.
    ///
    /// Use its `kind` parameter to determine the type of template or fragment to fetch.
    #[request(required, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub template: RenderQueryTemplate,
    /// Input for the template to fetch.
    ///
    /// Input is injected in the template to render the final string/JSON object.
    ///
    /// If `null` or missing, the template will not be rendered.
    ///
    /// Use its `kind` parameter to determine the type of document to fetch.
    #[request(default, error = DeserrJsonError<InvalidRenderInput>)]
    pub input: Option<RenderQueryInput>,
}

// implementation note: this is a set as a struct because deserr does not support untagged enums
#[routes::request(override_error = DeserrJsonError<InvalidRenderTemplate>)]
#[derive(Debug, Clone, PartialEq)]
pub struct RenderQueryTemplate {
    /// Kind of template or fragment to fetch.
    ///
    /// Determines which other parameters are allowed and mandatory.
    #[request(required, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub kind: RenderQueryTemplateKind,
    /// Index to fetch the template or fragment from.
    ///
    /// - Mandatory for `kind`s: `documentTemplate`, `chatDocumentTemplate`, `indexingFragment` and `searchFragment`.
    #[request(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub index_uid: Option<IndexUid>,
    /// Embedder to fetch the template or fragment from.
    ///
    /// - Mandatory for `kind`s: `documentTemplate`, `chatDocumentTemplate`, `indexingFragment` and `searchFragment`.
    #[request(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub embedder: Option<String>,
    /// Name of the fragment to fetch.
    ///
    /// - Mandatory for `kind`s: `indexingFragment` and `searchFragment`.
    #[request(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub fragment: Option<String>,
    /// Inline value of the template or fragment.
    ///
    /// - Mandatory for `kind`s: `inlineDocumentTemplate` and `inlineFragment`.
    #[request(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub inline: Option<Value>,
    /// If present, truncate document template rendering to the specified number of bytes.
    ///
    /// - Available for `kind`s: `documentTemplate`, `inlineDocumentTemplate` and `chatDocumentTemplate`
    /// - If present for `documentTemplate` overrides the setting of the index.
    /// - If missing for `documentTemplate`, the setting of the index is used.
    /// - If missing for `inlineDocumentTemplate`, the default value of 400 bytes is used.
    #[request(default, error = DeserrJsonError<InvalidRenderTemplate>, schema_type = Option<u64>)]
    pub document_template_max_bytes: Option<NonZeroUsize>,
}

#[routes::request(override_error = DeserrJsonError<InvalidRenderTemplate>)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RenderQueryTemplateKind {
    /// Fetches the fragment associated with the `embedders.embedder.documentTemplate` setting of the specified index
    ///
    /// - Requires `indexUid`, `embedder` to be present and not `null`
    DocumentTemplate,
    /// Fetches the template associated with the `chat.documentTemplate` setting of the specified index
    ///
    /// - Requires `indexUid` to be present and not `null`
    /// - Requires the `chatCompletions` experimental feature
    ChatDocumentTemplate,
    /// Fetches the fragment associated with the `embedders.embedder.indexingFragments.fragment` setting of the specified index
    ///
    /// - Requires `indexUid`, `embedder`, `fragment` to be present and not `null`
    /// - Requires the `multimodal` experimental feature
    IndexingFragment,
    /// Fetches the fragment associated with the `embedders.embedder.searchFragments.fragment` setting of the specified index
    ///
    /// - Requires `indexUid`, `embedder`, `fragment` to be present and not `null`
    /// - Requires the `multimodal` experimental feature
    SearchFragment,
    /// Uses the document template provided inline.
    ///
    /// - Requires `inline` to be present and not `null`
    InlineDocumentTemplate,
    /// Uses the fragment provided inline.
    ///
    /// - Requires `inline` to be present and not `null`
    /// - Requires the `multimodal` experimental feature
    InlineFragment,
}

impl std::fmt::Display for RenderQueryTemplateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenderQueryTemplateKind::DocumentTemplate => f.write_str("documentTemplate"),
            RenderQueryTemplateKind::ChatDocumentTemplate => f.write_str("chatDocumentTemplate"),
            RenderQueryTemplateKind::IndexingFragment => f.write_str("indexingFragment"),
            RenderQueryTemplateKind::SearchFragment => f.write_str("searchFragment"),
            RenderQueryTemplateKind::InlineDocumentTemplate => {
                f.write_str("inlineDocumentTemplate")
            }
            RenderQueryTemplateKind::InlineFragment => f.write_str("inlineFragment"),
        }
    }
}

#[routes::request(override_error = DeserrJsonError<InvalidRenderInput>)]
#[derive(Debug, Clone, PartialEq)]
pub struct RenderQueryInput {
    #[request(required, error = DeserrJsonError<InvalidRenderInput>)]
    pub kind: RenderQueryInputKind,
    #[request(default, error = DeserrJsonError<InvalidRenderInput>)]
    pub index_uid: Option<IndexUid>,
    #[request(default, error = DeserrJsonError<InvalidRenderInput>)]
    pub id: Option<String>,
    #[request(default, error = DeserrJsonError<InvalidRenderInput>)]
    pub inline: Option<Value>,
}

#[routes::request(override_error = DeserrJsonError<InvalidRenderInput>)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RenderQueryInputKind {
    /// Fetches the document associated with the `id` setting of the specified index
    ///
    /// - Requires `indexUid`, `id` to be present and not `null`
    IndexDocument,
    /// Uses the document specified inline as a JSON object.
    ///
    /// - Requires `inline` to be present.
    InlineDocument,
    /// Uses the search query specified inline as a JSON object.
    ///
    /// - Requires `inline` to be present.
    InlineSearch,
}

impl std::fmt::Display for RenderQueryInputKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenderQueryInputKind::IndexDocument => f.write_str("indexDocument"),
            RenderQueryInputKind::InlineDocument => f.write_str("inlineDocument"),
            RenderQueryInputKind::InlineSearch => f.write_str("inlineSearch"),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct RenderResult {
    /// **Un**rendered template or fragment, fetched in index or echoed back from inline template in request.
    template: Value,
    /// Result of rendering the template by injecting the `input` to the template.
    ///
    /// `null` if `input` was `null` or missing in the request.
    rendered: Option<Value>,
}
