use std::cell::RefCell;
use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use bumpalo::Bump;
use bumparaw_collections::RawMap;
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use liquid::ValueView;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidRenderInput, InvalidRenderInputDocumentId, InvalidRenderInputInline,
    InvalidRenderTemplate,
};
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::prompt::{
    build_doc, build_doc_fields, OwnedFields, Prompt, PromptData,
};
use meilisearch_types::milli::update::new::document::DocumentFromDb;
use meilisearch_types::milli::vector::db::IndexEmbeddingConfig;
use meilisearch_types::milli::vector::json_template::{self, JsonTemplate};
use meilisearch_types::milli::vector::EmbedderOptions;
use meilisearch_types::milli::{FieldsIdsMap, GlobalFieldsIdsMap, Span, Token};
use meilisearch_types::{heed, milli, Index};
use serde::Serialize;
use serde_json::Value;
use tracing::debug;
use utoipa::ToSchema;
use wip::{fixme, wip, WipCloneExt, WipOptionExt as _, WipResultExt as _};

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
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/render-template"),
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
    let query = params.into_inner();
    debug!(parameters = ?query, "Render document");

    let RenderQuery { template, input } = query;

    let template_index_uid = template.index_uid.as_deref();
    let input_index_uid = input.as_ref().and_then(|input| input.index_uid.as_deref());

    // check index permissions
    {
        if let Some(index_uid) = template_index_uid {
            if !index_scheduler.filters().is_index_authorized(index_uid) {
                return Err(AuthenticationError::InvalidToken.into());
            }
        }

        if let Some(index_uid) = input_index_uid {
            if !index_scheduler.filters().is_index_authorized(index_uid) {
                return Err(AuthenticationError::InvalidToken.into());
            }
        }
    }
    wip::fixme!("document template max bytes");

    let result = tokio::task::spawn_blocking(|| {
        let template_index = if let Some(index_uid) = template_index_uid {
            Some(index_scheduler.index(index_uid))
        } else {
            None
        }
        .transpose()?;

        let template_index_rtxn =
            template_index.as_ref().map(|index| index.read_txn()).transpose()?;

        let template =
            fetch_template(&template, template_index.as_ref(), template_index_rtxn.as_ref())?;

        let rendered = if let Some(input) = input {
            let input_index;
            let (input_index, input_index_rtxn, fields_ids_map) =
                match (input_index_uid, template_index_uid) {
                    (None, _) => {
                        // avoid simultaneously opening several indexes
                        drop(template_index_rtxn);
                        drop(template_index);
                        (None, None, Default::default())
                    }
                    (Some(input_index_uid), Some(template_index_uid))
                        if input_index_uid == template_index_uid =>
                    {
                        let fidmap = template_index
                            .as_ref()
                            .unwrap()
                            .fields_ids_map(template_index_rtxn.as_ref().unwrap())
                            .wip();
                        // reuse previous index and txn
                        (template_index, template_index_rtxn, fidmap)
                    }
                    (Some(index_uid), _) => {
                        // avoid simultaneously opening several indexes
                        drop(template_index_rtxn);
                        drop(template_index);
                        input_index = index_scheduler.index(index_uid)?;
                        let input_index_rtxn = input_index.read_txn()?;
                        let fidmap = input_index.fields_ids_map(&input_index_rtxn).wip();
                        (Some(input_index), Some(input_index_rtxn), fidmap)
                    }
                };

            let input = fetch_input(
                &input,
                input_index.as_ref(),
                input_index_rtxn.as_ref(),
                &fields_ids_map,
            )?;

            drop(input_index_rtxn);
            drop(input_index);

            Some(render_template(&template, &input)?)
        } else {
            None
        };

        Ok((template, rendered))
    })
    .await?;

    let mut aggregate = RenderAggregator::from_query(&query);

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

fn render_template(
    template: &RenderingTemplate,
    input: &RenderableInput,
) -> Result<Value, RenderError> {
    template.render(input, field_id_map, doc_alloc)
}

fn fetch_input<'doc>(
    RenderQueryInput { index_uid, document_id, inline_document, inline_query }: &RenderQueryInput,
    input_index: Option<&Index>,
    input_index_rtxn: Option<&RoTxn<'_, heed::WithoutTls>>,
    field_id_map: &FieldsIdsMap,
) -> Result<RenderableInput<'doc>, RenderError> {
    let index_rtxn = input_index.zip(input_index_rtxn);
    Ok(match (index_rtxn, document_id, inline_document, inline_query) {
        (None, None, Some(q), None) => RenderableInput::Search(q.clone_fixme()),
        (None, None, None, Some(d)) => RenderableInput::InlineDocument(d.clone_fixme()),
        (Some((index, rtxn)), Some(external_docid), None, None) => {
            let internal_docid =
                index.external_documents_ids().get(rtxn, external_docid).wip().wip();
            let doc = DocumentFromDb::new(internal_docid, rtxn, index, field_id_map).wip().wip();

            RenderableInput::IndexDocument(doc)
        }
        _ => wip!(),
    })
}

enum RenderableInput<'doc> {
    Search(RawMap<'doc>),
    InlineDocument(RawMap<'doc>),
    IndexDocument(DocumentFromDb<'doc, FieldsIdsMap>),
}

fn fetch_template(
    template: &RenderQueryTemplate,
    template_index: Option<&Index>,
    template_index_rtxn: Option<&RoTxn<'_, heed::WithoutTls>>,
) -> Result<RenderingTemplate, RenderError> {
    Ok(match template.kind {
        RenderQueryTemplateKind::DocumentTemplate => {
            let index = template_index.wip();
            let rtxn = template_index_rtxn.wip();
            let embedder = template.embedder.as_deref().wip();

            let configs = index.embedding_configs().embedding_configs(rtxn).wip();
            let config = configs.into_iter().find(|config| config.name == embedder).wip();

            fixme!("error if embedder is not document template");
            fixme!("errors if too many parameters");

            RenderingTemplate::Template(config.config.prompt.try_into().wip())
        }
        RenderQueryTemplateKind::ChatDocumentTemplate => {
            fixme!("require chat feature");
            let index = template_index.wip();
            let rtxn = template_index_rtxn.wip();

            let chat = index.chat_config(rtxn).wip();

            RenderingTemplate::Template(chat.prompt.try_into().wip())
        }
        RenderQueryTemplateKind::IndexingFragment => {
            let index = template_index.wip();
            let rtxn = template_index_rtxn.wip();
            let embedder = template.embedder.as_deref().wip();
            let fragment = template.fragment.as_deref().wip();

            let configs = index.embedding_configs().embedding_configs(rtxn).wip();
            let config = configs.into_iter().find(|config| config.name == embedder).wip();

            fixme!("error if embedder is document template");

            let fragment = config.config.embedder_options.indexing_fragment(fragment).wip();

            RenderingTemplate::Fragment(JsonTemplate::new(fragment.clone_fixme()).wip())
        }
        RenderQueryTemplateKind::SearchFragment => {
            let index = template_index.wip();
            let rtxn = template_index_rtxn.wip();
            let embedder = template.embedder.as_deref().wip();
            let fragment = template.fragment.as_deref().wip();

            let configs = index.embedding_configs().embedding_configs(rtxn).wip();
            let config = configs.into_iter().find(|config| config.name == embedder).wip();

            fixme!("error if embedder is document template");

            let fragment = config.config.embedder_options.search_fragment(fragment).wip();

            RenderingTemplate::Fragment(JsonTemplate::new(fragment.clone_fixme()).wip())
        }
        RenderQueryTemplateKind::InlineDocumentTemplate => {
            let inline = template.inline.as_ref().wip();
            let inline = inline.as_str().wip();

            fixme!("inject max_bytes");

            fixme!("remove to_owned");
            RenderingTemplate::Template(Prompt::new(inline.to_owned(), None).wip())
        }
        RenderQueryTemplateKind::InlineFragment => {
            let inline = template.inline.as_ref().wip();
            RenderingTemplate::Fragment(JsonTemplate::new(inline.clone_fixme()).wip())
        }
    })
}

enum RenderingTemplate {
    Template(milli::prompt::Prompt),
    Fragment(milli::vector::json_template::JsonTemplate),
}

impl RenderingTemplate {
    pub fn render<'doc>(
        &self,
        input: RenderableInput<'doc>,
        field_id_map: &RefCell<GlobalFieldsIdsMap>,
        doc_alloc: &'doc Bump,
    ) -> Result<Value, RenderError> {
        Ok(match (input, self) {
            (RenderableInput::IndexDocument(doc), RenderingTemplate::Template(prompt)) => wip!(),
            (RenderableInput::Search(_), RenderingTemplate::Template(_)) => wip!(),
            (RenderableInput::Search(q), RenderingTemplate::Fragment(fragment)) => {
                fragment.render(&q).wip()
            }
            (RenderableInput::InlineDocument(doc), RenderingTemplate::Template(prompt)) => {
                Value::String(
                    prompt.render_document(None, &doc, field_id_map, doc_alloc).wip().into(),
                )
            }
            (RenderableInput::InlineDocument(doc), RenderingTemplate::Fragment(fragment)) => {
                fragment.render_document(&doc, doc_alloc).wip()
            }
            (RenderableInput::IndexDocument(doc), RenderingTemplate::Fragment(fragment)) => {
                fragment.render_document(&doc, doc_alloc).wip()
            }
        })
    }

    pub fn into_value(self) -> Value {
        match self {
            RenderingTemplate::Template(prompt) => {
                let data: PromptData = prompt.into();
                Value::String(data.template)
            }
            RenderingTemplate::Fragment(json_template) => json_template.template().clone_fixme(),
        }
    }
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
    MissingChatCompletionTemplate,
    UnknownChatCompletionTemplate(String),

    DocumentNotFound(String),
    DocumentMustBeMap,
    BothInlineDocAndDocId,
    TemplateParsing(json_template::Error),
    TemplateRendering(json_template::Error),
    InputConversion(liquid::Error),
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
        fn format_span(span: &Span<'_>) -> String {
            let base_column = span.get_utf8_column();
            let size = span.fragment().chars().count();
            format!("`{}` (cols {}:{})", span.fragment(), base_column, base_column + size)
        }

        fn format_token(token: &Token<'_>) -> String {
            if let Some(base_column) = token.get_utf8_column() {
                let size = token.fragment().chars().count();
                format!("`{}` (cols {}:{})", token.fragment(), base_column, base_column + size)
            } else {
                format!("`{}`", token.fragment())
            }
        }

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
                    format!("Embedder `{}` does not exist.\n  Hint: Available embedders are {}.",
                        &embedder,
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            EmbedderUsesFragments { embedder } => ResponseError::from_msg(
                format!("Requested document template for embedder `{}` but it uses fragments.\n  Hint: Use `indexingFragments` or `searchFragments` instead.", embedder),
                Code::InvalidRenderTemplateId,
            ),
            ReponseError(response_error) => response_error,
            MissingFragment { embedder, kind, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("{} fragment name was not provided.\n  Hint: Available {} fragments for embedder `{}` are {}.",
                        kind.capitalized(),
                        kind.as_str(),
                        &embedder,
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            FragmentDoesNotExist { embedder, fragment, kind, mut available } => {
                available.sort_unstable();
                ResponseError::from_msg(
                    format!("{} fragment `{}` does not exist for embedder `{}`.\n  Hint: Available {} fragments are {}.",
                        kind.capitalized(),
                        &fragment,
                        &embedder,
                        kind.as_str(),
                        available.iter().map(|s| format!("`{s}`")).collect::<Vec<_>>().join(", ")),
                    Code::InvalidRenderTemplateId,
                )
            },
            MissingChatCompletionTemplate => ResponseError::from_msg(
                String::from("Missing chat completion template ID. The only available template is `documentTemplate`."),
                Code::InvalidRenderTemplateId,
            ),
            UnknownChatCompletionTemplate(id) => ResponseError::from_msg(
                format!("Unknown chat completion template ID {}. The only available template is `documentTemplate`.", &id),
                Code::InvalidRenderTemplateId,
            ),
            DocumentNotFound(doc_id) => ResponseError::from_msg(
                format!("Document with ID `{doc_id}` not found."),
                Code::RenderDocumentNotFound,
            ),
            DocumentMustBeMap => ResponseError::from_msg(
                String::from("The `doc` field must be a map."),
                Code::InvalidRenderInput,
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
            InputConversion(err) => ResponseError::from_msg(
                format!("Error converting input to a liquid object: {err}"),
                Code::InvalidRenderInput,
            ),
        }
    }
}

#[allow(clippy::result_large_err)]
async fn render(index: Index, query: RenderQuery) -> Result<RenderResult, ResponseError> {
    let RenderQuery { template, input } = query;
    let rtxn = index.read_txn()?;
    let (template, fields_available) = match (template.inline, template.id) {
        (Some(inline), None) => (inline, true),
        (None, Some(id)) => parse_template_id(&index, &rtxn, &id)?,
        (Some(_), Some(_)) => return Err(MultipleTemplates.into()),
        (None, None) => return Err(MissingTemplate.into()),
    };
    let fields_already_present = input
        .as_ref()
        .is_some_and(|i| i.inline.as_ref().is_some_and(|i| i.get("fields").is_some()));
    let fields_unused = match template.as_str() {
        Some(template) => {
            // might be a false positive if fields appear as a non-variable
            // it is OK to be over-eager here, it will just translate to more work
            !template.contains("fields")
        }
        None => true, // non-text templates cannot use `fields`
    };
    let has_inline_doc =
        input.as_ref().is_some_and(|i| i.inline.as_ref().is_some_and(|i| i.get("doc").is_some()));
    let has_document_id = input.as_ref().is_some_and(|i| i.document_id.is_some());
    let has_doc = has_inline_doc || has_document_id;
    let insert_fields = fields_available && has_doc && !fields_unused && !fields_already_present;
    if has_inline_doc && has_document_id {
        return Err(BothInlineDocAndDocId.into());
    }

    let mut rendered = Value::Null;
    if let Some(input) = input {
        let inline = input.inline.unwrap_or_default();
        let mut object = liquid::to_object(&inline).map_err(InputConversion)?;

        let doc = match object.get_mut("doc") {
            Some(liquid::model::Value::Object(doc)) => Some(doc),
            Some(liquid::model::Value::Nil) => None,
            None => None,
            _ => return Err(DocumentMustBeMap.into()),
        };
        if insert_fields {
            if let Some(doc) = doc {
                let doc = doc.clone();
                let fid_map_with_meta = index.fields_ids_map_with_metadata(&rtxn)?;
                let fields = OwnedFields::new(&doc, &fid_map_with_meta);
                object.insert("fields".into(), fields.to_value());
            }
        }

        if let Some(document_id) = input.document_id {
            if insert_fields {
                let fid_map_with_meta = index.fields_ids_map_with_metadata(&rtxn)?;
                let (document, fields) =
                    build_doc_fields(&index, &rtxn, &document_id, &fid_map_with_meta)?
                        .ok_or_else(|| DocumentNotFound(document_id))?;
                object.insert("doc".into(), document);
                object.insert("fields".into(), fields);
            } else {
                let fid_map = index.fields_ids_map(&rtxn)?;
                let document = build_doc(&index, &rtxn, &document_id, &fid_map)?
                    .ok_or_else(|| DocumentNotFound(document_id))?;
                object.insert("doc".into(), document);
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

// implementation note: this is a set as a struct because deserr does not support untagged enums
#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderTemplate>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryTemplate {
    /// Kind of template or fragment to fetch.
    ///
    /// Determines which other parameters are allowed and mandatory.
    #[deserr(error = DeserrJsonError<InvalidRenderTemplate>)]
    pub kind: RenderQueryTemplateKind,
    /// Index to fetch the template or fragment from.
    ///
    /// - Mandatory for `kind`s: `documentTemplate`, `chatDocumentTemplate`,
    ///  `indexingFragment` and `searchFragment`.
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub index_uid: Option<IndexUid>,
    /// Embedder to fetch the template or fragment from.
    ///
    /// - Mandatory for `kind`s: `documentTemplate`, `chatDocumentTemplate`,
    ///  `indexingFragment` and `searchFragment`.
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub embedder: Option<String>,
    /// Name of the fragment to fetch.
    ///
    /// - Mandatory for `kind`s: `indexingFragment` and `searchFragment`.
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub fragment: Option<String>,
    /// Inline value of the template or fragment.
    ///
    /// - Mandatory for `kind`s: `inlineDocumentTemplate` and `inlineFragment`.
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplate>)]
    pub inline: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderTemplate>, rename_all = camelCase, deny_unknown_fields)]
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

#[derive(Debug, Clone, Default, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderInput>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryInput {
    #[deserr(default, error = DeserrJsonError<InvalidRenderInput>)]
    pub index_uid: Option<IndexUid>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputDocumentId>)]
    pub document_id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputInline>)]
    pub inline_document: Option<BTreeMap<String, Value>>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderInputInline>)]
    pub inline_query: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct RenderResult {
    template: Value,
    rendered: Option<Value>,
}
