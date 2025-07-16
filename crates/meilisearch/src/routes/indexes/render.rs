use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use itertools::structs;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use meilisearch_types::serde_cs::vec::CS;
use serde::Serialize;
use serde_json::Value;
use tracing::debug;
use utoipa::{IntoParams, OpenApi, ToSchema};

use super::ActionPolicy;
use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::indexes::similar_analytics::{SimilarAggregator, SimilarGET, SimilarPOST};
use crate::search::{
    add_search_rules, perform_similar, RankingScoreThresholdSimilar, RetrieveVectors, Route,
    SearchKind, SimilarQuery, SimilarResult, DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};

#[derive(OpenApi)]
#[openapi(
    paths(render),
    tags(
        (
            name = "Render templates",
            description = "The /render route allows rendering templates used by Meilisearch.",
            external_docs(url = "https://www.meilisearch.com/docs/reference/api/render"),
        ),
    ),
)]
pub struct RenderApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::post().to(SeqHandler(render)))
    );
}

/// Render templates with POST
#[utoipa::path(
    post,
    path = "{indexUid}/render",
    tag = "Render templates",
    security(("Bearer" = ["templates.render", "*.get", "*"])),
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
pub async fn render(
    index_scheduler: GuardedData<ActionPolicy<{ actions::SEARCH }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebJson<RenderQuery, DeserrJsonError>,
    req: HttpRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = IndexUid::try_from(index_uid.into_inner())?;

    let query = params.into_inner();
    debug!(parameters = ?query, "Render template");

    //let mut aggregate = SimilarAggregator::<SimilarPOST>::from_query(&query);

    let rendered = RenderResult {
        rendered: String::from("TODO")
    };

    // if let Ok(similar) = &similar {
    //     aggregate.succeed(similar);
    // }
    // analytics.publish(aggregate, &req);

    debug!(returns = ?rendered, "Render template");
    Ok(HttpResponse::Ok().json(rendered))
}

#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQuery {
    #[deserr(error = DeserrJsonError<InvalidRenderTemplate>)]
    pub template: RenderQueryTemplate,
    #[deserr(error = DeserrJsonError<InvalidRenderInput>)]
    pub input: RenderQueryInput,
}

#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderTemplate>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryTemplate {
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateId>)]
    id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateInline>)]
    inline: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidRenderInput>, rename_all = camelCase, deny_unknown_fields)]
pub struct RenderQueryInput {
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateId>)]
    document_id: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidRenderTemplateId>)]
    inline: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, PartialEq, ToSchema)]
pub struct RenderResult {
    rendered: String,
}
