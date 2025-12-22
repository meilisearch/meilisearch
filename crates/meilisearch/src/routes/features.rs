use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::Serialize;
use tracing::debug;
use utoipa::{OpenApi, ToSchema};

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

#[derive(OpenApi)]
#[openapi(
    paths(get_features, patch_features),
    tags((
        name = "Experimental features",
        description = "The `/experimental-features` route allows you to activate or deactivate some of Meilisearch's experimental features.

This route is **synchronous**. This means that no task object will be returned, and any activated or deactivated features will be made available or unavailable immediately.",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/experimental_features"),
    )),
)]
pub struct ExperimentalFeaturesApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_features))
            .route(web::patch().to(SeqHandler(patch_features))),
    );
}

/// Get all experimental features
///
/// Get a list of all experimental features that can be activated via the
/// /experimental-features route and whether or not they are currently
/// activated.
#[utoipa::path(
    get,
    path = "",
    tag = "Experimental features",
    security(("Bearer" = ["experimental_features.get", "experimental_features.*", "*"])),
    responses(
        (status = OK, description = "Experimental features are returned", body = RuntimeTogglableFeatures, content_type = "application/json", example = json!(RuntimeTogglableFeatures {
            metrics: Some(true),
            logs_route: Some(false),
            edit_documents_by_function: Some(false),
            contains_filter: Some(false),
            network: Some(false),
            get_task_documents_route: Some(false),
            composite_embedders: Some(false),
            chat_completions: Some(false),
            multimodal: Some(false),
            vector_store_setting: Some(false),
        })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn get_features(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::EXPERIMENTAL_FEATURES_GET }>,
        Data<IndexScheduler>,
    >,
) -> HttpResponse {
    let features = index_scheduler.features();

    let features = features.runtime_features();
    let features: RuntimeTogglableFeatures = features.into();
    debug!(returns = ?features, "Get features");
    HttpResponse::Ok().json(features)
}

/// Experimental features that can be toggled at runtime
#[derive(Debug, Deserr, ToSchema, Serialize)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct RuntimeTogglableFeatures {
    /// Enable the /metrics endpoint for Prometheus metrics
    #[deserr(default)]
    pub metrics: Option<bool>,
    /// Enable the /logs route for log configuration
    #[deserr(default)]
    pub logs_route: Option<bool>,
    /// Enable document editing via JavaScript functions
    #[deserr(default)]
    pub edit_documents_by_function: Option<bool>,
    /// Enable the CONTAINS filter operator
    #[deserr(default)]
    pub contains_filter: Option<bool>,
    /// Enable network features for distributed search
    #[deserr(default)]
    pub network: Option<bool>,
    /// Enable the route to get documents from tasks
    #[deserr(default)]
    pub get_task_documents_route: Option<bool>,
    /// Enable composite embedders for multi-source embeddings
    #[deserr(default)]
    pub composite_embedders: Option<bool>,
    /// Enable chat completion capabilities
    #[deserr(default)]
    pub chat_completions: Option<bool>,
    /// Enable multimodal search with images and other media
    #[deserr(default)]
    pub multimodal: Option<bool>,
    /// Enable vector store settings configuration
    #[deserr(default)]
    pub vector_store_setting: Option<bool>,
}

impl From<meilisearch_types::features::RuntimeTogglableFeatures> for RuntimeTogglableFeatures {
    fn from(value: meilisearch_types::features::RuntimeTogglableFeatures) -> Self {
        let meilisearch_types::features::RuntimeTogglableFeatures {
            metrics,
            logs_route,
            edit_documents_by_function,
            contains_filter,
            network,
            get_task_documents_route,
            composite_embedders,
            chat_completions,
            multimodal,
            vector_store_setting,
        } = value;

        Self {
            metrics: Some(metrics),
            logs_route: Some(logs_route),
            edit_documents_by_function: Some(edit_documents_by_function),
            contains_filter: Some(contains_filter),
            network: Some(network),
            get_task_documents_route: Some(get_task_documents_route),
            composite_embedders: Some(composite_embedders),
            chat_completions: Some(chat_completions),
            multimodal: Some(multimodal),
            vector_store_setting: Some(vector_store_setting),
        }
    }
}

#[derive(Serialize)]
pub struct PatchExperimentalFeatureAnalytics {
    metrics: bool,
    logs_route: bool,
    edit_documents_by_function: bool,
    contains_filter: bool,
    network: bool,
    get_task_documents_route: bool,
    composite_embedders: bool,
    chat_completions: bool,
    multimodal: bool,
    vector_store_setting: bool,
}

impl Aggregate for PatchExperimentalFeatureAnalytics {
    fn event_name(&self) -> &'static str {
        "Experimental features Updated"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            metrics: new.metrics,
            logs_route: new.logs_route,
            edit_documents_by_function: new.edit_documents_by_function,
            contains_filter: new.contains_filter,
            network: new.network,
            get_task_documents_route: new.get_task_documents_route,
            composite_embedders: new.composite_embedders,
            chat_completions: new.chat_completions,
            multimodal: new.multimodal,
            vector_store_setting: new.vector_store_setting,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Configure experimental features
///
/// Activate or deactivate experimental features.
#[utoipa::path(
    patch,
    path = "",
    tag = "Experimental features",
    security(("Bearer" = ["experimental_features.update", "experimental_features.*", "*"])),
    responses(
        (status = OK, description = "Experimental features are returned", body = RuntimeTogglableFeatures, content_type = "application/json", example = json!(RuntimeTogglableFeatures {
            metrics: Some(true),
            logs_route: Some(false),
            edit_documents_by_function: Some(false),
            contains_filter: Some(false),
            network: Some(false),
            get_task_documents_route: Some(false),
            composite_embedders: Some(false),
            chat_completions: Some(false),
            multimodal: Some(false),
            vector_store_setting: Some(false),
         })),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn patch_features(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::EXPERIMENTAL_FEATURES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    new_features: AwebJson<RuntimeTogglableFeatures, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let features = index_scheduler.features();
    debug!(parameters = ?new_features, "Patch features");

    let old_features = features.runtime_features();
    let new_features = meilisearch_types::features::RuntimeTogglableFeatures {
        metrics: new_features.0.metrics.unwrap_or(old_features.metrics),
        logs_route: new_features.0.logs_route.unwrap_or(old_features.logs_route),
        edit_documents_by_function: new_features
            .0
            .edit_documents_by_function
            .unwrap_or(old_features.edit_documents_by_function),
        contains_filter: new_features.0.contains_filter.unwrap_or(old_features.contains_filter),
        network: new_features.0.network.unwrap_or(old_features.network),
        get_task_documents_route: new_features
            .0
            .get_task_documents_route
            .unwrap_or(old_features.get_task_documents_route),
        composite_embedders: new_features
            .0
            .composite_embedders
            .unwrap_or(old_features.composite_embedders),
        chat_completions: new_features.0.chat_completions.unwrap_or(old_features.chat_completions),
        multimodal: new_features.0.multimodal.unwrap_or(old_features.multimodal),
        vector_store_setting: new_features
            .0
            .vector_store_setting
            .unwrap_or(old_features.vector_store_setting),
    };

    // explicitly destructure for analytics rather than using the `Serialize` implementation, because
    // it renames to camelCase, which we don't want for analytics.
    // **Do not** ignore fields with `..` or `_` here, because we want to add them in the future.
    let meilisearch_types::features::RuntimeTogglableFeatures {
        metrics,
        logs_route,
        edit_documents_by_function,
        contains_filter,
        network,
        get_task_documents_route,
        composite_embedders,
        chat_completions,
        multimodal,
        vector_store_setting,
    } = new_features;

    analytics.publish(
        PatchExperimentalFeatureAnalytics {
            metrics,
            logs_route,
            edit_documents_by_function,
            contains_filter,
            network,
            get_task_documents_route,
            composite_embedders,
            chat_completions,
            multimodal,
            vector_store_setting,
        },
        &req,
    );
    index_scheduler.put_runtime_features(new_features)?;
    let new_features: RuntimeTogglableFeatures = new_features.into();
    debug!(returns = ?new_features, "Patch features");
    Ok(HttpResponse::Ok().json(new_features))
}
