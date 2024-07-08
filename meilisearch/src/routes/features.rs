use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde_json::json;
use tracing::debug;

use crate::analytics::Analytics;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_features)))
            .route(web::patch().to(SeqHandler(patch_features))),
    );
}

async fn get_features(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::EXPERIMENTAL_FEATURES_GET }>,
        Data<IndexScheduler>,
    >,
    req: HttpRequest,
    analytics: Data<dyn Analytics>,
) -> HttpResponse {
    let features = index_scheduler.features();

    analytics.publish("Experimental features Seen".to_string(), json!(null), Some(&req));
    let features = features.runtime_features();
    debug!(returns = ?features, "Get features");
    HttpResponse::Ok().json(features)
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct RuntimeTogglableFeatures {
    #[deserr(default)]
    pub vector_store: Option<bool>,
    #[deserr(default)]
    pub metrics: Option<bool>,
    #[deserr(default)]
    pub logs_route: Option<bool>,
    #[deserr(default)]
    pub edit_documents_by_function: Option<bool>,
}

async fn patch_features(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::EXPERIMENTAL_FEATURES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    new_features: AwebJson<RuntimeTogglableFeatures, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let features = index_scheduler.features();
    debug!(parameters = ?new_features, "Patch features");

    let old_features = features.runtime_features();
    let new_features = meilisearch_types::features::RuntimeTogglableFeatures {
        vector_store: new_features.0.vector_store.unwrap_or(old_features.vector_store),
        metrics: new_features.0.metrics.unwrap_or(old_features.metrics),
        logs_route: new_features.0.logs_route.unwrap_or(old_features.logs_route),
        edit_documents_by_function: new_features
            .0
            .edit_documents_by_function
            .unwrap_or(old_features.edit_documents_by_function),
    };

    // explicitly destructure for analytics rather than using the `Serialize` implementation, because
    // the it renames to camelCase, which we don't want for analytics.
    // **Do not** ignore fields with `..` or `_` here, because we want to add them in the future.
    let meilisearch_types::features::RuntimeTogglableFeatures {
        vector_store,
        metrics,
        logs_route,
        edit_documents_by_function,
    } = new_features;

    analytics.publish(
        "Experimental features Updated".to_string(),
        json!({
            "vector_store": vector_store,
            "metrics": metrics,
            "logs_route": logs_route,
            "edit_documents_by_function": edit_documents_by_function,
        }),
        Some(&req),
    );
    index_scheduler.put_runtime_features(new_features)?;
    debug!(returns = ?new_features, "Patch features");
    Ok(HttpResponse::Ok().json(new_features))
}
