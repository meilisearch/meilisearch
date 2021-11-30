use actix_web::{web, HttpResponse};
use chrono::{DateTime, Utc};
use log::debug;
use serde::{Deserialize, Serialize};

use meilisearch_error::ResponseError;
use meilisearch_lib::index::{Settings, Unchecked};
use meilisearch_lib::MeiliSearch;

use crate::extractors::authentication::{policies::*, GuardedData};
use crate::ApiKeys;

mod dump;
pub mod indexes;
mod tasks;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::scope("/tasks").configure(tasks::configure))
        .service(web::resource("/health").route(web::get().to(get_health)))
        .service(web::scope("/dumps").configure(dump::configure))
        .service(web::resource("/keys").route(web::get().to(list_keys)))
        .service(web::resource("/stats").route(web::get().to(get_stats)))
        .service(web::resource("/version").route(web::get().to(get_version)))
        .service(web::scope("/indexes").configure(indexes::configure));
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
#[serde(tag = "name")]
pub enum UpdateType {
    ClearAll,
    Customs,
    DocumentsAddition {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>,
    },
    DocumentsPartial {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>,
    },
    DocumentsDeletion {
        #[serde(skip_serializing_if = "Option::is_none")]
        number: Option<usize>,
    },
    Settings {
        settings: Settings<Unchecked>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub duration: f64, // in seconds
    pub enqueued_at: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub error: ResponseError,
    pub duration: f64, // in seconds
    pub enqueued_at: DateTime<Utc>,
    pub processed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnqueuedUpdateResult {
    pub update_id: u64,
    #[serde(rename = "type")]
    pub update_type: UpdateType,
    pub enqueued_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_processing_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "status")]
pub enum UpdateStatusResponse {
    Enqueued {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Processing {
        #[serde(flatten)]
        content: EnqueuedUpdateResult,
    },
    Failed {
        #[serde(flatten)]
        content: FailedUpdateResult,
    },
    Processed {
        #[serde(flatten)]
        content: ProcessedUpdateResult,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexUpdateResponse {
    pub update_id: u64,
}

impl IndexUpdateResponse {
    pub fn with_id(update_id: u64) -> Self {
        Self { update_id }
    }
}

/// Always return a 200 with:
/// ```json
/// {
///     "status": "Meilisearch is running"
/// }
/// ```
pub async fn running() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "MeiliSearch is running" }))
}

async fn get_stats(
    meilisearch: GuardedData<Private, MeiliSearch>,
) -> Result<HttpResponse, ResponseError> {
    let response = meilisearch.get_all_stats().await?;

    debug!("returns: {:?}", response);
    Ok(HttpResponse::Ok().json(response))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionResponse {
    commit_sha: String,
    commit_date: String,
    pkg_version: String,
}

async fn get_version(_meilisearch: GuardedData<Private, MeiliSearch>) -> HttpResponse {
    let commit_sha = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
    let commit_date = option_env!("VERGEN_GIT_COMMIT_TIMESTAMP").unwrap_or("unknown");

    HttpResponse::Ok().json(VersionResponse {
        commit_sha: commit_sha.to_string(),
        commit_date: commit_date.to_string(),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

#[derive(Serialize)]
struct KeysResponse {
    private: Option<String>,
    public: Option<String>,
}

pub async fn list_keys(meilisearch: GuardedData<Admin, ApiKeys>) -> HttpResponse {
    let api_keys = (*meilisearch).clone();
    HttpResponse::Ok().json(&KeysResponse {
        private: api_keys.private,
        public: api_keys.public,
    })
}

pub async fn get_health() -> Result<HttpResponse, ResponseError> {
    Ok(HttpResponse::Ok().json(serde_json::json!({ "status": "available" })))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::extractors::authentication::GuardedData;

    /// A type implemented for a route that uses a authentication policy `Policy`.
    ///
    /// This trait is used for regression testing of route authenticaton policies.
    trait Is<Policy, Data, T> {}

    macro_rules! impl_is_policy {
        ($($param:ident)*) => {
            impl<Policy, Func, Data, $($param,)* Res> Is<Policy, Data, (($($param,)*), Res)> for Func
                where Func: Fn(GuardedData<Policy, Data>, $($param,)*) -> Res {}

        };
    }

    impl_is_policy! {}
    impl_is_policy! {A}
    impl_is_policy! {A B}
    impl_is_policy! {A B C}
    impl_is_policy! {A B C D}
    impl_is_policy! {A B C D E}

    /// Emits a compile error if a route doesn't have the correct authentication policy.
    ///
    /// This works by trying to cast the route function into a Is<Policy, _> type, where Policy it
    /// the authentication policy defined for the route.
    macro_rules! test_auth_routes {
        ($($policy:ident => { $($route:expr,)*})*) => {
            #[test]
            fn test_auth() {
                $($(let _: &dyn Is<$policy, _, _> = &$route;)*)*
            }
        };
    }

    test_auth_routes! {
        Public => {
            indexes::search::search_with_url_query,
            indexes::search::search_with_post,

            indexes::documents::get_document,
            indexes::documents::get_all_documents,
        }
        Private => {
            get_stats,
            get_version,

            indexes::create_index,
            indexes::list_indexes,
            indexes::get_index_stats,
            indexes::delete_index,
            indexes::update_index,
            indexes::get_index,

            dump::create_dump,

            indexes::settings::filterable_attributes::get,
            indexes::settings::displayed_attributes::get,
            indexes::settings::searchable_attributes::get,
            indexes::settings::stop_words::get,
            indexes::settings::synonyms::get,
            indexes::settings::distinct_attribute::get,
            indexes::settings::filterable_attributes::update,
            indexes::settings::displayed_attributes::update,
            indexes::settings::searchable_attributes::update,
            indexes::settings::stop_words::update,
            indexes::settings::synonyms::update,
            indexes::settings::distinct_attribute::update,
            indexes::settings::filterable_attributes::delete,
            indexes::settings::displayed_attributes::delete,
            indexes::settings::searchable_attributes::delete,
            indexes::settings::stop_words::delete,
            indexes::settings::synonyms::delete,
            indexes::settings::distinct_attribute::delete,
            indexes::settings::delete_all,
            indexes::settings::get_all,
            indexes::settings::update_all,

            indexes::documents::clear_all_documents,
            indexes::documents::delete_documents,
            indexes::documents::update_documents,
            indexes::documents::add_documents,
            indexes::documents::delete_document,

            indexes::tasks::get_all_tasks_status,
            indexes::tasks::get_task_status,
        }
        Admin => { list_keys, }
    }
}
