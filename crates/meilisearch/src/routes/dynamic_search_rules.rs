use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use index_scheduler::IndexScheduler;
use itertools::Itertools;
use meilisearch_types::deserr::{DeserrJsonError, DeserrQueryParamError};
use meilisearch_types::dynamic_search_rules::{
    DynamicSearchRule, DynamicSearchRuleUpdateRequest, RuleUid,
};
use meilisearch_types::error::deserr_codes::{
    InvalidDynamicSearchRuleFilter, InvalidDynamicSearchRuleFilterActive,
    InvalidDynamicSearchRuleFilterQuery, InvalidDynamicSearchRuleLimit,
    InvalidDynamicSearchRuleOffset,
};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::SearchResult;
use meilisearch_types::tasks::{DsrUpdate, KindWithContent};
use serde::Serialize;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::proxy::{proxy, task_network_and_check_leader_and_version, Body};
use crate::routes::indexes::documents::CustomMetadataQuery;
use crate::routes::{Pagination, PaginationView, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT};

#[routes::routes(
    routes(
        "" => [post(list_rules), delete(clear_rules)],
        "/{uid}" => [get(get_rule), patch(update_or_create_rule), delete(delete_rule)],
    ),
    tag = "Search rules",
    tags((
        name = "Search rules",
        description = "The `/dynamic-search-rules` route allows you to configure search rules.",
    ))
)]
pub struct DynamicSearchRulesApi;

#[routes::request(override_error = DeserrJsonError<InvalidDynamicSearchRuleFilter>)]
#[derive(Debug)]
pub struct ListRulesFilter {
    /// Only include rules whose names match these patterns (e.g. `["black-friday", "promo*"]`).
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilterQuery>)]
    pub query: Option<String>,
    /// Only include rules that are active (true) or not active (false).
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilterActive>)]
    pub active: Option<bool>,
}

#[routes::request]
#[derive(Debug)]
pub struct ListRules {
    /// Number of rules to skip. Defaults to 0.
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleOffset>)]
    pub offset: usize,
    /// Maximum number of rules to return. Default to 20.
    #[request(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidDynamicSearchRuleLimit>)]
    pub limit: usize,
    /// Optional filter to restrict which rules are returned (e.g. by attribute patterns or by properties like if the rule is active or not)
    #[request(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilter>)]
    pub filter: Option<ListRulesFilter>,
}

#[derive(Debug, thiserror::Error)]
enum DynamicSearchRulesError {
    #[error("Dynamic search rule `{0}` not found.")]
    NotFound(RuleUid),
}

impl ErrorCode for DynamicSearchRulesError {
    fn error_code(&self) -> Code {
        match self {
            DynamicSearchRulesError::NotFound(_) => Code::DynamicSearchRuleNotFound,
        }
    }
}

#[derive(Serialize, Default)]
struct UpdateDynamicSearchRuleAnalytics;

impl Aggregate for UpdateDynamicSearchRuleAnalytics {
    fn event_name(&self) -> &'static str {
        "Dynamic Search Rules Created or Updated"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

#[derive(Serialize, Default)]
struct DeleteDynamicSearchRuleAnalytics;

impl Aggregate for DeleteDynamicSearchRuleAnalytics {
    fn event_name(&self) -> &'static str {
        "Dynamic Search Rules Deleted"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// List search rules
///
/// Return all search rules configured on the instance.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
    request_body = ListRules,
    responses(
        (status = OK, description = "Dynamic search rules are returned.", body = PaginationView<DynamicSearchRule>, content_type = "application/json", example = json!({
            "results": [
                {
                    "uid": "black-friday",
                    "description": "Black Friday 2025 rules",
                    "precedence": 10,
                    "active": true,
                    "conditions": {
                        "query": {
                            "isEmpty": true
                        },
                        "time": {
                            "start": "2025-11-28T00:00:00Z",
                            "end": "2025-11-28T23:59:59Z"
                        }
                    },
                    "actions": [
                        {
                            "selector": { "indexUid": "products", "id": "123" },
                            "action": { "type": "pin", "position": 1 }
                        }
                    ]
                }
            ],
            "offset": 0,
            "limit": 20,
            "total": 1
        })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
    ),
)]
async fn list_rules(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_GET }>,
        Data<IndexScheduler>,
    >,
    body: AwebJson<ListRules, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    let dsrs = index_scheduler.dynamic_search_rules(
        index_scheduler.features(),
        "Calling the `POST /dynamic-search-rules` route",
    )?;

    let ListRules { offset, limit, filter } = body.into_inner();

    let pagination = Pagination { offset, limit };

    let Some(dsrs) = dsrs.milli_dsrs()? else {
        let pagination_view = pagination.empty();
        return Ok(HttpResponse::Ok().json(pagination_view));
    };
    let mut rule_ids = dsrs.all_rule_ids()?;

    let query = if let Some(filter) = filter {
        if let Some(is_active) = filter.active {
            rule_ids &= dsrs.active_rule_ids(is_active)?;
        }
        filter.query
    } else {
        None
    };

    let SearchResult {
        matching_words: _,
        candidates,
        documents_ids: rule_ids,
        document_scores: _,
        degraded: _,
        used_negative_operator: _,
        query_vector: _,
    } = dsrs.search_in_description_and_words(query, rule_ids, limit, offset)?;

    let rules = dsrs
        .rules_from_rule_ids(rule_ids)
        .map_ok(|doc| {
            DynamicSearchRule::try_from_meili_doc(
                doc,
                meilisearch_types::milli::FaultSource::Runtime,
            )
        })
        .map(|res| res.flatten());

    let rules: meilisearch_types::milli::Result<Vec<_>> = rules.collect();

    let pagination_view =
        PaginationView { results: rules?, offset, limit, total: candidates.len() as usize };

    Ok(HttpResponse::Ok().json(pagination_view))
}

/// Get a search rule
///
/// Retrieve a single search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
    responses(
        (status = OK, description = "Dynamic search rule returned.", body = DynamicSearchRule, content_type = "application/json", example = json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "precedence": 10,
            "active": true,
            "conditions": {
                "query": {
                    "isEmpty": true
                },
                "time": {
                    "start": "2025-11-28T00:00:00Z",
                    "end": "2025-11-28T23:59:59Z"
                }
            },
            "actions": [
                {
                    "selector": { "indexUid": "products", "id": "123" },
                    "action": { "type": "pin", "position": 1 }
                }
            ]
        })),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        })),
        (status = 404, description = "Dynamic search rule not found.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "Dynamic search rule `black-friday` not found.",
            "code": "dynamic_search_rule_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
        })),
    ),
)]
async fn get_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_GET }>,
        Data<IndexScheduler>,
    >,
    uid: Path<RuleUid>,
) -> Result<HttpResponse, ResponseError> {
    let rules = index_scheduler.dynamic_search_rules(
        index_scheduler.features(),
        "Calling the `GET /dynamic-search-rules/{:ruleUid}` route",
    )?;

    let uid = uid.into_inner();
    let rule = rules.get(&uid)?.ok_or(DynamicSearchRulesError::NotFound(uid))?;

    Ok(HttpResponse::Ok().json(rule))
}

/// Create or update a search rule
///
/// Partially update a search rule by replacing the provided fields. If the rule doesn't exist, it will be created.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.update", "dynamicSearchRules.*", "*"])),
    request_body = DynamicSearchRuleUpdateRequest,
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
    responses(
        (status = ACCEPTED, description = "Task enqueued to update dynamic search rule.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "dsrUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        }))
    ),
)]
async fn update_or_create_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<RuleUid>,
    query: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    body: AwebJson<DynamicSearchRuleUpdateRequest, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;
    let network = index_scheduler.network();

    let CustomMetadataQuery { custom_metadata } = query.into_inner();

    let uid = uid.into_inner();
    let rule = body.into_inner();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let mut task = {
        let kind = KindWithContent::DsrUpdate(DsrUpdate::CreateOrUpdate {
            rule_id: uid,
            update: rule.clone(),
        });
        index_scheduler.register_with_custom_metadata(
            kind,
            None,
            custom_metadata,
            false,
            task_network,
        )
    }?;

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, None, &req, task_network, network, Body::inline(rule), &task)
            .await?;
    }

    let task: SummarizedTaskView = task.into();

    analytics.publish(UpdateDynamicSearchRuleAnalytics, &req);
    tracing::debug!(returns = ?task, "Update DSR");

    Ok(HttpResponse::Accepted().json(task))
}

/// Delete a search rule
///
/// Delete a search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.delete", "dynamicSearchRules.*", "*.delete", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
    responses(
        (status = ACCEPTED, description = "Dynamic search rule task deletion.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "dsrUpdate",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        }))
    ),
)]
async fn delete_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_DELETE }>,
        Data<IndexScheduler>,
    >,
    query: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    uid: Path<RuleUid>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let CustomMetadataQuery { custom_metadata } = query.into_inner();

    let uid = uid.into_inner();

    let mut task = {
        let kind = KindWithContent::DsrUpdate(DsrUpdate::Deletion(uid));
        index_scheduler.register_with_custom_metadata(
            kind,
            None,
            custom_metadata,
            false,
            task_network,
        )?
    };

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, None, &req, task_network, network, Body::none(), &task).await?;
    }

    analytics.publish(DeleteDynamicSearchRuleAnalytics, &req);

    let task: SummarizedTaskView = task.into();

    tracing::debug!(returns = ?task, "Delete DSR");
    Ok(HttpResponse::Accepted().json(task))
}

/// Delete all search rules.
///
/// This will delete **all** the currently defined search rules.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.delete", "dynamicSearchRules.*", "*.delete", "*"])),
    responses(
        (status = ACCEPTED, description = "Enqueued a task to delete all search rules.", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "dsrClear",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "The Authorization header is missing. It must use the bearer authorization method.",
            "code": "missing_authorization_header",
            "type": "auth",
            "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
        }))
    ),
)]
async fn clear_rules(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_DELETE }>,
        Data<IndexScheduler>,
    >,
    query: AwebQueryParameter<CustomMetadataQuery, DeserrQueryParamError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;
    let network = index_scheduler.network();
    let task_network = task_network_and_check_leader_and_version(&req, &network)?;

    let CustomMetadataQuery { custom_metadata } = query.into_inner();

    let mut task = {
        let kind = KindWithContent::DsrClear;
        index_scheduler.register_with_custom_metadata(
            kind,
            None,
            custom_metadata,
            false,
            task_network,
        )?
    };

    if let Some(task_network) = task.network.take() {
        proxy(&index_scheduler, None, &req, task_network, network, Body::none(), &task).await?;
    }

    analytics.publish(DeleteDynamicSearchRuleAnalytics, &req);

    let task: SummarizedTaskView = task.into();

    tracing::debug!(returns = ?task, "Clear DSRs");
    Ok(HttpResponse::Accepted().json(task))
}
