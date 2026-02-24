use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::dynamic_search_rules::{Condition, DynamicSearchRule, RuleAction};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use serde::Serialize;
use tracing::debug;
use utoipa::ToSchema;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;

#[routes::routes(
    routes(
        "" => [get(list_rules), post(create_rule)],
        "/{uid}" => [get(get_rule), patch(update_rule), delete(delete_rule)],
    ),
    tag = "Dynamic search rules",
    tags((
        name = "Dynamic search rules",
        description = "The `/dynamic-search-rules` route allows you to configure dynamic search rules.",
    ))
)]
pub struct DynamicSearchRulesApi;

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
struct CreateDynamicSearchRuleRequest {
    /// Unique identifier of the dynamic search rule.
    uid: String,
    /// Human-readable description of the dynamic search rule.
    #[deserr(default)]
    description: Option<String>,
    /// Priority of the dynamic search rule. Lower values take precedence over higher ones.
    #[deserr(default)]
    priority: Option<u64>,
    /// Whether the dynamic search rule is active.
    #[deserr(default)]
    active: bool,
    /// Conditions that must match before the dynamic search rule applies.
    #[deserr(default)]
    conditions: Vec<Condition>,
    /// Actions to apply when the dynamic search rule matches.
    actions: Vec<RuleAction>,
}

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
struct UpdateDynamicSearchRuleRequest {
    /// Human-readable description of the dynamic search rule.
    #[deserr(default)]
    description: Option<String>,
    /// Priority of the dynamic search rule. Lower values take precedence over higher ones.
    #[deserr(default)]
    priority: Option<u64>,
    /// Whether the dynamic search rule is active.
    #[deserr(default)]
    active: Option<bool>,
    /// Conditions that must match before the dynamic search rule applies.
    #[deserr(default)]
    conditions: Option<Vec<Condition>>,
    /// Actions to apply when the dynamic search rule matches.
    #[deserr(default)]
    actions: Option<Vec<RuleAction>>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ListRulesResponse {
    /// Dynamic search rules configured on the instance.
    results: Vec<DynamicSearchRule>,
}

#[derive(Debug, thiserror::Error)]
enum DynamicSearchRulesError {
    #[error("Dynamic search rule `{0}` not found.")]
    NotFound(String),
    #[error("Dynamic search rule `{0}` already exists.")]
    AlreadyExists(String),
}

impl ErrorCode for DynamicSearchRulesError {
    fn error_code(&self) -> Code {
        match self {
            DynamicSearchRulesError::NotFound(_) => Code::DynamicSearchRuleNotFound,
            DynamicSearchRulesError::AlreadyExists(_) => Code::BadRequest,
        }
    }
}

#[derive(Serialize, Default)]
struct CreateDynamicSearchRuleAnalytics;

impl Aggregate for CreateDynamicSearchRuleAnalytics {
    fn event_name(&self) -> &'static str {
        "Dynamic Search Rules Created"
    }

    fn aggregate(self: Box<Self>, _new: Box<Self>) -> Box<Self> {
        self
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

#[derive(Serialize, Default)]
struct UpdateDynamicSearchRuleAnalytics;

impl Aggregate for UpdateDynamicSearchRuleAnalytics {
    fn event_name(&self) -> &'static str {
        "Dynamic Search Rules Updated"
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

/// List dynamic search rules
///
/// Return all dynamic search rules configured on the instance.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
    responses(
        (status = OK, description = "Dynamic search rules are returned.", body = ListRulesResponse, content_type = "application/json", example = json!({
            "results": [
                {
                    "uid": "black-friday",
                    "description": "Black Friday 2025 rules",
                    "priority": 10,
                    "active": true,
                    "conditions": [
                        { "scope": "query", "isEmpty": true },
                        { "scope": "time", "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z" }
                    ],
                    "actions": [
                        {
                            "selector": { "indexUid": "products", "id": "123" },
                            "action": { "type": "pin", "position": 1 }
                        }
                    ]
                }
            ]
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
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let rules = index_scheduler.dynamic_search_rules();
    let results = rules.values().cloned().collect::<Vec<_>>();
    let response = ListRulesResponse { results };

    debug!(returns = ?response, "list dynamic search rules");
    Ok(HttpResponse::Ok().json(response))
}

/// Get a dynamic search rule
///
/// Retrieve a single dynamic search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the dynamic search rule.", nullable = false)),
    responses(
        (status = OK, description = "Dynamic search rule returned.", body = DynamicSearchRule, content_type = "application/json", example = json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "priority": 10,
            "active": true,
            "conditions": [
                { "scope": "query", "isEmpty": true },
                { "scope": "time", "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z" }
            ],
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
    uid: Path<String>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let uid = uid.into_inner();
    let rules = index_scheduler.dynamic_search_rules();
    let rule = rules.get(&uid).ok_or(DynamicSearchRulesError::NotFound(uid))?;

    debug!(returns = ?rule, "get dynamic search rule");
    Ok(HttpResponse::Ok().json(rule))
}

/// Create a dynamic search rule
///
/// Create a new dynamic search rule with optional metadata, matching conditions, and actions.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.post", "dynamicSearchRules.*", "*.post", "*"])),
    request_body = CreateDynamicSearchRuleRequest,
    responses(
        (status = CREATED, description = "Dynamic search rule created.", body = DynamicSearchRule, content_type = "application/json", example = json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "priority": 10,
            "active": true,
            "conditions": [
                { "scope": "query", "isEmpty": true },
                { "scope": "time", "start": "2025-11-28T00:00:00Z", "end": "2025-11-28T23:59:59Z" }
            ],
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
        (status = 400, description = "Bad request.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "Dynamic search rule `black-friday` already exists.",
            "code": "bad_request",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#bad_request"
        })),
    ),
)]
async fn create_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_CREATE }>,
        Data<IndexScheduler>,
    >,
    body: AwebJson<CreateDynamicSearchRuleRequest, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let body = body.into_inner();
    let rules = index_scheduler.dynamic_search_rules();

    if rules.contains_key(&body.uid) {
        return Err(DynamicSearchRulesError::AlreadyExists(body.uid).into());
    }

    let rule = DynamicSearchRule {
        uid: body.uid,
        description: body.description,
        priority: body.priority,
        active: body.active,
        conditions: body.conditions,
        actions: body.actions,
    };

    index_scheduler.put_dynamic_search_rule(&rule)?;
    analytics.publish(CreateDynamicSearchRuleAnalytics, &req);

    debug!(returns = ?rule, "created dynamic search rule");
    Ok(HttpResponse::Created().json(rule))
}

/// Update a dynamic search rule
///
/// Partially update a dynamic search rule by replacing the provided fields.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.patch", "dynamicSearchRules.*", "*.patch", "*"])),
    request_body = UpdateDynamicSearchRuleRequest,
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the dynamic search rule.", nullable = false)),
    responses(
        (status = OK, description = "Dynamic search rule updated.", body = DynamicSearchRule, content_type = "application/json", example = json!({
            "uid": "black-friday",
            "description": "Black Friday 2025 rules",
            "priority": 5,
            "active": true,
            "conditions": [
                { "scope": "query", "isEmpty": true }
            ],
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
async fn update_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<String>,
    body: AwebJson<UpdateDynamicSearchRuleRequest, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let uid = uid.into_inner();
    let body = body.into_inner();
    let rules = index_scheduler.dynamic_search_rules();

    let mut rule =
        rules.get(&uid).cloned().ok_or_else(|| DynamicSearchRulesError::NotFound(uid.clone()))?;

    if let Some(description) = body.description {
        rule.description = Some(description);
    }

    if let Some(priority) = body.priority {
        rule.priority = Some(priority);
    }

    if let Some(active) = body.active {
        rule.active = active;
    }

    if let Some(conditions) = body.conditions {
        rule.conditions = conditions;
    }

    if let Some(actions) = body.actions {
        rule.actions = actions;
    }

    index_scheduler.put_dynamic_search_rule(&rule)?;
    analytics.publish(UpdateDynamicSearchRuleAnalytics, &req);

    debug!(returns = ?rule, "updated dynamic search rule");
    Ok(HttpResponse::Ok().json(rule))
}

/// Delete a dynamic search rule
///
/// Delete a dynamic search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.delete", "dynamicSearchRules.*", "*.delete", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the dynamic search rule.", nullable = false)),
    responses(
        (status = NO_CONTENT, description = "Dynamic search rule deleted."),
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
async fn delete_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_DELETE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<String>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let uid = uid.into_inner();
    let deleted = index_scheduler.delete_dynamic_search_rule(&uid)?;

    if !deleted {
        return Err(DynamicSearchRulesError::NotFound(uid).into());
    }

    analytics.publish(DeleteDynamicSearchRuleAnalytics, &req);

    debug!("deleted dynamic search rule `{uid}`");
    Ok(HttpResponse::NoContent().finish())
}
