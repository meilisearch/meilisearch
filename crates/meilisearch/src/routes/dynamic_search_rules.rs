use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::dynamic_search_rules::{Condition, DynamicSearchRule, RuleAction, RuleUid};
use meilisearch_types::error::deserr_codes::{
    InvalidDynamicSearchRuleFilter, InvalidDynamicSearchRuleFilterActive,
    InvalidDynamicSearchRuleFilterAttributePatterns, InvalidDynamicSearchRuleLimit,
    InvalidDynamicSearchRuleOffset,
};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::{AttributePatterns, PatternMatch};
use serde::Serialize;
use utoipa::ToSchema;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::{Pagination, PaginationView, PAGINATION_DEFAULT_LIMIT};

#[routes::routes(
    routes(
        "" => [post(list_rules)],
        "/{uid}" => [get(get_rule), post(create_rule), patch(update_rule), delete(delete_rule)],
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
    #[schema(value_type = Option<String>)]
    description: Setting<String>,
    /// Priority of the dynamic search rule. Lower values take precedence over higher ones.
    #[deserr(default)]
    #[schema(value_type = Option<u64>)]
    priority: Setting<u64>,
    /// Whether the dynamic search rule is active.
    #[deserr(default)]
    #[schema(value_type = Option<bool>)]
    active: Setting<bool>,
    /// Conditions that must match before the dynamic search rule applies.
    #[deserr(default)]
    #[schema(value_type = Option<Vec<Condition>>)]
    conditions: Setting<Vec<Condition>>,
    /// Actions to apply when the dynamic search rule matches.
    #[deserr(default)]
    #[schema(value_type = Option<Vec<RuleAction>>)]
    actions: Setting<Vec<RuleAction>>,
}

#[derive(Deserr, Debug, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidDynamicSearchRuleFilter>, rename_all = camelCase, deny_unknown_fields)]
pub struct ListRulesFilter {
    /// Only include rules whose names match these patterns (e.g. `["black-friday", "promo*"]`).
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilterAttributePatterns>)]
    pub attribute_patterns: Option<AttributePatterns>,
    /// Only include rules that are active (true) or not active (false).
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilterActive>)]
    pub active: Option<bool>,
}

#[derive(Deserr, Debug, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct ListRules {
    /// Number of rules to skip. Defaults to 0.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleOffset>)]
    pub offset: usize,
    /// Maximum number of rules to return. Default to 20.
    #[schema(required = false)]
    #[deserr(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidDynamicSearchRuleLimit>)]
    pub limit: usize,
    /// Optional filter to restrict which rules are returned (e.g. by attribute patterns or by properties like if the rule is active or not)
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleFilter>)]
    pub filter: Option<ListRulesFilter>,
}

impl ListRules {
    fn apply_filter(&self, rule: &DynamicSearchRule) -> bool {
        if let Some(filter) = &self.filter {
            if let Some(patterns) = &filter.attribute_patterns {
                if matches!(
                    patterns.match_str(&rule.uid),
                    PatternMatch::NoMatch | PatternMatch::Parent
                ) {
                    return false;
                }
            }

            if let Some(active) = &filter.active {
                if *active != rule.active {
                    return false;
                }
            }
        }

        true
    }
}

#[derive(Debug, thiserror::Error)]
enum DynamicSearchRulesError {
    #[error("Dynamic search rule `{0}` not found.")]
    NotFound(RuleUid),
    #[error("Dynamic search rule `{0}` already exists.")]
    AlreadyExists(RuleUid),
    #[error("Cannot reset Dynamic search rule `{0}` action.")]
    CannotResetActions(RuleUid),
}

impl ErrorCode for DynamicSearchRulesError {
    fn error_code(&self) -> Code {
        match self {
            DynamicSearchRulesError::NotFound(_) => Code::DynamicSearchRuleNotFound,
            DynamicSearchRulesError::AlreadyExists(_) => Code::BadRequest,
            DynamicSearchRulesError::CannotResetActions(_) => {
                Code::CannotResetDynamicSearchRuleActions
            }
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
    request_body = ListRules,
    responses(
        (status = OK, description = "Dynamic search rules are returned.", body = PaginationView<DynamicSearchRule>, content_type = "application/json", example = json!({
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
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let rules = index_scheduler
        .dynamic_search_rules()
        .values()
        .filter(|rule| body.0.apply_filter(rule))
        .cloned()
        .collect::<Vec<_>>();

    let pagination = Pagination { offset: body.0.offset, limit: body.0.limit };
    let pagination_view = pagination.auto_paginate_sized(rules.into_iter());

    Ok(HttpResponse::Ok().json(pagination_view))
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
    uid: Path<RuleUid>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let uid = uid.into_inner();
    let rules = index_scheduler.dynamic_search_rules();
    let rule = rules.get(&uid).ok_or(DynamicSearchRulesError::NotFound(uid))?;

    Ok(HttpResponse::Ok().json(rule))
}

/// Create a dynamic search rule
///
/// Create a new dynamic search rule with optional metadata, matching conditions, and actions.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.create", "dynamicSearchRules.*", "*"])),
    request_body = CreateDynamicSearchRuleRequest,
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the dynamic search rule.", nullable = false)),
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
        (status = 404, description = "Dynamic search rule not found.", body = ResponseError, content_type = "application/json", example = json!({
            "message": "Dynamic search rule `black-friday` not found.",
            "code": "dynamic_search_rule_not_found",
            "type": "invalid_request",
            "link": "https://docs.meilisearch.com/errors#dynamic_search_rule_not_found"
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
    uid: Path<RuleUid>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let body = body.into_inner();
    let rules = index_scheduler.dynamic_search_rules();
    let uid = uid.into_inner();

    if rules.contains_key(&uid) {
        return Err(DynamicSearchRulesError::AlreadyExists(uid).into());
    }

    let rule = DynamicSearchRule {
        uid,
        description: body.description,
        priority: body.priority,
        active: body.active,
        conditions: body.conditions,
        actions: body.actions,
    };

    index_scheduler.put_dynamic_search_rule(&rule)?;
    analytics.publish(CreateDynamicSearchRuleAnalytics, &req);

    Ok(HttpResponse::Created().json(rule))
}

/// Update a dynamic search rule
///
/// Partially update a dynamic search rule by replacing the provided fields.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.update", "dynamicSearchRules.*", "*"])),
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
    uid: Path<RuleUid>,
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

    match body.description {
        Setting::Set(description) => rule.description = Some(description),
        Setting::Reset => rule.description = None,
        Setting::NotSet => (),
    }

    match body.priority {
        Setting::Set(priority) => rule.priority = Some(priority),
        Setting::Reset => rule.priority = None,
        Setting::NotSet => (),
    }

    match body.active {
        Setting::Set(active) => rule.active = active,
        Setting::Reset => rule.active = false,
        Setting::NotSet => (),
    }

    match body.conditions {
        Setting::Set(conditions) => rule.conditions = conditions,
        Setting::Reset => rule.conditions.clear(),
        Setting::NotSet => (),
    }

    match body.actions {
        Setting::Set(actions) => rule.actions = actions,
        Setting::Reset => return Err(DynamicSearchRulesError::CannotResetActions(uid).into()),
        Setting::NotSet => (),
    }

    index_scheduler.put_dynamic_search_rule(&rule)?;
    analytics.publish(UpdateDynamicSearchRuleAnalytics, &req);

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
    uid: Path<RuleUid>,
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

    Ok(HttpResponse::NoContent().finish())
}
