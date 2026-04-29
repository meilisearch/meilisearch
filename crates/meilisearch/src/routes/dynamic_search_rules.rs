use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::dynamic_search_rules::{Condition, DynamicSearchRule, RuleAction, RuleUid};
use meilisearch_types::error::deserr_codes::{
    InvalidDynamicSearchRuleActions, InvalidDynamicSearchRuleActive,
    InvalidDynamicSearchRuleConditions, InvalidDynamicSearchRuleDescription,
    InvalidDynamicSearchRuleFilter, InvalidDynamicSearchRuleFilterActive,
    InvalidDynamicSearchRuleFilterAttributePatterns, InvalidDynamicSearchRuleLimit,
    InvalidDynamicSearchRuleOffset, InvalidDynamicSearchRulePriority,
    InvalidDynamicSearchRuleQuery,
};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::AttributePatterns;
use serde::Serialize;
use utoipa::ToSchema;

use crate::analytics::{Aggregate, Analytics};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::{PaginationView, PAGINATION_DEFAULT_LIMIT};

#[routes::routes(
    routes(
        "" => [post(list_rules)],
        "/{uid}" => [get(get_rule), patch(update_or_create_rule), delete(delete_rule)],
    ),
    tag = "Search rules",
    tags((
        name = "Search rules",
        description = "The `/dynamic-search-rules` route allows you to configure search rules.",
    ))
)]
pub struct DynamicSearchRulesApi;

#[derive(Debug, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
struct UpdateOrCreateDynamicSearchRuleRequest {
    /// Human-readable description of the dynamic search rule.
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleDescription>)]
    #[schema(value_type = Option<String>)]
    description: Setting<String>,
    /// Precedence of the dynamic search rule. Lower numeric values take precedence over higher
    /// ones. If omitted, the rule is treated as having the lowest precedence. This precedence is
    /// used to resolve conflicts between matching rules:
    /// - If the same document is selected by multiple rules, the smallest `priority` number wins
    /// - If different documents are pinned to the same position, they are ordered by ascending `priority`
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRulePriority>)]
    #[schema(value_type = Option<u64>)]
    priority: Setting<u64>,
    /// Whether the dynamic search rule is active.
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleActive>)]
    #[schema(value_type = Option<bool>)]
    active: Setting<bool>,
    /// Conditions that must match before the dynamic search rule applies.
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleConditions>)]
    #[schema(value_type = Option<Vec<Condition>>)]
    conditions: Setting<Vec<Condition>>,
    /// Actions to apply when the dynamic search rule matches.
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleActions>)]
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
    /// Search query used to rank and restrict rules by description, query conditions, and targeted index.
    #[schema(required = false)]
    #[deserr(default, error = DeserrJsonError<InvalidDynamicSearchRuleQuery>)]
    pub q: Option<String>,
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

#[derive(Debug, thiserror::Error)]
enum DynamicSearchRulesError {
    #[error("Dynamic search rule `{0}` not found.")]
    NotFound(RuleUid),
    #[error("Cannot reset the actions of a dynamic search rule.\n - Note: for rule `{0}`.")]
    CannotResetActions(RuleUid),
    #[error(
        "Cannot set an empty list of actions to a dynamic search rule.\n - Note: for rule `{0}`."
    )]
    EmptyActions(RuleUid),
}

impl ErrorCode for DynamicSearchRulesError {
    fn error_code(&self) -> Code {
        match self {
            DynamicSearchRulesError::NotFound(_) => Code::DynamicSearchRuleNotFound,
            DynamicSearchRulesError::CannotResetActions(_) => Code::InvalidDynamicSearchRuleActions,
            DynamicSearchRulesError::EmptyActions(_) => Code::InvalidDynamicSearchRuleActions,
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

    let ListRules { q, offset, limit, filter } = body.0;
    let active = filter.as_ref().and_then(|filter| filter.active);
    let attribute_patterns = filter.as_ref().and_then(|filter| filter.attribute_patterns.as_ref());
    let page = index_scheduler.list_dynamic_search_rules(
        q.as_deref(),
        active,
        attribute_patterns,
        offset,
        limit,
    )?;
    let pagination_view = PaginationView::new(page.offset, page.limit, page.total, page.results);

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
    let rules = index_scheduler.dynamic_search_rules()?;
    let rule = rules.get(&uid).ok_or(DynamicSearchRulesError::NotFound(uid))?;

    Ok(HttpResponse::Ok().json(rule))
}

/// Create or update a search rule
///
/// Partially update a search rule by replacing the provided fields. If the rule doesn't exist, it will be created.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.update", "dynamicSearchRules.*", "*"])),
    request_body = UpdateOrCreateDynamicSearchRuleRequest,
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
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
async fn update_or_create_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<RuleUid>,
    body: AwebJson<UpdateOrCreateDynamicSearchRuleRequest, DeserrJsonError>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler
        .features()
        .check_dynamic_search_rules("Using the `/dynamic-search-rules` routes")?;

    let uid = uid.into_inner();
    let UpdateOrCreateDynamicSearchRuleRequest {
        description: new_description,
        priority: new_priority,
        active: new_active,
        conditions: new_conditions,
        actions: new_actions,
    } = body.into_inner();

    let rules = index_scheduler.dynamic_search_rules()?;
    let (mut rule, is_new) = rules
        .get(&uid)
        .cloned()
        .map(|r| (r, false))
        .unwrap_or_else(|| (private_default_dynamic_search_rule(uid.clone()), true));

    let DynamicSearchRule { uid: _, description, priority, active, conditions, actions } =
        &mut rule;

    match new_description {
        Setting::Set(new_description) => *description = Some(new_description),
        Setting::Reset => *description = None,
        Setting::NotSet => (),
    }

    match new_priority {
        Setting::Set(new_priority) => *priority = Some(new_priority),
        Setting::Reset => *priority = None,
        Setting::NotSet => (),
    }

    match new_active {
        Setting::Set(new_active) => *active = new_active,
        Setting::Reset => *active = true,
        Setting::NotSet => (),
    }

    match new_conditions {
        Setting::Set(new_conditions) => *conditions = new_conditions,
        Setting::Reset => conditions.clear(),
        Setting::NotSet => (),
    }

    match new_actions {
        Setting::Set(new_actions) if new_actions.is_empty() => {
            return Err(DynamicSearchRulesError::EmptyActions(uid).into())
        }
        Setting::Set(new_actions) => *actions = new_actions,
        Setting::Reset => return Err(DynamicSearchRulesError::CannotResetActions(uid).into()),
        Setting::NotSet if is_new => return Err(DynamicSearchRulesError::EmptyActions(uid).into()),
        Setting::NotSet => (),
    }

    index_scheduler.put_dynamic_search_rule(&rule)?;

    if is_new {
        analytics.publish(CreateDynamicSearchRuleAnalytics, &req);
        Ok(HttpResponse::Created().json(rule))
    } else {
        analytics.publish(UpdateDynamicSearchRuleAnalytics, &req);
        Ok(HttpResponse::Ok().json(rule))
    }
}

/// Delete a search rule
///
/// Delete a search rule by its unique identifier.
#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.delete", "dynamicSearchRules.*", "*.delete", "*"])),
    params(("uid" = String, Path, example = "black-friday", description = "Unique identifier of the search rule.", nullable = false)),
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

fn private_default_dynamic_search_rule(uid: RuleUid) -> DynamicSearchRule {
    DynamicSearchRule {
        uid,
        description: None,
        priority: None,
        active: true,
        conditions: vec![],
        actions: vec![],
    }
}
