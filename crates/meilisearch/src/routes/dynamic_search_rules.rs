use actix_web::web::{self, Data, Path};
use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::dynamic_search_rules::{Condition, DynamicSearchRule, RuleAction};
use meilisearch_types::error::{Code, ErrorCode, ResponseError};
use meilisearch_types::keys::actions;
use serde::{Deserialize, Serialize};
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
    tag = "DynamicSearchRules",
    tags((
        name = "DynamicSearchRules",
        description = "The `/dynamic-search-rules` route allows you to configure dynamic search rules.",
    ))
)]
pub struct DynamicSearchRulesApi;

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
struct CreateDynamicSearchRuleRequest {
    uid: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    priority: Option<u64>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    conditions: Vec<Condition>,
    actions: Vec<RuleAction>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
struct UpdateDynamicSearchRuleRequest {
    description: Option<String>,
    priority: Option<u64>,
    active: Option<bool>,
    conditions: Option<Vec<Condition>>,
    actions: Option<Vec<RuleAction>>,
}

#[derive(Debug, Serialize)]
struct ListRulesResponse<'a> {
    results: Vec<&'a DynamicSearchRule>,
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

#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
)]
async fn list_rules(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_GET }>,
        Data<IndexScheduler>,
    >,
) -> Result<HttpResponse, ResponseError> {
    let rules = index_scheduler.dynamic_search_rules();
    let results = rules.values().collect::<Vec<_>>();
    let response = ListRulesResponse { results };

    debug!(returns = ?response, "list dynamic search rules");
    Ok(HttpResponse::Ok().json(response))
}

#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.get", "dynamicSearchRules.*", "*.get", "*"])),
)]
async fn get_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_GET }>,
        Data<IndexScheduler>,
    >,
    uid: Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let uid = uid.into_inner();
    let rules = index_scheduler.dynamic_search_rules();
    let rule = rules.get(&uid).ok_or(DynamicSearchRulesError::NotFound(uid))?;

    debug!(returns = ?rule, "get dynamic search rule");
    Ok(HttpResponse::Ok().json(rule))
}

#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.post", "dynamicSearchRules.*", "*.post", "*"])),
    request_body = CreateDynamicSearchRuleRequest,
)]
async fn create_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_CREATE }>,
        Data<IndexScheduler>,
    >,
    body: web::Json<CreateDynamicSearchRuleRequest>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
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

#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.patch", "dynamicSearchRules.*", "*.patch", "*"])),
    request_body = UpdateDynamicSearchRuleRequest,
)]
async fn update_rule(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::DYNAMIC_SEARCH_RULES_UPDATE }>,
        Data<IndexScheduler>,
    >,
    uid: Path<String>,
    body: web::Json<UpdateDynamicSearchRuleRequest>,
    req: HttpRequest,
    analytics: Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
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

#[routes::path(
    security(("Bearer" = ["dynamicSearchRules.delete", "dynamicSearchRules.*", "*.delete", "*"])),
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
    let uid = uid.into_inner();
    let deleted = index_scheduler.delete_dynamic_search_rule(&uid)?;

    if !deleted {
        return Err(DynamicSearchRulesError::NotFound(uid).into());
    }

    analytics.publish(DeleteDynamicSearchRuleAnalytics, &req);

    debug!("deleted dynamic search rule `{uid}`");
    Ok(HttpResponse::NoContent().finish())
}
