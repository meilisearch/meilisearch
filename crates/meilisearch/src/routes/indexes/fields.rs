use std::collections::BTreeMap;

use actix_web::web::Data;
use actix_web::{web, HttpResponse};
use deserr::actix_web::AwebQueryParameter;
use deserr::Deserr;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::DeserrQueryParamError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::ResponseError;
use meilisearch_types::facet_values_sort::FacetValuesSort;
use meilisearch_types::locales::Locale;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::{self, FilterableAttributesRule, LocalizedAttributesRule};
use meilisearch_types::settings::{settings, SecretPolicy};

use index_scheduler::IndexScheduler;
use serde::Serialize;
use utoipa::{IntoParams, ToSchema};

use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;

use super::{Pagination, PAGINATION_DEFAULT_LIMIT};

/// Field configuration for a specific field in the index
#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    pub displayed: FieldDisplayConfig,
    pub searchable: FieldSearchConfig,
    pub distinct: FieldDistinctConfig,
    pub filterable: FieldFilterableConfig,
    pub localized: FieldLocalizedConfig,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldDisplayConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldSearchConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldDistinctConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFacetSearchConfig {
    pub sort_by: String,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFilterConfig {
    pub equality: bool,
    pub comparison: bool,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFilterableConfig {
    pub enabled: bool,
    pub facet_search: FieldFacetSearchConfig,
    pub filter: FieldFilterConfig,
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldLocalizedConfig {
    pub locales: Vec<String>,
}

#[derive(Deserr, Debug, Clone, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct ListFields {
    #[param(value_type = Option<String>, example = "cat*")]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexFieldsSearch>)]
    pub search: Option<String>,
    #[param(value_type = Option<usize>, default, example = 100)]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexOffset>)]
    pub offset: Param<usize>,
    #[param(value_type = Option<usize>, default = 20, example = 1)]
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidIndexLimit>)]
    pub limit: Param<usize>,
    #[param(value_type = Option<String>, example = "displayed.enabled = true && localized.locales : fra")]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexFieldsFilter>)]
    pub filter: Option<String>,
}

impl ListFields {
    fn into_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

#[derive(Debug)]
enum FilterCond {
    BoolEq { path: Vec<String>, value: bool },
    Contains { path: Vec<String>, value: String },
}

fn parse_filter(expr: &str) -> Vec<FilterCond> {
    let mut conditions = Vec::new();
    for cond_str in expr.split("&&") {
        let cond_str = cond_str.trim();
        if cond_str.is_empty() {
            continue;
        }
        if let Some((lhs, rhs)) = cond_str.split_once('=') {
            let path: Vec<String> = lhs.trim().split('.').map(|s| s.trim().to_string()).collect();
            let value = matches!(rhs.trim(), "true" | "True" | "TRUE");
            conditions.push(FilterCond::BoolEq { path, value });
        } else if let Some((lhs, rhs)) = cond_str.split_once(':') {
            let path: Vec<String> = lhs.trim().split('.').map(|s| s.trim().to_string()).collect();
            conditions.push(FilterCond::Contains { path, value: rhs.trim().to_string() });
        }
    }
    conditions
}

fn field_satisfies(field: &Field, conds: &[FilterCond]) -> bool {
    let field_value = serde_json::to_value(field).unwrap_or_default();
    for cond in conds {
        match cond {
            FilterCond::BoolEq { path, value } => {
                let mut current = &field_value;
                for part in path {
                    if let Some(map) = current.as_object() {
                        current = map.get(part.as_str()).unwrap_or(&serde_json::Value::Null);
                    } else {
                        return false;
                    }
                }
                match current {
                    serde_json::Value::Bool(v) if v == value => {}
                    _ => return false,
                }
            }
            FilterCond::Contains { path, value } => {
                let mut current = &field_value;
                for part in path {
                    if let Some(map) = current.as_object() {
                        current = map.get(part.as_str()).unwrap_or(&serde_json::Value::Null);
                    } else {
                        return false;
                    }
                }
                match current {
                    serde_json::Value::String(s) if s.contains(value) => {}
                    serde_json::Value::Array(arr) => {
                        if !arr
                            .iter()
                            .any(|v| matches!(v, serde_json::Value::String(s) if s == value))
                        {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
        }
    }
    true
}

fn field_matches_search(field_name: &str, search_pattern: &str) -> bool {
    if search_pattern.is_empty() {
        return true;
    }
    if let Some(stripped) = search_pattern.strip_suffix('*') {
        field_name.starts_with(stripped)
    } else if let Some(stripped) = search_pattern.strip_prefix('*') {
        field_name.ends_with(stripped)
    } else if search_pattern.contains('*') {
        let parts: Vec<&str> = search_pattern.split('*').collect();
        if parts.len() == 2 {
            field_name.starts_with(parts[0]) && field_name.ends_with(parts[1])
        } else {
            field_name.contains(&search_pattern.replace('*', ""))
        }
    } else {
        field_name.contains(search_pattern)
    }
}

fn enrich_field(
    field_name: String,
    displayed_fields: &[String],
    searchable_fields: &[String],
    distinct_field: Option<&String>,
    filterable_rules: &[FilterableAttributesRule],
    localized_rules: &[LocalizedAttributesRule],
    facet_sort_config: &BTreeMap<String, FacetValuesSort>,
) -> Field {
    let displayed_enabled = displayed_fields.iter().any(|f| f == "*" || f == &field_name);
    let searchable_enabled = searchable_fields.iter().any(|f| f == "*" || f == &field_name);
    let distinct_enabled = distinct_field == Some(&field_name);

    let is_filterable = filterable_rules.iter().any(|rule| match rule {
        FilterableAttributesRule::Pattern(p) => {
            p.match_str(&field_name) == milli::PatternMatch::Match
        }
        FilterableAttributesRule::Field(f) => f == &field_name,
    });

    let sort_by = facet_sort_config
        .get(&field_name)
        .or_else(|| facet_sort_config.get("*"))
        .map(|s| match s {
            FacetValuesSort::Alpha => "alpha",
            FacetValuesSort::Count => "count",
        })
        .unwrap_or("alpha")
        .to_string();

    let mut locales = Vec::new();
    for rule in localized_rules {
        if rule.match_str(&field_name) == milli::PatternMatch::Match {
            locales.extend(rule.locales().iter().map(|l| {
                let loc: Locale = (*l).into();
                format!("{:?}", loc).to_lowercase()
            }));
        }
    }

    Field {
        name: field_name,
        displayed: FieldDisplayConfig { enabled: displayed_enabled },
        searchable: FieldSearchConfig { enabled: searchable_enabled },
        distinct: FieldDistinctConfig { enabled: distinct_enabled },
        filterable: FieldFilterableConfig {
            enabled: is_filterable,
            facet_search: FieldFacetSearchConfig { sort_by },
            filter: FieldFilterConfig { equality: is_filterable, comparison: is_filterable },
        },
        localized: FieldLocalizedConfig { locales },
    }
}

pub async fn get_index_fields(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_GET }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    params: AwebQueryParameter<ListFields, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    let index_uid = meilisearch_types::index_uid::IndexUid::try_from(index_uid.into_inner())?;
    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.read_txn()?;

    let all_field_names: Vec<String> =
        index.fields_ids_map(&rtxn)?.names().map(|s| s.to_string()).collect();

    let settings = settings(&index, &rtxn, SecretPolicy::RevealSecrets)?;

    let displayed_fields = match &*settings.displayed_attributes {
        Setting::Set(f) => f.clone(),
        Setting::Reset | Setting::NotSet => vec!["*".to_string()],
    };
    let searchable_fields = match &*settings.searchable_attributes {
        Setting::Set(f) => f.clone(),
        Setting::Reset | Setting::NotSet => vec!["*".to_string()],
    };
    let distinct_field = match &settings.distinct_attribute {
        Setting::Set(f) => Some(f),
        _ => None,
    };
    let filterable_rules = match &settings.filterable_attributes {
        Setting::Set(r) => r.clone(),
        _ => Vec::new(),
    };
    let localized_rules: Vec<LocalizedAttributesRule> = match &settings.localized_attributes {
        Setting::Set(r) => r.iter().cloned().map(|r| r.into()).collect(),
        _ => Vec::new(),
    };
    let facet_sort_config = match &settings.faceting {
        Setting::Set(f) => match &f.sort_facet_values_by {
            Setting::Set(c) => c.clone(),
            _ => BTreeMap::new(),
        },
        _ => BTreeMap::new(),
    };

    let filtered_fields: Vec<String> = if let Some(search) = &params.search {
        all_field_names.into_iter().filter(|n| field_matches_search(n, search)).collect()
    } else {
        all_field_names
    };

    let mut enriched_fields: Vec<Field> = filtered_fields
        .into_iter()
        .map(|n| {
            enrich_field(
                n,
                &displayed_fields,
                &searchable_fields,
                distinct_field,
                &filterable_rules,
                &localized_rules,
                &facet_sort_config,
            )
        })
        .collect();

    if let Some(expr) = &params.filter {
        let conds = parse_filter(expr);
        enriched_fields.retain(|f| field_satisfies(f, &conds));
    }

    let total = enriched_fields.len();
    let pagination = (*params).clone().into_pagination();
    let paginated_fields = enriched_fields
        .into_iter()
        .skip(pagination.offset)
        .take(pagination.limit)
        .collect::<Vec<_>>();
    let ret = pagination.format_with(total, paginated_fields);
    Ok(HttpResponse::Ok().json(ret))
}
