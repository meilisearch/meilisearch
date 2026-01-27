use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::{
    InvalidIndexFieldsFilter, InvalidIndexFieldsFilterAttributePatterns,
    InvalidIndexFieldsFilterDisplayed, InvalidIndexFieldsFilterDistinct,
    InvalidIndexFieldsFilterFilterable, InvalidIndexFieldsFilterRankingRule,
    InvalidIndexFieldsFilterSearchable, InvalidIndexFieldsFilterSortable, InvalidIndexLimit,
    InvalidIndexOffset,
};
use meilisearch_types::error::ResponseError;
use meilisearch_types::facet_values_sort::FacetValuesSort;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::tokenizer::Language;
use meilisearch_types::milli::{
    AttributePatterns, FieldSortOrder, FilterFeatures, FilterableAttributesFeatures,
    MetadataBuilder, PatternMatch,
};
use serde::{Serialize, Serializer};
use utoipa::ToSchema;

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::{Pagination, PAGINATION_DEFAULT_LIMIT};

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Field<'a> {
    pub name: &'a str,
    pub displayed: FieldDisplayConfig,
    pub searchable: FieldSearchConfig,
    pub sortable: FieldSortableConfig,
    pub distinct: FieldDistinctConfig,
    pub ranking_rule: FieldRankingRuleConfig,
    pub filterable: FieldFilterableConfig,
    pub localized: FieldLocalizedConfig<'a>,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldDisplayConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldSearchConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldSortableConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldRankingRuleConfig {
    pub enabled: bool,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_field_sort_order"
    )]
    #[schema(value_type = Vec<String>)]
    pub order: Option<FieldSortOrder>,
}

fn serialize_field_sort_order<S: Serializer>(
    value: &Option<FieldSortOrder>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if let Some(value) = value {
        match value {
            FieldSortOrder::Asc => serializer.serialize_str("asc"),
            FieldSortOrder::Desc => serializer.serialize_str("desc"),
        }
    } else {
        serializer.serialize_none()
    }
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldDistinctConfig {
    pub enabled: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldFilterableConfig {
    pub enabled: bool,
    pub sort_by: FacetValuesSort,
    pub facet_search: bool,
    pub equality: bool,
    pub comparison: bool,
}

#[derive(Debug, Serialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FieldLocalizedConfig<'a> {
    #[schema(value_type = Vec<String>)]
    pub locales: &'a [Language],
}

#[derive(Deserr, Debug, Clone, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct ListFields {
    #[deserr(default, error = DeserrJsonError<InvalidIndexOffset>)]
    pub offset: usize,
    #[deserr(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidIndexLimit>)]
    pub limit: usize,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilter>)]
    pub filter: Option<ListFieldsFilter>,
}

impl ListFields {
    fn apply_filter(&self, field: &Field) -> bool {
        if let Some(filter) = &self.filter {
            if let Some(patterns) = &filter.attribute_patterns {
                if matches!(patterns.match_str(field.name), PatternMatch::NoMatch) {
                    return false;
                }
            }

            if let Some(displayed) = &filter.displayed {
                if *displayed != field.displayed.enabled {
                    return false;
                }
            }

            if let Some(searchable) = &filter.searchable {
                if *searchable != field.searchable.enabled {
                    return false;
                }
            }

            if let Some(sortable) = &filter.sortable {
                if *sortable != field.sortable.enabled {
                    return false;
                }
            }

            if let Some(distinct) = &filter.distinct {
                if *distinct != field.distinct.enabled {
                    return false;
                }
            }

            if let Some(ranking_rule) = &filter.ranking_rule {
                if *ranking_rule != field.ranking_rule.enabled {
                    return false;
                }
            }

            if let Some(filterable) = &filter.filterable {
                if *filterable != field.filterable.enabled {
                    return false;
                }
            }

            return true;
        }

        true
    }
}

#[derive(Deserr, Debug, Clone, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidIndexFieldsFilter>, rename_all = camelCase, deny_unknown_fields)]
pub struct ListFieldsFilter {
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterAttributePatterns>)]
    pub attribute_patterns: Option<AttributePatterns>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterDisplayed>)]
    pub displayed: Option<bool>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterSearchable>)]
    pub searchable: Option<bool>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterSortable>)]
    pub sortable: Option<bool>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterDistinct>)]
    pub distinct: Option<bool>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterRankingRule>)]
    pub ranking_rule: Option<bool>,
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterFilterable>)]
    pub filterable: Option<bool>,
}

#[utoipa::path(
    post,
    path = "/{indexUid}/fields",
    tag = "Fields",
    security(("Bearer" = ["fields.post", "fields.*", "*"])),
    params(("indexUid", example = "movies", description = "Index Unique Identifier", nullable = false)),
    request_body = ListFields,
)]
pub async fn post_index_fields(
    index_scheduler: GuardedData<ActionPolicy<{ actions::FIELDS_POST }>, Data<IndexScheduler>>,
    index_uid: web::Path<String>,
    body: AwebJson<ListFields, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    let index = index_scheduler.index(index_uid.as_str())?;
    let rtxn = index.read_txn()?;
    let builder = MetadataBuilder::from_index(&index, &rtxn)?;
    let fields = builder
        .fields_metadata()
        .iter()
        .filter_map(|(name, metadata)| {
            let FilterableAttributesFeatures { facet_search, filter } =
                metadata.filterable_attributes_features(builder.filterable_attributes());
            let FilterFeatures { equality, comparison } = filter;
            let is_filterable = equality || comparison || facet_search;

            let locales = builder
                .localized_attributes_rules()
                .and_then(|rules| metadata.locales(rules))
                .unwrap_or_default();

            let field = Field {
                name,
                displayed: FieldDisplayConfig { enabled: metadata.displayed },
                searchable: FieldSearchConfig { enabled: metadata.searchable.is_some() },
                sortable: FieldSortableConfig { enabled: metadata.sortable },
                distinct: FieldDistinctConfig { enabled: metadata.distinct },
                ranking_rule: FieldRankingRuleConfig {
                    enabled: metadata.is_asc_desc(),
                    order: metadata.asc_desc,
                },
                filterable: FieldFilterableConfig {
                    enabled: is_filterable,
                    sort_by: metadata.sort_by.into(),
                    facet_search,
                    equality,
                    comparison,
                },
                localized: FieldLocalizedConfig { locales },
            };

            if !body.0.apply_filter(&field) {
                return None;
            }

            Some(field)
        })
        // collect into a vector to get the total length for pagination
        .collect::<Vec<_>>();

    let pagination = Pagination { offset: body.0.offset, limit: body.0.limit };
    let pagination_view = pagination.auto_paginate_sized(fields.into_iter());

    Ok(HttpResponse::Ok().json(pagination_view))
}
