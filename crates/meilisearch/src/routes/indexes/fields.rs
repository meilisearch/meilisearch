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
    FilterableAttributesRule, LocalizedAttributesRule, Metadata, MetadataBuilder, PatternMatch,
};
use serde::{Serialize, Serializer};
use utoipa::ToSchema;

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::{Pagination, PaginationView, PAGINATION_DEFAULT_LIMIT};

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

impl<'a> Field<'a> {
    pub fn new(
        name: &'a str,
        metadata: &Metadata,
        filterable_attributes: &[FilterableAttributesRule],
        localized_attributes_rules: Option<&'a [LocalizedAttributesRule]>,
    ) -> Self {
        let FilterableAttributesFeatures { facet_search, filter } =
            metadata.filterable_attributes_features(filterable_attributes);
        let FilterFeatures { equality, comparison } = filter;
        let is_filterable = equality || comparison || facet_search;

        let locales = localized_attributes_rules
            .and_then(|rules| metadata.locales(rules))
            .unwrap_or_default();

        Field {
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
        }
    }
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
    /// Number of fields to skip. Defaults to 0.
    #[deserr(default, error = DeserrJsonError<InvalidIndexOffset>)]
    pub offset: usize,
    /// Maximum number of fields to return. Defaults to 20.
    #[deserr(default = PAGINATION_DEFAULT_LIMIT, error = DeserrJsonError<InvalidIndexLimit>)]
    pub limit: usize,
    /// Optional filter to restrict which fields are returned (e.g. by attribute patterns or by capability: displayed, searchable, sortable, filterable, etc.).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilter>)]
    pub filter: Option<ListFieldsFilter>,
}

impl ListFields {
    fn apply_filter(&self, field: &Field) -> bool {
        if let Some(filter) = &self.filter {
            if let Some(patterns) = &filter.attribute_patterns {
                if matches!(
                    patterns.match_str(field.name),
                    PatternMatch::NoMatch | PatternMatch::Parent
                ) {
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

/// Filter to restrict which index fields are returned.
#[derive(Deserr, Debug, Clone, ToSchema)]
#[deserr(error = DeserrJsonError<InvalidIndexFieldsFilter>, rename_all = camelCase, deny_unknown_fields)]
#[schema(description = "Filter fields by attribute name patterns or by capability (displayed, searchable, sortable, etc.). All criteria are ANDed.")]
pub struct ListFieldsFilter {
    /// Only include fields whose names match these patterns (e.g. `["title", "desc*"]`).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterAttributePatterns>)]
    pub attribute_patterns: Option<AttributePatterns>,
    /// Only include fields that are displayed (true) or not displayed (false) in search results.
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterDisplayed>)]
    pub displayed: Option<bool>,
    /// Only include fields that are searchable (true) or not searchable (false).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterSearchable>)]
    pub searchable: Option<bool>,
    /// Only include fields that are sortable (true) or not sortable (false).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterSortable>)]
    pub sortable: Option<bool>,
    /// Only include fields that are used as distinct attribute (true) or not (false).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterDistinct>)]
    pub distinct: Option<bool>,
    /// Only include fields that have a custom ranking rule (asc/desc) (true) or not (false).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterRankingRule>)]
    pub ranking_rule: Option<bool>,
    /// Only include fields that are filterable (true) or not filterable (false).
    #[deserr(default, error = DeserrJsonError<InvalidIndexFieldsFilterFilterable>)]
    pub filterable: Option<bool>,
}

#[utoipa::path(
    post,
    path = "/{indexUid}/fields",
    tag = "Indexes",
    summary = "List index fields",
    description = "Returns a paginated list of fields in the index with their metadata: whether they are displayed, searchable, sortable, filterable, distinct, have a custom ranking rule (asc/desc), and for filterable fields the sort order for facet values.",
    security(("Bearer" = ["fields.post", "fields.*", "*"])),
    params((
        "indexUid" = String,
        Path,
        description = "Unique identifier of the index whose fields to list",
        example = "movies",
        nullable = false
    )),
    request_body(
        content = ListFields
    ),
    responses(
        (status = 200, body = PaginationView<Field<'static>>, content_type = "application/json", example = json!({
            "results": [
                {
                    "name": "title",
                    "displayed": { "enabled": true },
                    "searchable": { "enabled": true },
                    "sortable": { "enabled": true },
                    "distinct": { "enabled": false },
                    "rankingRule": { "enabled": false, "order": [] },
                    "filterable": { "enabled": false, "sortBy": "count", "facetSearch": false, "equality": false, "comparison": false },
                    "localized": { "locales": [] }
                },
                {
                    "name": "genre",
                    "displayed": { "enabled": true },
                    "searchable": { "enabled": false },
                    "sortable": { "enabled": false },
                    "distinct": { "enabled": false },
                    "rankingRule": { "enabled": false, "order": [] },
                    "filterable": { "enabled": true, "sortBy": "alpha", "facetSearch": true, "equality": true, "comparison": false },
                    "localized": { "locales": [] }
                }
            ],
            "offset": 0,
            "limit": 20,
            "total": 2
        })),
        (status = 401, description = "Missing or invalid authorization header", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "Index not found", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Index `movies` not found.",
                "code": "index_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#index_not_found"
            }
        )),
    ),
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
            let field = Field::new(
                name,
                metadata,
                builder.filterable_attributes(),
                builder.localized_attributes_rules(),
            );

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
