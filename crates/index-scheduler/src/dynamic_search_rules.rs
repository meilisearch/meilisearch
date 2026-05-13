use crate::utils::clamp_to_page_size;
use crate::{IndexBudget, IndexSchedulerOptions, Result};
use http_client::policy::IpPolicy;
use meilisearch_types::dynamic_search_rules::{
    Condition, DynamicSearchRule, DynamicSearchRules, RuleAction, RuleUid,
};
use meilisearch_types::heed::{self, EnvFlags, RoTxn, RwTxn, WithoutTls};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli::documents::documents_batch_reader_from_objects;
use meilisearch_types::milli::index::PrefixSearch;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::update::{IndexDocumentsConfig, IndexerConfig};
use meilisearch_types::milli::{
    self, parse_index_filter_unchecked, AttributePatterns, CreateOrOpen, FieldsIdsMap,
    FilterableAttributesRule, IndexFilter, IndexFilterCondition, PatternMatch,
};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env::VarError;
use std::sync::Arc;
use time::OffsetDateTime;

const DYNAMIC_SEARCH_RULES_DIR_NAME: &str = "search_rules";
const DYNAMIC_SEARCH_RULES_MAX_LIMIT: usize = 10_000;
const GLOBAL_INDEX_UID_SENTINEL: &str = "__meilisearch_dsr_global__";

pub fn is_reserved_dynamic_search_rule_index_uid(index_uid: &str) -> bool {
    index_uid == GLOBAL_INDEX_UID_SENTINEL
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DbDynamicSearchRule {
    uid: RuleUid,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    precedence: u64,
    active: bool,

    conditions: Vec<DbActivationCondition>,

    actions: Vec<RuleAction>,
}

impl From<DbDynamicSearchRule> for DynamicSearchRule {
    fn from(mut value: DbDynamicSearchRule) -> Self {
        let conditions = value
            .conditions
            .into_iter()
            .map(|cond| match cond {
                DbActivationCondition::QueryIsEmpty => {
                    Condition::Query { is_empty: Some(true), contains: None }
                }
                DbActivationCondition::QueryIsNotEmpty => {
                    Condition::Query { is_empty: Some(false), contains: None }
                }
                DbActivationCondition::QueryContains { contains } => {
                    Condition::Query { is_empty: None, contains: Some(contains) }
                }
                DbActivationCondition::TimeWindow {
                    start,
                    end,
                    start_timestamp: _,
                    end_timestamp: _,
                } => Condition::Time { start, end },
                DbActivationCondition::Filter { filter, filter_facets: _ } => {
                    Condition::Filter { filter }
                }
            })
            .collect();

        for action in &mut value.actions {
            if action
                .selector
                .index_uid
                .as_ref()
                .is_some_and(|uid| uid.as_str() == GLOBAL_INDEX_UID_SENTINEL)
            {
                action.selector.index_uid = None;
            }
        }

        Self {
            uid: value.uid,
            description: value.description,
            priority: if value.precedence == u64::MAX { None } else { Some(value.precedence) },
            active: value.active,
            conditions,
            actions: value.actions,
        }
    }
}

impl From<DynamicSearchRule> for DbDynamicSearchRule {
    fn from(mut value: DynamicSearchRule) -> Self {
        let conditions = value
            .conditions
            .into_iter()
            .filter_map(|cond| match cond {
                Condition::Query { is_empty: Some(is_empty), contains: None } => {
                    Some(if is_empty {
                        DbActivationCondition::QueryIsEmpty
                    } else {
                        DbActivationCondition::QueryIsNotEmpty
                    })
                }

                Condition::Query { is_empty: None, contains: Some(contains) } => {
                    Some(DbActivationCondition::QueryContains { contains })
                }
                Condition::Time { start, end } => Some(DbActivationCondition::TimeWindow {
                    start,
                    end,
                    start_timestamp: start.map(OffsetDateTime::unix_timestamp),
                    end_timestamp: end.map(OffsetDateTime::unix_timestamp),
                }),
                Condition::Filter { filter } => {
                    let filter_facets = FilterSet::from(&filter).collect_facets();
                    Some(DbActivationCondition::Filter { filter, filter_facets })
                }

                _ => None,
            })
            .collect();

        for action in &mut value.actions {
            if action.selector.index_uid.is_none() {
                action.selector.index_uid =
                    Some(IndexUid::new_unchecked(GLOBAL_INDEX_UID_SENTINEL));
            }
        }

        Self {
            uid: value.uid,
            description: value.description,
            precedence: value.priority.unwrap_or(u64::MAX),
            active: value.active,
            actions: value.actions,
            conditions,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum DbActivationCondition {
    QueryIsEmpty,
    QueryIsNotEmpty,

    #[serde(rename_all = "camelCase")]
    QueryContains {
        contains: String,
    },

    #[serde(rename_all = "camelCase")]
    TimeWindow {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "time::serde::rfc3339::option"
        )]
        start: Option<OffsetDateTime>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "time::serde::rfc3339::option"
        )]
        end: Option<OffsetDateTime>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_timestamp: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_timestamp: Option<i64>,
    },

    #[serde(rename_all = "camelCase")]
    Filter {
        #[serde(
            serialize_with = "meilisearch_types::dynamic_search_rules::serialize_index_filter",
            deserialize_with = "meilisearch_types::dynamic_search_rules::deserialize_index_filter"
        )]
        filter: IndexFilter<'static>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        filter_facets: Vec<String>,
    },
}

fn dsr_milli_error(e: impl Into<milli::Error>) -> crate::error::Error {
    crate::error::Error::from_milli(e.into(), Some("$search_rules".to_string()))
}

fn quote_filter_value(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for c in value.chars() {
        match c {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

struct ActivationCtx<'a> {
    index_uid: &'a str,
    query: Option<&'a str>,
    filter_set: FilterSet,
    now: OffsetDateTime,
}

pub struct DynamicSearchRulePaginationView {
    pub results: Vec<DynamicSearchRule>,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
}

fn matches_attribute_patterns(
    rule: &DynamicSearchRule,
    attribute_patterns: Option<&AttributePatterns>,
) -> bool {
    attribute_patterns.is_none_or(|patterns| {
        !matches!(patterns.match_str(&rule.uid), PatternMatch::NoMatch | PatternMatch::Parent)
    })
}

fn paginate_rules(
    rules: impl IntoIterator<Item = DynamicSearchRule>,
    active: Option<bool>,
    attribute_patterns: Option<&AttributePatterns>,
    offset: usize,
    limit: usize,
) -> DynamicSearchRulePaginationView {
    let mut total = 0;
    let mut results = Vec::with_capacity(limit);

    for rule in rules {
        if active.is_some_and(|active| active != rule.active)
            || !matches_attribute_patterns(&rule, attribute_patterns)
        {
            continue;
        }

        if total >= offset && results.len() < limit {
            results.push(rule);
        }

        total += 1;
    }

    DynamicSearchRulePaginationView { results, offset, limit, total }
}

#[derive(Copy, Clone)]
enum FilterOp {
    Equal,
    Contains,
    In,
    Or,
    And,
}

enum FilterAst {
    Ident(String),
    // should be FilterAst but in this case, arrays only hold string value
    // so no need for making the type more complicated than it needs to be
    Array(HashSet<String>),
    Binary { lhs: Box<FilterAst>, op: FilterOp, rhs: Box<FilterAst> },
    Not(Box<FilterAst>),
}

impl FilterAst {
    fn intersect(&self, other: &Self) -> bool {
        match self {
            Self::Binary { lhs, op, rhs } => match (lhs.as_ref(), op, rhs.as_ref()) {
                (Self::Ident(attr), FilterOp::Equal, Self::Ident(value)) => {
                    is_attr_value_included(attr, value, other)
                }

                (Self::Ident(attr), FilterOp::Contains, Self::Ident(value)) => {
                    is_attr_value_contained(attr, value, other)
                }

                (Self::Ident(attr), FilterOp::In, Self::Array(values)) => {
                    values.iter().any(|value| is_attr_value_included(attr, value, other))
                }

                (_, FilterOp::Or, _) => lhs.intersect(other) || rhs.intersect(other),

                (_, FilterOp::And, _) => lhs.intersect(other) && rhs.intersect(other),

                _ => false,
            },

            Self::Not(inner) => match other {
                Self::Not(other_inner) => inner.intersect(other_inner),

                Self::Binary { lhs, op: FilterOp::Or | FilterOp::And, rhs } => {
                    self.intersect(lhs) || self.intersect(rhs)
                }

                _ => false,
            },

            _ => false,
        }
    }
}

fn is_attr_value_included(attr: &str, value: &str, other: &FilterAst) -> bool {
    match other {
        FilterAst::Binary { lhs, op, rhs } => match (lhs.as_ref(), op, rhs.as_ref()) {
            (FilterAst::Ident(other_attr), FilterOp::Equal, FilterAst::Ident(other_value)) => {
                attr == other_attr && value == other_value
            }

            (FilterAst::Ident(other_attr), FilterOp::Contains, FilterAst::Ident(other_value)) => {
                attr == other_attr && other_value.contains(value)
            }

            (FilterAst::Ident(other_attr), FilterOp::In, FilterAst::Array(other_values)) => {
                attr == other_attr && other_values.contains(value)
            }

            (_, FilterOp::Or | FilterOp::And, _) => {
                is_attr_value_included(attr, value, lhs) || is_attr_value_included(attr, value, rhs)
            }

            _ => false,
        },

        _ => false,
    }
}

fn is_attr_value_contained(attr: &str, value: &str, other: &FilterAst) -> bool {
    match other {
        FilterAst::Binary { lhs, op, rhs } => match (lhs.as_ref(), op, rhs.as_ref()) {
            (FilterAst::Ident(other_attr), FilterOp::Equal, FilterAst::Ident(other_value)) => {
                attr == other_attr && other_value.contains(value)
            }

            (FilterAst::Ident(other_attr), FilterOp::Contains, FilterAst::Ident(other_value)) => {
                attr == other_attr && value == other_value
            }

            (FilterAst::Ident(other_attr), FilterOp::In, FilterAst::Array(other_values)) => {
                attr == other_attr && other_values.iter().any(|v| v.contains(value))
            }

            (_, FilterOp::Or | FilterOp::And, _) => {
                is_attr_value_contained(attr, value, lhs)
                    || is_attr_value_contained(attr, value, rhs)
            }

            _ => false,
        },

        _ => false,
    }
}

#[derive(Default)]
struct FilterSet {
    ast: Option<FilterAst>,
}

impl FilterSet {
    fn from(filter: &IndexFilter) -> Self {
        fn go(current: &IndexFilterCondition<'_>) -> Option<FilterAst> {
            match current {
                IndexFilterCondition::Not(inner) => go(inner).map(|r| FilterAst::Not(Box::new(r))),

                IndexFilterCondition::Condition { fid, op } => match op {
                    milli::Condition::Equal(token) => Some(FilterAst::Binary {
                        lhs: Box::new(FilterAst::Ident(fid.fragment().to_string())),
                        op: FilterOp::Equal,
                        rhs: Box::new(FilterAst::Ident(milli::normalize_facet(token.fragment()))),
                    }),

                    milli::Condition::NotEqual(token) => {
                        Some(FilterAst::Not(Box::new(FilterAst::Binary {
                            lhs: Box::new(FilterAst::Ident(fid.fragment().to_string())),
                            op: FilterOp::Equal,
                            rhs: Box::new(FilterAst::Ident(milli::normalize_facet(
                                token.fragment(),
                            ))),
                        })))
                    }

                    milli::Condition::Contains { word, .. } => Some(FilterAst::Binary {
                        lhs: Box::new(FilterAst::Ident(fid.fragment().to_string())),
                        op: FilterOp::Contains,
                        rhs: Box::new(FilterAst::Ident(milli::normalize_facet(word.fragment()))),
                    }),

                    // unsupported for now
                    _ => None,
                },

                IndexFilterCondition::In { fid, els } => Some(FilterAst::Binary {
                    lhs: Box::new(FilterAst::Ident(fid.fragment().to_string())),
                    op: FilterOp::In,
                    rhs: Box::new(FilterAst::Array(
                        els.iter().map(|t| milli::normalize_facet(t.fragment())).collect(),
                    )),
                }),

                // OR/AND are left-associative
                IndexFilterCondition::Or(children) | IndexFilterCondition::And(children) => {
                    let [first, second, rest @ ..] = children.as_slice() else {
                        unreachable!("or/and have at least 2 children")
                    };

                    let op = if matches!(current, IndexFilterCondition::And(_)) {
                        FilterOp::And
                    } else {
                        FilterOp::Or
                    };

                    let mut binary = FilterAst::Binary {
                        lhs: Box::new(go(first)?),
                        op,
                        rhs: Box::new(go(second)?),
                    };

                    for child in rest {
                        binary = FilterAst::Binary {
                            lhs: Box::new(binary),
                            op,
                            rhs: Box::new(go(child)?),
                        };
                    }

                    Some(binary)
                }

                // unsupported
                _ => None,
            }
        }

        Self { ast: go(&filter.condition) }
    }

    fn intersect(&self, other: &Self) -> bool {
        if let Some(this) = &self.ast {
            if let Some(other) = &other.ast {
                return this.intersect(other);
            }

            return false;
        }

        false
    }

    fn collect_facets(&self) -> Vec<String> {
        fn go(ast: &FilterAst) -> Option<HashSet<String>> {
            match ast {
                FilterAst::Binary { lhs, op, rhs } => match (lhs.as_ref(), op, rhs.as_ref()) {
                    (FilterAst::Ident(attr), FilterOp::Equal, FilterAst::Ident(value)) => {
                        Some(HashSet::from_iter([format!("{attr}\\u0000{value}")]))
                    }

                    (FilterAst::Ident(attr), FilterOp::In, FilterAst::Array(values)) => {
                        Some(values.iter().map(|v| format!("{attr}\\u0000{v}")).collect())
                    }

                    (_, FilterOp::Or, _) => {
                        let mut acc = go(lhs)?;
                        acc.extend(go(rhs)?);

                        Some(acc)
                    }

                    (_, FilterOp::And, _) => match (go(lhs), go(rhs)) {
                        (Some(mut lhs), Some(rhs)) => {
                            lhs.extend(rhs);
                            Some(lhs)
                        }

                        (Some(facets), None) | (None, Some(facets)) => Some(facets),
                        _ => None,
                    },

                    _ => None,
                },

                _ => None,
            }
        }

        self.ast.as_ref().and_then(go).unwrap_or_default().into_iter().collect()
    }
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    pub(crate) index: milli::Index,
    indexer_config: Arc<IndexerConfig>,
    ip_policy: IpPolicy,
}

impl DynamicSearchRulesStore {
    pub fn new(options: &IndexSchedulerOptions, budget: &IndexBudget) -> Result<Self> {
        let dsr_db_path = options.indexes_path.join(DYNAMIC_SEARCH_RULES_DIR_NAME);

        std::fs::create_dir_all(&dsr_db_path)?;

        let mut newly_created = false;
        let create_or_open = if dsr_db_path.join("data.mdb").exists() {
            CreateOrOpen::Open
        } else {
            newly_created = true;
            CreateOrOpen::create_without_shards()
        };

        let mut env_options = heed::EnvOpenOptions::new().read_txn_without_tls();
        env_options.map_size(clamp_to_page_size(budget.map_size));

        // You can find more details about this experimental
        // environment variable on the following GitHub discussion:
        // <https://github.com/orgs/meilisearch/discussions/806>
        let max_readers = match std::env::var("MEILI_EXPERIMENTAL_INDEX_MAX_READERS") {
            Ok(value) => value.parse::<u32>().unwrap(),
            Err(VarError::NotPresent) => 1024,
            Err(VarError::NotUnicode(value)) => panic!(
                "Invalid unicode for the `MEILI_EXPERIMENTAL_INDEX_MAX_READERS` env var: {value:?}"
            ),
        };

        env_options.max_readers(max_readers);

        if options.enable_mdb_writemap {
            unsafe {
                env_options.flags(EnvFlags::WRITE_MAP);
            }
        }

        let index =
            milli::Index::new(env_options, dsr_db_path, create_or_open).map_err(dsr_milli_error)?;

        if newly_created {
            let mut wtxn = index.write_txn()?;
            let mut settings =
                milli::update::Settings::new(&mut wtxn, &index, &options.indexer_config);

            settings.set_primary_key("uid".to_string());
            settings.set_searchable_fields(vec![
                "description".to_string(),
                "conditions.contains".to_string(),
                "conditions.filter".to_string(),
                "actions.selector.indexUid".to_string(),
            ]);

            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field("active".to_string()),
                FilterableAttributesRule::Field("conditions.kind".to_string()),
                FilterableAttributesRule::Field("conditions.startTimestamp".to_string()),
                FilterableAttributesRule::Field("conditions.endTimestamp".to_string()),
                FilterableAttributesRule::Field("conditions.filterFacets".to_string()),
                FilterableAttributesRule::Field("actions.selector.indexUid".to_string()),
            ]);

            settings.set_sortable_fields(HashSet::from_iter(["precedence".to_string()]));

            settings.set_authorize_typos(false);
            settings.set_prefix_search(PrefixSearch::Disabled);
            settings.set_facet_search(true);

            settings
                .execute(&|| false, &Progress::default(), &options.ip_policy, Arc::default())
                .map_err(dsr_milli_error)?;

            wtxn.commit()?;
        }

        // TODO - should I check the index version? I presume yes
        Ok(Self {
            index,
            indexer_config: options.indexer_config.clone(),
            ip_policy: options.ip_policy.clone(),
        })
    }

    pub fn put(&self, new_rules: DynamicSearchRules) -> Result<()> {
        let mut wtxn = self.index.write_txn()?;
        let old_rules = self.get_internal(&wtxn)?;
        let mut to_delete = vec![];

        for old_rule in old_rules.keys() {
            if !new_rules.contains_key(old_rule) {
                to_delete.push(old_rule);
            }
        }

        if !to_delete.is_empty() {
            self.delete_many(&mut wtxn, to_delete)?;
        }

        self.ingest_rules(&mut wtxn, new_rules.into_values())?;
        wtxn.commit()?;

        Ok(())
    }

    pub fn put_one(&self, rule: &DynamicSearchRule) -> Result<()> {
        let mut wtxn = self.index.write_txn()?;
        self.ingest_rules(&mut wtxn, [rule.clone()])?;
        wtxn.commit()?;

        Ok(())
    }

    pub fn delete_one(&self, uid: &RuleUid) -> Result<bool> {
        let mut wtxn = self.index.write_txn()?;
        let count = self.delete_many(&mut wtxn, [uid])?;
        wtxn.commit()?;

        Ok(count > 0)
    }

    fn delete_many<'a>(
        &self,
        wtxn: &mut RwTxn<'_>,
        uids: impl IntoIterator<Item = &'a RuleUid>,
    ) -> Result<usize> {
        let external_document_ids = self.index.external_documents_ids();
        let mut to_delete = RoaringBitmap::new();

        for uid in uids {
            if let Some(ext_id) =
                external_document_ids.get(wtxn, uid.as_str()).map_err(dsr_milli_error)?
            {
                to_delete.insert(ext_id);
            }
        }

        let deleted = to_delete.len() as usize;
        if to_delete.is_empty() {
            return Ok(0);
        }

        let rtxn = self.index.read_txn()?;
        let db_fields_ids_map = self.index.fields_ids_map(wtxn)?;
        let mut new_fields_ids_map = db_fields_ids_map.clone();
        let primary_key =
            self.index.primary_key(&rtxn)?.expect("a rule to always have a defined primary key");
        let primary_key =
            milli::documents::PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
                .map_err(dsr_milli_error)?;

        let mut indexer = milli::update::new::indexer::DocumentDeletion::new();
        indexer.delete_documents_by_docids(to_delete);
        let indexer_alloc = bumpalo::Bump::new();
        let document_changes = indexer.into_changes(&indexer_alloc, primary_key);

        let progress = Progress::default();
        let embedder_stats = milli::progress::EmbedderStats::default();

        milli::update::new::indexer::index(
            wtxn,
            &self.index,
            &self.indexer_config.thread_pool,
            self.indexer_config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            None, // document deletion never changes the primary key
            &document_changes,
            Default::default(),
            &|| false,
            &progress,
            &self.ip_policy,
            &embedder_stats,
        )
        .map_err(dsr_milli_error)?;

        Ok(deleted)
    }

    pub fn get(&self) -> Result<DynamicSearchRules> {
        let rtxn = self.index.read_txn()?;

        self.get_internal(&rtxn)
    }

    fn get_internal(&self, rtxn: &RoTxn<WithoutTls>) -> Result<DynamicSearchRules> {
        let docids = self.index.documents_ids(rtxn).map_err(dsr_milli_error)?;
        let fields = self.index.fields_ids_map(rtxn).map_err(dsr_milli_error)?;
        let rules = self.load_rules_from_docids(rtxn, &fields, docids, None)?;

        Ok(rules.into_iter().map(|rule| (rule.uid.clone(), rule)).collect())
    }

    pub fn list(
        &self,
        query: Option<&str>,
        active: Option<bool>,
        attribute_patterns: Option<&AttributePatterns>,
        offset: usize,
        limit: usize,
    ) -> Result<DynamicSearchRulePaginationView> {
        let limit = limit.min(DYNAMIC_SEARCH_RULES_MAX_LIMIT);
        let query = query.filter(|query| !query.trim().is_empty());
        let rtxn = self.index.read_txn()?;
        let fields = self.index.fields_ids_map(&rtxn).map_err(dsr_milli_error)?;

        if let Some(query) = query {
            if attribute_patterns.is_none() {
                let result = self.search_rule_docids(&rtxn, query, active, offset, limit)?;
                let total = result.candidates.len() as usize;
                let rules =
                    self.load_rules_from_docids(&rtxn, &fields, result.documents_ids, None)?;

                return Ok(DynamicSearchRulePaginationView {
                    results: rules,
                    offset,
                    limit,
                    total,
                });
            }

            // attribute patterns are applied after milli search, so pagination must be
            // applied after that filtering too.
            let result = self.search_rule_docids(&rtxn, query, active, 0, usize::MAX)?;
            let rules = self.load_rules_from_docids(&rtxn, &fields, result.documents_ids, None)?;

            return Ok(paginate_rules(rules, active, attribute_patterns, offset, limit));
        }

        let docids = self.index.documents_ids(&rtxn).map_err(dsr_milli_error)?;
        let rules = self.load_rules_from_docids(&rtxn, &fields, docids, None)?;

        Ok(paginate_rules(rules, active, attribute_patterns, offset, limit))
    }

    fn search_rule_docids(
        &self,
        rtxn: &RoTxn<'_>,
        query: &str,
        active: Option<bool>,
        offset: usize,
        limit: usize,
    ) -> Result<milli::SearchResult> {
        let progress = Progress::default();
        let mut search = milli::Search::new(rtxn, &self.index, &progress);

        search.query(query).offset(offset).limit(limit).exhaustive_number_hits(true);
        let expr = active.map(|active| format!("active = {active}"));

        if let Some(filter) = &expr {
            search.filter(parse_index_filter_unchecked(filter).map_err(dsr_milli_error)?);
        }

        search.execute().map_err(dsr_milli_error)
    }

    pub fn search_for_rule_candidates(
        &self,
        query: Option<&str>,
        filter: Option<&IndexFilter<'_>>,
        index_uid: &str,
    ) -> Result<DynamicSearchRules> {
        let query = query.filter(|query| !query.trim().is_empty());
        let filter_set = filter.map(FilterSet::from).unwrap_or_default();
        let rtxn = self.index.read_txn()?;
        let fields = self.index.fields_ids_map(&rtxn).map_err(dsr_milli_error)?;
        let now = OffsetDateTime::now_utc();
        let now_timestamp = now.unix_timestamp();
        let base_filter = format!(
            r#"active = true AND (actions.selector.indexUid = "{index_uid}" OR actions.selector.indexUid = "{GLOBAL_INDEX_UID_SENTINEL}")"#,
        );
        let docids_without_conditions =
            self.run_filter(&rtxn, &format!(r#"{base_filter} AND conditions.kind NOT EXISTS"#))?;
        let docids_with_time_window = self.run_filter(
            &rtxn,
            &format!(
                r#"{base_filter} AND conditions.kind = "timeWindow" AND (conditions.startTimestamp <= {now_timestamp} OR conditions.startTimestamp NOT EXISTS) AND (conditions.endTimestamp >= {now_timestamp} OR conditions.endTimestamp NOT EXISTS)"#,
            ),
        )?;

        let docids_with_query_scope = if query.is_some() {
            let docids_with_contains = self.run_filter(
                &rtxn,
                &format!(r#"{base_filter} AND conditions.kind = "queryContains""#),
            )?;
            let docids_not_empty_query = self.run_filter(
                &rtxn,
                &format!(r#"{base_filter} AND conditions.kind = "queryIsNotEmpty""#),
            )?;

            docids_not_empty_query | docids_with_contains
        } else {
            self.run_filter(
                &rtxn,
                &format!(r#"{base_filter} AND conditions.kind = "queryIsEmpty""#),
            )?
        };

        let docids_with_filter_scope = if filter.is_some() {
            let filter_facets = filter_set.collect_facets();
            if filter_facets.is_empty() {
                self.run_filter(&rtxn, &format!(r#"{base_filter} AND conditions.kind = "filter""#))?
            } else {
                let filter_facets = filter_facets
                    .iter()
                    .map(|facet| quote_filter_value(facet))
                    .collect::<Vec<_>>()
                    .join(", ");
                self.run_filter(
                    &rtxn,
                    &format!(
                        r#"{base_filter} AND conditions.kind = "filter" AND (conditions.filterFacets IN [{filter_facets}] OR conditions.filterFacets NOT EXISTS)"#
                    ),
                )?
            }
        } else {
            RoaringBitmap::new()
        };

        let universe = docids_without_conditions
            | docids_with_time_window
            | docids_with_query_scope
            | docids_with_filter_scope;

        let rules = self.load_rules_from_docids(
            &rtxn,
            &fields,
            universe,
            Some(ActivationCtx { index_uid, query, filter_set, now }),
        )?;

        Ok(rules.into_iter().map(|rule| (rule.uid.clone(), rule)).collect())
    }

    fn run_filter(&self, rtxn: &RoTxn<'_>, filter: &str) -> Result<RoaringBitmap> {
        let filter = Some(parse_index_filter_unchecked(filter).map_err(dsr_milli_error)?);
        let set = milli::filtered_universe(&self.index, rtxn, &filter, &Progress::default())
            .map_err(dsr_milli_error)?;

        Ok(set)
    }

    fn load_rules_from_docids(
        &self,
        rtxn: &RoTxn<'_>,
        fields: &FieldsIdsMap,
        docids: impl IntoIterator<Item = milli::DocumentId>,
        activation_ctx: Option<ActivationCtx<'_>>,
    ) -> Result<Vec<DynamicSearchRule>> {
        let docs = self.index.iter_documents(rtxn, docids).map_err(dsr_milli_error)?;
        let mut rules = Vec::new();

        for doc in docs {
            let (_id, obkv) = doc.map_err(dsr_milli_error)?;
            let obj = milli::all_obkv_to_json(obkv, fields).map_err(dsr_milli_error)?;
            let db_rule: DbDynamicSearchRule = serde_json::from_value(obj.into()).map_err(|e| {
                dsr_milli_error(milli::Error::UserError(milli::UserError::SerdeJson(e)))
            })?;

            let mut rule: DynamicSearchRule = db_rule.into();

            if let Some(ctx) = &activation_ctx {
                rule.actions.retain(|action| {
                    action
                        .selector
                        .index_uid
                        .as_ref()
                        .is_none_or(|uid| uid.as_str() == ctx.index_uid)
                });

                if rule.actions.is_empty() {
                    continue;
                }

                let normalized_query = ctx.query.map(milli::normalize_facet);

                // all conditions were already checked individually, but if we have multiple conditions,
                // we need to tell that they are all true as a whole for the rule to be activated
                let activated = rule.conditions.iter().all(|cond| match cond {
                    Condition::Query { is_empty: None, contains: None } => {
                        unreachable!("this situation is not possible")
                    }
                    Condition::Query { is_empty: Some(is_empty), .. } => {
                        ctx.query.is_none() && *is_empty || ctx.query.is_some() && !*is_empty
                    }
                    Condition::Query { contains: Some(contains), .. } => {
                        // Keep `contains` literal: DSR-index search would add token/prefix semantics
                        // that differ from substring activation.
                        let normalized_contains = milli::normalize_facet(contains);
                        normalized_query.as_ref().is_some_and(|q| q.contains(&normalized_contains))
                    }
                    Condition::Time { start: None, end: None } => true,
                    &Condition::Time { start: Some(start), end: None } => start <= ctx.now,
                    &Condition::Time { start: None, end: Some(end) } => end >= ctx.now,
                    &Condition::Time { start: Some(start), end: Some(end) } => {
                        start <= ctx.now && end >= ctx.now
                    }
                    Condition::Filter { filter } => {
                        FilterSet::from(filter).intersect(&ctx.filter_set)
                    }
                });

                if !activated {
                    continue;
                }
            }

            rules.push(rule);
        }

        Ok(rules)
    }

    fn ingest_rules<'a>(
        &'a self,
        wtxn: &mut RwTxn<'a>,
        rules: impl IntoIterator<Item = DynamicSearchRule>,
    ) -> Result<()> {
        let objects = rules
            .into_iter()
            .map(DbDynamicSearchRule::from)
            .map(|rule| serde_json::to_value(&rule).expect("serialization to always succeed"))
            .map(|rule| {
                if let serde_json::Value::Object(obj) = rule {
                    obj
                } else {
                    unreachable!("a dynamic search rule is always an object")
                }
            });

        let embedder_stats = Arc::default();
        let builder = milli::update::IndexDocuments::new(
            wtxn,
            &self.index,
            &self.indexer_config,
            IndexDocumentsConfig::default(),
            &|_step| {},
            &|| false,
            &embedder_stats,
            &self.ip_policy,
        )
        .map_err(dsr_milli_error)?;

        let reader = documents_batch_reader_from_objects(objects);

        let (builder, user_result) = builder.add_documents(reader).map_err(dsr_milli_error)?;
        user_result.map_err(dsr_milli_error)?;
        builder.execute().map_err(dsr_milli_error)?;

        Ok(())
    }
}
