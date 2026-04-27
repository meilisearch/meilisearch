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
use meilisearch_types::milli::{self, CreateOrOpen, FieldsIdsMap, FilterableAttributesRule};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env::VarError;
use std::sync::Arc;
use time::OffsetDateTime;

const DSR_DIR_NAME: &str = "search_rules";
const GLOBAL_INDEX_UID_SENTINEL: &str = "__global__";

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

                _ => None,
            })
            .collect();

        for action in &mut value.actions {
            if action.selector.index_uid.as_ref().is_none() {
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
}

fn dsr_milli_error(e: impl Into<milli::Error>) -> crate::error::Error {
    crate::error::Error::from_milli(e.into(), Some("$search_rules".to_string()))
}

struct ActivationCtx<'a> {
    index_uid: &'a str,
    query: Option<&'a str>,
    now: OffsetDateTime,
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    pub(crate) index: milli::Index,
    indexer_config: Arc<IndexerConfig>,
    ip_policy: IpPolicy,
}

impl DynamicSearchRulesStore {
    pub fn new(options: &IndexSchedulerOptions, budget: &IndexBudget) -> Result<Self> {
        let dsr_db_path = options.indexes_path.join(DSR_DIR_NAME);

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
                "actions.selector.indexUid".to_string(),
            ]);

            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field("active".to_string()),
                FilterableAttributesRule::Field("conditions.kind".to_string()),
                FilterableAttributesRule::Field("conditions.startTimestamp".to_string()),
                FilterableAttributesRule::Field("conditions.endTimestamp".to_string()),
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

        self.load_rules_from_docids(rtxn, fields, docids, None)
    }

    pub fn search_for_rule_candidates(
        &self,
        query: Option<&str>,
        index_uid: &str,
    ) -> Result<DynamicSearchRules> {
        let rtxn = self.index.read_txn()?;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let progress = Progress::default();
        let fields = self.index.fields_ids_map(&rtxn).map_err(dsr_milli_error)?;
        let base_filter = format!(
            r#"active = true AND (actions.selector.indexUid = "{index_uid}" OR actions.selector.indexUid = "{GLOBAL_INDEX_UID_SENTINEL}")"#,
        );
        let docids_without_conditions =
            self.run_filter(&rtxn, &format!(r#"{base_filter} AND conditions.kind NOT EXISTS"#))?;
        let docids_with_time_window =
            self.run_filter(
                &rtxn,
                &format!(
                    r#"{base_filter} AND conditions.kind = "timeWindow" AND (conditions.startTimestamp <= {now} OR conditions.startTimestamp NOT EXISTS) AND (conditions.endTimestamp >= {now} OR conditions.endTimestamp NOT EXISTS)"#,
                    base_filter = base_filter,
                    now = now,
                ),
            )?;

        let query = query.filter(|q| !q.trim().is_empty());
        let docids_with_query_scope = if let Some(query) = query {
            let mut docids_with_contains = self.run_filter(
                &rtxn,
                &format!(r#"{base_filter} AND conditions.kind = "queryContains""#),
            )?;
            let docids_not_empty_query = self.run_filter(
                &rtxn,
                &format!(r#"{base_filter} AND conditions.kind = "queryIsNotEmpty""#),
            )?;

            if !docids_with_contains.is_empty() {
                if let Some(contains_fid) = fields.id("conditions.contains") {
                    let mut ctx =
                        milli::SearchContext::new(&self.index, &rtxn).map_err(dsr_milli_error)?;
                    ctx.attributes_to_search_on(&["conditions.contains".to_string()])
                        .map_err(dsr_milli_error)?;

                    docids_with_contains = ctx
                        .resolve_query_terms_within_field_id(
                            query,
                            contains_fid,
                            &docids_with_contains,
                            None,
                            None,
                            &progress,
                        )
                        .map_err(dsr_milli_error)?;
                }
            }

            docids_not_empty_query | docids_with_contains
        } else {
            self.run_filter(
                &rtxn,
                &format!(r#"{base_filter} AND conditions.kind = "queryIsEmpty""#),
            )?
        };

        let universe =
            docids_without_conditions | docids_with_time_window | docids_with_query_scope;

        self.load_rules_from_docids(
            &rtxn,
            fields,
            universe,
            Some(ActivationCtx { index_uid, query, now: OffsetDateTime::now_utc() }),
        )
    }

    fn run_filter(&self, rtxn: &RoTxn<'_>, filter: &str) -> Result<RoaringBitmap> {
        let filter = milli::Filter::from_str(filter)
            .expect("filter is manually created and always valid")
            .unwrap();

        let filter = crate::filter::filters_into_index_filters_unchecked(vec![Some(filter)])?
            .pop()
            .expect("we always expect one filter");

        let set = milli::filtered_universe(&self.index, rtxn, &filter, &Progress::default())
            .map_err(dsr_milli_error)?;

        Ok(set)
    }

    fn load_rules_from_docids(
        &self,
        rtxn: &RoTxn<'_>,
        fields: FieldsIdsMap,
        docids: RoaringBitmap,
        activation_ctx: Option<ActivationCtx<'_>>,
    ) -> Result<DynamicSearchRules> {
        let docs = self.index.iter_documents(rtxn, docids).map_err(dsr_milli_error)?;
        let mut rules = DynamicSearchRules::new();

        for doc in docs {
            let (_id, obkv) = doc.map_err(dsr_milli_error)?;
            let obj = milli::all_obkv_to_json(obkv, &fields).map_err(dsr_milli_error)?;
            let db_rule: DbDynamicSearchRule = serde_json::from_value(obj.into()).map_err(|e| {
                dsr_milli_error(milli::Error::UserError(milli::UserError::SerdeJson(e)))
            })?;

            let uid = db_rule.uid.clone();
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
                        let normalized_contains = milli::normalize_facet(contains);
                        normalized_query.as_ref().is_some_and(|q| q.contains(&normalized_contains))
                    }
                    Condition::Time { start: None, end: None } => true,
                    &Condition::Time { start: Some(start), end: None } => start <= ctx.now,
                    &Condition::Time { start: None, end: Some(end) } => end >= ctx.now,
                    &Condition::Time { start: Some(start), end: Some(end) } => {
                        start <= ctx.now && end >= ctx.now
                    }
                });

                if !activated {
                    continue;
                }
            }

            rules.insert(uid, rule);
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
