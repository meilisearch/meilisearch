use http_client::policy::IpPolicy;
use meilisearch_types::dynamic_search_rules::{
    Condition, DynamicSearchRule, DynamicSearchRules, RuleAction, RuleUid,
};
use meilisearch_types::heed::{self, EnvFlags, RoTxn};
use meilisearch_types::milli::documents::documents_batch_reader_from_objects;
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::update::{IndexDocumentsConfig, IndexerConfig};
use meilisearch_types::milli::{self, CreateOrOpen, FieldsIdsMap, FilterableAttributesRule};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env::VarError;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::utils::clamp_to_page_size;
use crate::{IndexBudget, IndexSchedulerOptions, Result};

const DSR_DIR_NAME: &str = "search_rules";

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
    fn from(value: DbDynamicSearchRule) -> Self {
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
                DbActivationCondition::TimeWindow { start, end } => Condition::Time { start, end },
            })
            .collect();

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
    fn from(value: DynamicSearchRule) -> Self {
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
                Condition::Time { start, end } => {
                    Some(DbActivationCondition::TimeWindow { start, end })
                }

                _ => None,
            })
            .collect();

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
    },
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    pub(crate) index: milli::Index,
    indexer_config: Arc<IndexerConfig>,
    ip_policy: IpPolicy,
}

fn dsr_milli_error(e: impl Into<milli::Error>) -> crate::error::Error {
    crate::error::Error::from_milli(e.into(), Some("$search_rules".to_string()))
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
                FilterableAttributesRule::Field("conditions.start".to_string()),
                FilterableAttributesRule::Field("conditions.end".to_string()),
                FilterableAttributesRule::Field("actions.selector.indexUid".to_string()),
            ]);

            settings.set_sortable_fields(HashSet::from_iter(["precedence".to_string()]));

            settings.set_authorize_typos(true);
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

    pub fn put(&self, value: DynamicSearchRules) -> Result<()> {
        self.ingest_rules(value.into_values())
    }

    pub fn put_one(&self, rule: &DynamicSearchRule) -> Result<()> {
        self.ingest_rules([rule.clone()])
    }

    pub fn delete_one(&self, uid: &RuleUid) -> Result<bool> {
        let mut wtxn = self.index.write_txn()?;
        let rtxn = self.index.read_txn()?;
        let external_document_ids = self.index.external_documents_ids();

        let Some(ext_id) =
            external_document_ids.get(&wtxn, uid.as_str()).map_err(dsr_milli_error)?
        else {
            return Ok(false);
        };

        let db_fields_ids_map = self.index.fields_ids_map(&wtxn)?;
        let mut new_fields_ids_map = db_fields_ids_map.clone();
        let primary_key =
            self.index.primary_key(&rtxn)?.expect("a rule to always have a defined primary key");
        let primary_key =
            milli::documents::PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
                .map_err(dsr_milli_error)?;

        let mut to_delete = roaring::RoaringBitmap::new();
        to_delete.insert(ext_id);

        let mut indexer = milli::update::new::indexer::DocumentDeletion::new();
        indexer.delete_documents_by_docids(to_delete);
        let indexer_alloc = bumpalo::Bump::new();
        let document_changes = indexer.into_changes(&indexer_alloc, primary_key);

        let progress = Progress::default();
        let embedder_stats = milli::progress::EmbedderStats::default();

        milli::update::new::indexer::index(
            &mut wtxn,
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

        wtxn.commit()?;

        Ok(true)
    }

    pub fn get(&self) -> Result<DynamicSearchRules> {
        let rtxn = self.index.read_txn()?;
        let docids = self.index.documents_ids(&rtxn).map_err(dsr_milli_error)?;
        let fields = self.index.fields_ids_map(&rtxn).map_err(dsr_milli_error)?;

        self.load_rules_from_docids(&rtxn, fields, docids)
    }

    pub fn search_for_rule_candidates(&self, query: Option<&str>) -> Result<DynamicSearchRules> {
        let rtxn = self.index.read_txn()?;
        let progress = Progress::default();
        let fields = self.index.fields_ids_map(&rtxn).map_err(dsr_milli_error)?;
        let docids_without_conditions =
            self.run_filter(&rtxn, r#"active = true AND conditions.kind NOT EXISTS"#)?;
        let docids_with_time_window =
            self.run_filter(&rtxn, r#"active = true AND conditions.kind = "timeWindow""#)?;
        let docids_with_query_scope = if let Some(query) = query {
            let mut docids_with_contains =
                self.run_filter(&rtxn, r#"active = true AND conditions.kind = "queryContains""#)?;
            let docids_not_empty_query =
                self.run_filter(&rtxn, r#"active = true AND conditions.kind = "queryIsNotEmpty""#)?;

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
            self.run_filter(&rtxn, r#"active = true AND conditions.kind = "queryIsEmpty""#)?
        };

        let universe =
            docids_without_conditions | docids_with_time_window | docids_with_query_scope;

        self.load_rules_from_docids(&rtxn, fields, universe)
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
    ) -> Result<DynamicSearchRules> {
        let docs = self.index.iter_documents(rtxn, docids).map_err(dsr_milli_error)?;
        let mut rules = DynamicSearchRules::new();

        for doc in docs {
            let (_id, obkv) = doc.map_err(dsr_milli_error)?;
            let obj = milli::all_obkv_to_json(obkv, &fields).map_err(dsr_milli_error)?;
            let db_rule: DbDynamicSearchRule = serde_json::from_value(obj.into()).map_err(|e| {
                dsr_milli_error(milli::Error::UserError(milli::UserError::SerdeJson(e)))
            })?;

            rules.insert(db_rule.uid.clone(), db_rule.into());
        }

        Ok(rules)
    }

    fn ingest_rules(&self, rules: impl IntoIterator<Item = DynamicSearchRule>) -> Result<()> {
        let mut wtxn = self.index.write_txn()?;

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
            &mut wtxn,
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

        wtxn.commit()?;

        Ok(())
    }
}
