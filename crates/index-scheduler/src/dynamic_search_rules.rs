use std::collections::HashSet;
use std::env::VarError;
use std::sync::Arc;
use std::u64;

use meilisearch_types::dynamic_search_rules::{
    Condition, DynamicSearchRule, DynamicSearchRules, RuleAction, RuleUid,
};
use meilisearch_types::heed::RwTxn;
use meilisearch_types::heed::{self, EnvFlags};
use meilisearch_types::milli::progress::Progress;
use meilisearch_types::milli::{self, CreateOrOpen, FilterableAttributesRule};
use serde::{Deserialize, Serialize};
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
}

impl DynamicSearchRulesStore {
    pub fn new(options: &IndexSchedulerOptions, budget: &IndexBudget) -> Result<Self> {
        let dsr_db_path = options.indexes_path.join(DSR_DIR_NAME);

        std::fs::create_dir_all(&dsr_db_path)?;

        let mut newly_created = false;
        let create_or_open = if dsr_db_path.join("data.mdb").exists() {
            newly_created = true;
            CreateOrOpen::Open
        } else {
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

        let index = milli::Index::new(env_options, dsr_db_path, create_or_open)
            .map_err(|e| crate::error::Error::from_milli(e, Some("$search_rules".to_string())))?;

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

            settings.execute(
                &|| false,
                &Progress::default(),
                &options.ip_policy,
                Arc::default(),
            ).map_err(|e| crate::error::Error::from_milli(e, Some("$search_rules".to_string())))?;

            wtxn.commit()?;
        }

        // TODO - should I check the index version? I presume yes
        Ok(Self { index })
    }

    pub fn put(&self, mut wtxn: RwTxn, value: DynamicSearchRules) -> Result<()> {
        // self.persisted.clear(&mut wtxn)?;
        // for (uid, rule) in &value {
        //     self.persisted.put(&mut wtxn, uid, rule)?;
        // }
        // wtxn.commit()?;

        // let mut runtime = self.runtime.write().unwrap();
        // *runtime = Arc::new(value);
        Ok(())
    }

    pub fn get(&self) -> Arc<DynamicSearchRules> {
        todo!()
        // self.runtime.read().unwrap().clone()
    }

    pub fn put_one(&self, wtxn: &mut RwTxn, rule: &DynamicSearchRule) -> Result<()> {
        // self.persisted.put(wtxn, &rule.uid, rule)?;

        // let mut lock = self.runtime.write().unwrap();
        // Arc::make_mut(&mut lock).insert(rule.uid.clone(), rule.clone());
        Ok(())
    }

    pub fn delete_one(&self, wtxn: &mut RwTxn, uid: &RuleUid) -> Result<bool> {
        // let deleted = self.persisted.delete(wtxn, uid)?;

        // if deleted {
        //     let mut lock = self.runtime.write().unwrap();
        //     Arc::make_mut(&mut lock).remove(uid);
        // }
        // Ok(deleted)
        todo!()
    }

    pub fn list(&self) -> Result<Vec<DynamicSearchRule>> {
        todo!()
    }
}
