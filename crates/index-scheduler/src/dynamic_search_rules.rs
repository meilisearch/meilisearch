use std::collections::HashSet;
use std::env::VarError;
use std::sync::Arc;
use std::u64;

use meilisearch_types::dynamic_search_rules::{
    Condition, DynamicSearchRule, DynamicSearchRules, RuleAction, RuleUid,
};
use meilisearch_types::heed::RwTxn;
use meilisearch_types::heed::{self, EnvFlags};
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

    #[serde(flatten)]
    query_is_empty_rule: DbActivationQueryIsEmpty,

    #[serde(flatten)]
    query_contains_rule: DbActivationQueryContains,

    #[serde(flatten)]
    time_window_rule: DbActivationTimeWindow,

    pub actions: Vec<RuleAction>,
}

impl From<DbDynamicSearchRule> for DynamicSearchRule {
    fn from(value: DbDynamicSearchRule) -> Self {
        let mut conditions = Vec::new();

        if value.query_is_empty_rule.query_is_empty_enabled {
            conditions.push(Condition::Query { is_empty: Some(true), contains: None })
        }

        if value.query_contains_rule.query_contains_enabled {
            conditions.push(Condition::Query {
                is_empty: None,
                contains: Some(value.query_contains_rule.query_contains),
            });
        }

        if value.time_window_rule.time_window_enabled {
            let start = if value.time_window_rule.time_window_has_start {
                Some(value.time_window_rule.time_window_start)
            } else {
                None
            };

            let end = if value.time_window_rule.time_window_has_end {
                Some(value.time_window_rule.time_window_end)
            } else {
                None
            };

            conditions.push(Condition::Time { start, end });
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
    fn from(value: DynamicSearchRule) -> Self {
        let mut query_is_empty_rule = DbActivationQueryIsEmpty::default();
        let mut query_contains_rule = DbActivationQueryContains::default();
        let mut time_window_rule = DbActivationTimeWindow::default();

        for condition in value.conditions {
            match condition {
                Condition::Query { is_empty, contains } => {
                    query_is_empty_rule.query_is_empty_enabled = is_empty.is_some_and(|b| b);

                    if let Some(string) = contains {
                        query_contains_rule.query_contains_enabled = true;
                        query_contains_rule.query_contains = string;
                    }
                }

                Condition::Time { start, end } => {
                    time_window_rule.time_window_enabled = true;

                    if let Some(start) = start {
                        time_window_rule.time_window_has_start = true;
                        time_window_rule.time_window_start = start;
                    }

                    if let Some(end) = end {
                        time_window_rule.time_window_has_end = true;
                        time_window_rule.time_window_end = end;
                    }
                }
            }
        }

        Self {
            uid: value.uid,
            description: value.description,
            precedence: value.priority.unwrap_or(u64::MAX),
            active: value.active,
            query_is_empty_rule,
            query_contains_rule,
            time_window_rule,
            actions: value.actions,
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct DbActivationQueryIsEmpty {
    query_is_empty_enabled: bool,
}

#[derive(Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct DbActivationQueryContains {
    query_contains_enabled: bool,
    query_contains: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DbActivationTimeWindow {
    time_window_enabled: bool,
    time_window_has_start: bool,
    time_window_has_end: bool,

    #[serde(with = "time::serde::rfc3339")]
    time_window_start: OffsetDateTime,

    #[serde(with = "time::serde::rfc3339")]
    time_window_end: OffsetDateTime,
}

impl Default for DbActivationTimeWindow {
    fn default() -> Self {
        Self {
            time_window_enabled: false,
            time_window_has_start: false,
            time_window_has_end: false,
            time_window_start: OffsetDateTime::UNIX_EPOCH,
            time_window_end: OffsetDateTime::UNIX_EPOCH,
        }
    }
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
            settings.set_searchable_fields(vec!["description".to_string()]);

            settings.set_filterable_fields(vec![
                FilterableAttributesRule::Field("active".to_string()),
                FilterableAttributesRule::Field("queryIsEmptyEnabled".to_string()),
                FilterableAttributesRule::Field("queryContainsEnabled".to_string()),
                FilterableAttributesRule::Field("timeWindowEnabled".to_string()),
                FilterableAttributesRule::Field("timeWindowHasStart".to_string()),
                FilterableAttributesRule::Field("timeWindowHasEnd".to_string()),
                FilterableAttributesRule::Field("timeWindowStart".to_string()),
                FilterableAttributesRule::Field("timeWindowEnd".to_string()),
            ]);

            settings.set_sortable_fields(HashSet::from_iter(["precedence".to_string()]));

            settings.set_authorize_typos(true);
            settings.set_facet_search(true);

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
