use std::sync::{Arc, RwLock};
use std::u64;

use meilisearch_types::dynamic_search_rules::{
    Condition, DynamicSearchRule, DynamicSearchRules, RuleAction, RuleUid,
};
use meilisearch_types::heed;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{IndexSchedulerOptions, Result};

const NUMBER_OF_DATABASES: u32 = 1;
const DSR_DIR_NAME: &str = "search_rules";
const DSR_DB_SIZE: usize = 1 * 1_024 * 1_024 * 1_024; // 1 GB

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

#[derive(Serialize, Deserialize)]
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
        let query_is_empty_rule = DbActivationQueryIsEmpty::default();
        let query_contains_rule = DbActivationQueryContains::default();
        let time_window_rule = DbActivationTimeWindow::default();
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
struct DbActivationQueryIsEmpty {
    query_is_empty_enabled: bool,
}

#[derive(Serialize, Deserialize, Default)]
struct DbActivationQueryContains {
    query_contains_enabled: bool,
    query_contains: String,
}

#[derive(Serialize, Deserialize)]
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
    pub(crate) db: Database<Str, SerdeJson<DynamicSearchRule>>,
}

impl DynamicSearchRulesStore {
    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn new(options: &IndexSchedulerOptions, _from_db_version: (u32, u32, u32)) -> Result<Self> {
        let dsr_db_path = options.indexes_path.join(DSR_DIR_NAME);
        std::fs::create_dir_all(&dsr_db_path)?;

        let env = unsafe {
            let env_options = heed::EnvOpenOptions::new();
            let mut env_options = env_options.read_txn_without_tls();

            env_options.max_dbs(Self::nb_db()).map_size(DSR_DB_SIZE).open(dsr_db_path)
        }?;

        let db = {
            let mut wtxn = env.write_txn()?;
            env.create_database(&mut wtxn, Some(db_name::DYNAMIC_SEARCH_RULES))
        }?;

        Ok(Self { db })
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
