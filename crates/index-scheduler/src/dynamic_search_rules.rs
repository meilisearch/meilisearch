use std::sync::{Arc, RwLock};

use meilisearch_types::dynamic_search_rules::{DynamicSearchRule, DynamicSearchRules, RuleUid};
use meilisearch_types::heed;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use meilisearch_types::index_uid::IndexUid;

use crate::{IndexSchedulerOptions, Result};

const NUMBER_OF_DATABASES: u32 = 1;
const DSR_DIR_NAME: &str = "search_rules";
const DSR_DB_SIZE: usize = 1 * 1_024 * 1_024 * 1_024; // 1 GB

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
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
        self.persisted.clear(&mut wtxn)?;
        for (uid, rule) in &value {
            self.persisted.put(&mut wtxn, uid, rule)?;
        }
        wtxn.commit()?;

        let mut runtime = self.runtime.write().unwrap();
        *runtime = Arc::new(value);
        Ok(())
    }

    pub fn get(&self) -> Arc<DynamicSearchRules> {
        self.runtime.read().unwrap().clone()
    }

    pub fn put_one(&self, wtxn: &mut RwTxn, rule: &DynamicSearchRule) -> Result<()> {
        self.persisted.put(wtxn, &rule.uid, rule)?;

        let mut lock = self.runtime.write().unwrap();
        Arc::make_mut(&mut lock).insert(rule.uid.clone(), rule.clone());
        Ok(())
    }

    pub fn delete_one(&self, wtxn: &mut RwTxn, uid: &RuleUid) -> Result<bool> {
        let deleted = self.persisted.delete(wtxn, uid)?;

        if deleted {
            let mut lock = self.runtime.write().unwrap();
            Arc::make_mut(&mut lock).remove(uid);
        }
        Ok(deleted)
    }
}
