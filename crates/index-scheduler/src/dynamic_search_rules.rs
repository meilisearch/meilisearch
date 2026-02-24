use std::sync::{Arc, RwLock};

use meilisearch_types::dynamic_search_rules::{DynamicSearchRule, DynamicSearchRules};
use meilisearch_types::heed;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};

use crate::Result;

const NUMBER_OF_DATABASES: u32 = 1;

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    pub(crate) persisted: Database<Str, SerdeJson<DynamicSearchRule>>,
    runtime: Arc<RwLock<Arc<DynamicSearchRules>>>,
}

impl DynamicSearchRulesStore {
    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn new(env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> Result<Self> {
        let persisted = env.create_database(wtxn, Some(db_name::DYNAMIC_SEARCH_RULES))?;
        let rules: DynamicSearchRules = persisted
            .iter(wtxn)?
            .map(|entry| {
                entry.map(|(key, rule): (&str, DynamicSearchRule)| (key.to_string(), rule))
            })
            .collect::<Result<DynamicSearchRules, heed::Error>>()?;

        Ok(Self { persisted, runtime: Arc::new(RwLock::new(Arc::new(rules))) })
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

    pub fn delete_one(&self, wtxn: &mut RwTxn, uid: &str) -> Result<bool> {
        let deleted = self.persisted.delete(wtxn, uid)?;

        if deleted {
            let mut lock = self.runtime.write().unwrap();
            Arc::make_mut(&mut lock).remove(uid);
        }
        Ok(deleted)
    }
}
