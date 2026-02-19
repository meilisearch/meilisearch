use std::sync::{Arc, RwLock};

use meilisearch_types::dynamic_search_rules::{DynamicSearchRule, DynamicSearchRules};
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RoTxn, RwTxn, WithoutTls};

use crate::Result;

const NUMBER_OF_DATABASES: u32 = 1;

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    pub(crate) persisted: Database<Str, SerdeJson<DynamicSearchRule>>,
    runtime: Arc<RwLock<DynamicSearchRules>>,
}

impl DynamicSearchRulesStore {
    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn new(env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> Result<Self> {
        let persisted = env.create_database(wtxn, Some(db_name::DYNAMIC_SEARCH_RULES))?;
        let rules: DynamicSearchRules = persisted
            .iter(wtxn)?
            .filter_map(|entry| entry.ok())
            .map(|(key, rule): (&str, DynamicSearchRule)| (key.to_string(), rule))
            .collect();

        Ok(Self { persisted, runtime: Arc::new(RwLock::new(rules)) })
    }

    pub fn put(&self, mut wtxn: RwTxn, value: DynamicSearchRules) -> Result<()> {
        self.persisted.clear(&mut wtxn)?;
        for (uid, rule) in &value {
            self.persisted.put(&mut wtxn, uid, rule)?;
        }
        wtxn.commit()?;

        let mut runtime = self.runtime.write().unwrap();
        *runtime = value;
        Ok(())
    }

    pub fn get(&self) -> DynamicSearchRules {
        DynamicSearchRules::clone(&*self.runtime.read().unwrap())
    }

    pub fn get_one(&self, rtxn: &RoTxn, uid: &str) -> Result<Option<DynamicSearchRule>> {
        Ok(self.persisted.get(rtxn, uid)?)
    }

    pub fn put_one(&self, wtxn: &mut RwTxn, uid: &str, rule: &DynamicSearchRule) -> Result<()> {
        self.persisted.put(wtxn, uid, rule)?;

        let mut runtime = self.runtime.write().unwrap();
        runtime.insert(uid.to_string(), rule.clone());
        Ok(())
    }

    pub fn delete_one(&self, wtxn: &mut RwTxn, uid: &str) -> Result<bool> {
        let deleted = self.persisted.delete(wtxn, uid)?;

        if deleted {
            let mut runtime = self.runtime.write().unwrap();
            runtime.remove(uid);
        }
        Ok(deleted)
    }
}
