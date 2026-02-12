use std::sync::{Arc, RwLock};

use meilisearch_types::dynamic_search_rules::DynamicSearchRules;
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};

use crate::Result;

const NUMBER_OF_DATABASES: u32 = 1;

mod db_name {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

mod db_keys {
    pub const DYNAMIC_SEARCH_RULES: &str = "dynamic-search-rules";
}

#[derive(Clone)]
pub(crate) struct DynamicSearchRulesStore {
    persisted: Database<Str, SerdeJson<DynamicSearchRules>>,
    runtime: Arc<RwLock<DynamicSearchRules>>,
}

impl DynamicSearchRulesStore {
    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn new(env: &Env<WithoutTls>, wtxn: &mut RwTxn) -> Result<Self> {
        let persisted = env.create_database(wtxn, Some(db_name::DYNAMIC_SEARCH_RULES))?;
        let rules: DynamicSearchRules =
            persisted.get(wtxn, db_keys::DYNAMIC_SEARCH_RULES)?.unwrap_or_default();

        Ok(Self { persisted, runtime: Arc::new(RwLock::new(rules)) })
    }

    pub fn put(&self, mut wtxn: RwTxn, value: DynamicSearchRules) -> Result<()> {
        self.persisted.put(&mut wtxn, db_keys::DYNAMIC_SEARCH_RULES, &value)?;
        wtxn.commit()?;

        let mut runtime = self.runtime.write().unwrap();
        *runtime = value;
        Ok(())
    }

    pub fn get(&self) -> DynamicSearchRules {
        DynamicSearchRules::clone(&*self.runtime.read().unwrap())
    }
}
