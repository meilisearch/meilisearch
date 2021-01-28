use std::sync::Arc;

use heed::Env;

use super::IndexStore;

pub struct UpdateStore {
    env: Env,
    index_store: Arc<IndexStore>,
}

impl UpdateStore {
    pub fn new(env: Env, index_store: Arc<IndexStore>) -> anyhow::Result<Self> {
        Ok(Self { env, index_store })
    }
}

