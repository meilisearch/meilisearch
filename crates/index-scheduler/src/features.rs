use std::sync::{Arc, RwLock};

use meilisearch_types::features::{InstanceTogglableFeatures, Network, RuntimeTogglableFeatures};
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RwTxn};

use crate::error::FeatureNotEnabledError;
use crate::Result;

/// The number of database used by features
const NUMBER_OF_DATABASES: u32 = 1;
/// Database const names for the `FeatureData`.
mod db_name {
    pub const EXPERIMENTAL_FEATURES: &str = "experimental-features";
}

mod db_keys {
    pub const EXPERIMENTAL_FEATURES: &str = "experimental-features";
    pub const NETWORK: &str = "network";
}

#[derive(Clone)]
pub(crate) struct FeatureData {
    persisted: Database<Str, SerdeJson<RuntimeTogglableFeatures>>,
    runtime: Arc<RwLock<RuntimeTogglableFeatures>>,
    network: Arc<RwLock<Network>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RoFeatures {
    runtime: RuntimeTogglableFeatures,
}

impl RoFeatures {
    fn new(data: &FeatureData) -> Self {
        let runtime = data.runtime_features();
        Self { runtime }
    }

    pub fn runtime_features(&self) -> RuntimeTogglableFeatures {
        self.runtime
    }

    pub fn check_metrics(&self) -> Result<()> {
        if self.runtime.metrics {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Getting metrics",
                feature: "metrics",
                issue_link: "https://github.com/meilisearch/product/discussions/625",
            }
            .into())
        }
    }

    pub fn check_logs_route(&self) -> Result<()> {
        if self.runtime.logs_route {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Modifying logs through the `/logs/*` routes",
                feature: "logs route",
                issue_link: "https://github.com/orgs/meilisearch/discussions/721",
            }
            .into())
        }
    }

    pub fn check_edit_documents_by_function(&self, disabled_action: &'static str) -> Result<()> {
        if self.runtime.edit_documents_by_function {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action,
                feature: "edit documents by function",
                issue_link: "https://github.com/orgs/meilisearch/discussions/762",
            }
            .into())
        }
    }

    pub fn check_contains_filter(&self) -> Result<()> {
        if self.runtime.contains_filter {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Using `CONTAINS` or `STARTS WITH` in a filter",
                feature: "contains filter",
                issue_link: "https://github.com/orgs/meilisearch/discussions/763",
            }
            .into())
        }
    }

    pub fn check_network(&self, disabled_action: &'static str) -> Result<()> {
        if self.runtime.network {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action,
                feature: "network",
                issue_link: "https://github.com/orgs/meilisearch/discussions/805",
            }
            .into())
        }
    }

    pub fn check_get_task_documents_route(&self) -> Result<()> {
        if self.runtime.get_task_documents_route {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Getting the documents of an enqueued task",
                feature: "get task documents route",
                issue_link: "https://github.com/orgs/meilisearch/discussions/808",
            }
            .into())
        }
    }
}

impl FeatureData {
    pub(crate) const fn nb_db() -> u32 {
        NUMBER_OF_DATABASES
    }

    pub fn new(
        env: &Env,
        wtxn: &mut RwTxn,
        instance_features: InstanceTogglableFeatures,
    ) -> Result<Self> {
        let runtime_features_db =
            env.create_database(wtxn, Some(db_name::EXPERIMENTAL_FEATURES))?;

        let persisted_features: RuntimeTogglableFeatures =
            runtime_features_db.get(wtxn, db_keys::EXPERIMENTAL_FEATURES)?.unwrap_or_default();
        let InstanceTogglableFeatures { metrics, logs_route, contains_filter } = instance_features;
        let runtime = Arc::new(RwLock::new(RuntimeTogglableFeatures {
            metrics: metrics || persisted_features.metrics,
            logs_route: logs_route || persisted_features.logs_route,
            contains_filter: contains_filter || persisted_features.contains_filter,
            ..persisted_features
        }));

        let network_db = runtime_features_db.remap_data_type::<SerdeJson<Network>>();
        let network: Network = network_db.get(wtxn, db_keys::NETWORK)?.unwrap_or_default();

        Ok(Self {
            persisted: runtime_features_db,
            runtime,
            network: Arc::new(RwLock::new(network)),
        })
    }

    pub fn put_runtime_features(
        &self,
        mut wtxn: RwTxn,
        features: RuntimeTogglableFeatures,
    ) -> Result<()> {
        self.persisted.put(&mut wtxn, db_keys::EXPERIMENTAL_FEATURES, &features)?;
        wtxn.commit()?;

        // safe to unwrap, the lock will only fail if:
        // 1. requested by the same thread concurrently -> it is called and released in methods that don't call each other
        // 2. there's a panic while the thread is held -> it is only used for an assignment here.
        let mut toggled_features = self.runtime.write().unwrap();
        *toggled_features = features;
        Ok(())
    }

    fn runtime_features(&self) -> RuntimeTogglableFeatures {
        // sound to unwrap, the lock will only fail if:
        // 1. requested by the same thread concurrently -> it is called and released in methods that don't call each other
        // 2. there's a panic while the thread is held -> it is only used for copying the data here
        *self.runtime.read().unwrap()
    }

    pub fn features(&self) -> RoFeatures {
        RoFeatures::new(self)
    }

    pub fn put_network(&self, mut wtxn: RwTxn, new_network: Network) -> Result<()> {
        self.persisted.remap_data_type::<SerdeJson<Network>>().put(
            &mut wtxn,
            db_keys::NETWORK,
            &new_network,
        )?;
        wtxn.commit()?;

        let mut network = self.network.write().unwrap();
        *network = new_network;
        Ok(())
    }

    pub fn network(&self) -> Network {
        Network::clone(&*self.network.read().unwrap())
    }
}
