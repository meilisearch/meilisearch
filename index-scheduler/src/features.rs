use std::sync::{Arc, RwLock};

use meilisearch_types::features::{
    InstanceTogglableFeatures, RuntimeTogglableFeatures, RuntimeToggledFeatures,
};
use meilisearch_types::heed::types::{SerdeJson, Str};
use meilisearch_types::heed::{Database, Env, RoTxn, RwTxn};

use crate::error::Error::RuntimeFeatureToggleError;
use crate::error::FeatureNotEnabledError;
use crate::Result;

const EXPERIMENTAL_FEATURES: &str = "experimental-features";

#[derive(Clone)]
pub(crate) struct FeatureData {
    runtime: Database<Str, SerdeJson<RuntimeTogglableFeatures>>,
    instance: InstanceTogglableFeatures,
    runtime_toggled: Arc<RwLock<RuntimeToggledFeatures>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RoFeatures {
    runtime: RuntimeTogglableFeatures,
}

impl RoFeatures {
    fn new(txn: RoTxn<'_>, data: &FeatureData) -> Result<Self> {
        let runtime = data.runtime_features(txn)?;
        Ok(Self { runtime })
    }

    pub fn runtime_features(&self) -> RuntimeTogglableFeatures {
        self.runtime
    }

    pub fn check_score_details(&self) -> Result<()> {
        if self.runtime.score_details {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Computing score details",
                feature: "score details",
                issue_link: "https://github.com/meilisearch/product/discussions/674",
            }
            .into())
        }
    }

    pub fn check_metrics(&self) -> Result<()> {
        if self.runtime.metrics {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Getting metrics",
                feature: "metrics",
                issue_link: "https://github.com/meilisearch/meilisearch/discussions/3518",
            }
            .into())
        }
    }

    pub fn check_vector(&self) -> Result<()> {
        if self.runtime.vector_store {
            Ok(())
        } else {
            Err(FeatureNotEnabledError {
                disabled_action: "Passing `vector` as a query parameter",
                feature: "vector store",
                issue_link: "https://github.com/meilisearch/product/discussions/677",
            }
            .into())
        }
    }
}

impl FeatureData {
    pub fn new(env: &Env, instance_features: InstanceTogglableFeatures) -> Result<Self> {
        let mut wtxn = env.write_txn()?;
        let runtime_features_db = env.create_database(&mut wtxn, Some(EXPERIMENTAL_FEATURES))?;
        wtxn.commit()?;

        let txn = env.read_txn()?;
        let runtime_features: RuntimeTogglableFeatures =
            runtime_features_db.get(&txn, EXPERIMENTAL_FEATURES)?.unwrap_or_default();

        Ok(Self {
            runtime: runtime_features_db,
            instance: instance_features,
            runtime_toggled: Arc::new(RwLock::new(RuntimeToggledFeatures {
                metrics: (false, runtime_features.metrics),
            })),
        })
    }

    pub fn put_runtime_features(
        &self,
        mut wtxn: RwTxn,
        features: RuntimeTogglableFeatures,
    ) -> Result<()> {
        self.runtime.put(&mut wtxn, EXPERIMENTAL_FEATURES, &features)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn put_runtime_toggled_features(&self, features: RuntimeToggledFeatures) -> Result<()> {
        let mut toggled_features =
            self.runtime_toggled.write().map_err(|_| RuntimeFeatureToggleError)?;

        *toggled_features = features;
        Ok(())
    }

    fn runtime_features(&self, txn: RoTxn) -> Result<RuntimeTogglableFeatures> {
        Ok(RuntimeTogglableFeatures {
            metrics: self.is_metrics_enabled()?,
            ..self.runtime.get(&txn, EXPERIMENTAL_FEATURES)?.unwrap_or_default()
        })
    }

    pub fn is_metrics_enabled(&self) -> Result<bool> {
        let toggled_features =
            self.runtime_toggled.read().map_err(|_| RuntimeFeatureToggleError)?;

        let (is_toggled, current_value) = toggled_features.metrics;
        match (self.instance.metrics, is_toggled, current_value) {
            (false, _, curr) => Ok(curr),
            (_, true, curr) => Ok(curr),
            _ => Ok(true),
        }
    }

    pub fn features(&self, txn: RoTxn) -> Result<RoFeatures> {
        RoFeatures::new(txn, self)
    }
}
