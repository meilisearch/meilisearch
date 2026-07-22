use arroy::Error::UnmatchingDistance as ArroyUnmatchingDistance;
use hannoy::Error::UnmatchingDistance as HannoyUnmatchingDistance;
use heed::RwTxn;

use super::UpgradeIndex;
use crate::update::upgrade::UpgradeParams;
use crate::vector::{QuantizationStatus, VectorStore, VectorStoreBackend};
use crate::{Error, Index, InternalError, Result};

/// Convert old Annoy vector stores to Hannoy ones
pub(super) struct ConvertArroyToHannoy();

impl UpgradeIndex for ConvertArroyToHannoy {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        UpgradeParams { progress, must_stop_processing, .. }: UpgradeParams<'_>,
    ) -> Result<bool> {
        let embedders = index.embedding_configs();
        let backend = index.get_vector_store(wtxn)?.unwrap_or_default();

        let rtxn = index.read_txn()?;
        let mut configs = embedders.embedding_configs(wtxn)?;
        for config in &mut configs {
            let embedder_info = embedders.embedder_info(wtxn, &config.name)?.unwrap();

            let quantized = config.config.quantized();
            let vector_store =
                VectorStore::new(backend, index.vector_store, embedder_info.embedder_id, quantized);

            // Read the dimensions to be able to know the real quantization
            // parameter, it corresponds to the quantization of the first store.
            let vector_store = match vector_store.dimensions(&rtxn) {
                Err(Error::InternalError(internal_error)) => match internal_error {
                    InternalError::ArroyError(ArroyUnmatchingDistance { .. })
                    | InternalError::HannoyError(HannoyUnmatchingDistance { .. }) => {
                        // If there is an error reading the dimensions (first store).
                        // Change the config to set the correct quantization.
                        config.config.quantized = Some(!quantized);
                        VectorStore::new(
                            backend,
                            index.vector_store,
                            embedder_info.embedder_id,
                            config.config.quantized(),
                        )
                    }
                    otherwise => return Err(otherwise.into()),
                },
                Err(e) => return Err(e),
                Ok(_) => vector_store,
            };

            match backend {
                VectorStoreBackend::Arroy => {
                    // We make sure to only do the backend conversion when using arroy.
                    //
                    // Continue the hannoy conversion with the right quantization. Note that
                    // when changing the backend the misconfigured quantization stores are
                    // simply deleted.
                    vector_store.change_backend(
                        &rtxn,
                        wtxn,
                        progress.clone(),
                        must_stop_processing,
                        None,
                    )?;
                }
                VectorStoreBackend::Hannoy => {
                    // If the store is hannoy we clean the stores in case some were misconfigured.
                    // Note that we never experienced an issue with hannoy stores but we are not sure
                    // they are affected too.
                    let detected = vector_store.clean_stores(wtxn)?;
                    config.config.quantized =
                        change_quantized_config(config.config.quantized, detected);
                }
            }
        }

        embedders.put_embedding_configs(wtxn, configs)?;
        index.put_vector_store(wtxn, VectorStoreBackend::Hannoy)?;

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 37, 0)
    }

    fn description(&self) -> &'static str {
        "Migrates Arroy vector storage to Hannoy format"
    }
}

pub struct AddShards {}

impl super::UpgradeIndex for AddShards {
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 37, 0)
    }

    fn upgrade(
        &self,
        wtxn: &mut heed::RwTxn,
        index: &crate::Index,
        UpgradeParams { shards, .. }: UpgradeParams<'_>,
    ) -> crate::Result<bool> {
        let Some(shards) = shards else {
            return Ok(false);
        };

        let shard_docids = index.shard_docids();

        for shard in shards.as_sorted_slice() {
            shard_docids.add_shard(wtxn, shard.name.as_str())?;

            // before this upgrade, there is at most one shard owned by the remote.
            // if we find it, we can associate all docids to that shard.
            if shard.is_own {
                let docids = index.documents_ids(wtxn)?;

                shard_docids.put_docids(wtxn, &shard.name, &docids)?;
            }
        }

        Ok(false)
    }

    fn description(&self) -> &'static str {
        "adding shards to network objects"
    }
}

pub fn change_quantized_config(
    config: Option<bool>,
    detected: Option<QuantizationStatus>,
) -> Option<bool> {
    match (config, detected) {
        // empty store, listen to config
        (config, None) => config,

        // conflicts, change to detected
        (Some(false) | None, Some(QuantizationStatus::Quantized)) => Some(true),
        (Some(true), Some(QuantizationStatus::NonQuantized)) => Some(false),

        // no conflict, retain config
        (config @ (Some(false) | None), Some(QuantizationStatus::NonQuantized))
        | (config @ Some(true), Some(QuantizationStatus::Quantized)) => config,
    }
}
