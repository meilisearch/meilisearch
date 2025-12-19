use std::collections::BTreeSet;

use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::vector::VectorStore;
use crate::{make_enum_progress, Index, MustStopProcessing, Result};

pub(super) struct CleanupFidBasedDatabases();

impl UpgradeIndex for CleanupFidBasedDatabases {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        must_stop_processing: &MustStopProcessing,
        progress: Progress,
    ) -> Result<bool> {
        make_enum_progress! {
            enum CleanupFidBasedDatabases {
                RetrievingFidsToDelete,
                DeletingFidBasedDatabases,
            }
        };

        // Force-delete the fid-based databases for the fids that are not searchable.
        // This is a sanity cleanup step to ensure that the database is not corrupted.
        progress.update_progress(CleanupFidBasedDatabases::RetrievingFidsToDelete);
        let fid_map = index.fields_ids_map_with_metadata(wtxn)?;
        let fids_to_delete: BTreeSet<_> = fid_map
            .iter()
            .filter_map(|(id, _, metadata)| if !metadata.is_searchable() { Some(id) } else { None })
            .collect();

        if !fids_to_delete.is_empty() {
            progress.update_progress(CleanupFidBasedDatabases::DeletingFidBasedDatabases);
            crate::update::new::indexer::delete_old_fid_based_databases_from_fids(
                wtxn,
                index,
                &|| must_stop_processing.get(),
                &fids_to_delete,
                &progress,
            )?;
        }

        Ok(false)
    }
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 32, 0)
    }

    fn description(&self) -> &'static str {
        "Cleaning up the fid-based databases"
    }
}

/// Rebuilds the hannoy graph and do not touch to the embeddings.
///
/// This follows a bug in hannoy v0.0.9 and v0.1.0 where the graph
/// was not built correctly.
pub(super) struct RebuildHannoyGraph();

impl UpgradeIndex for RebuildHannoyGraph {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        must_stop_processing: &MustStopProcessing,
        progress: Progress,
    ) -> Result<bool> {
        let embedders = index.embedding_configs();
        let backend = index.get_vector_store(wtxn)?.unwrap_or_default();

        for config in embedders.embedding_configs(wtxn)? {
            let embedder_info = embedders.embedder_info(wtxn, &config.name)?.unwrap();
            let mut vector_store = VectorStore::new(
                backend,
                index.vector_store,
                embedder_info.embedder_id,
                config.config.quantized(),
            );

            let seed = rand::random();
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            vector_store.rebuild_graph(
                wtxn,
                progress.clone(),
                &mut rng,
                vector_store.dimensions(wtxn)?.unwrap(),
                &|| must_stop_processing.get(),
            )?;
        }

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 32, 0)
    }

    fn description(&self) -> &'static str {
        "Rebuilding graph links"
    }
}
