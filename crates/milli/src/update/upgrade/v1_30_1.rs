use heed::RwTxn;
use rand::SeedableRng as _;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::vector::VectorStore;
use crate::{Index, Result};

/// Rebuilds the hannoy graph and do not touch to the embeddings.
///
/// This follows a bug in hannoy v0.0.9 and v0.1.0 where the graph
/// was not built correctly.
pub(super) struct RebuildHannoyGraph;

impl UpgradeIndex for RebuildHannoyGraph {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool> {
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
                &|| false,
            )?;
        }

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 30, 1)
    }

    fn description(&self) -> &'static str {
        "Rebuilding graph links"
    }
}
