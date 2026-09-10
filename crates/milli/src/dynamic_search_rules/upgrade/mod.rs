use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::dynamic_search_rules::upgrade::metadata::Metadata;
use crate::dynamic_search_rules::DynamicSearchRulesView;
use crate::progress::Progress;
use crate::update::new::indexer::{DocumentUpdateChanges, DocumentUpdater};
use crate::update::IndexerConfig;
use crate::{Index, MustStopProcessing, Result};

mod metadata;
mod v1_53;
mod v1_54;

pub use metadata::{create_metadata, METADATA_UID};

/// Upgrade the internal document schema of the DSR index.
///
/// This function checks the version contained in the current "metadata" document and depending on its value:
///
/// - updates the metadata document version to the Meilisearch binary version
/// - iterates over all the rules and update them to the last schema version.
///
/// This function commits up to two write transactions to the DSR index.
pub fn upgrade_dsrs(
    index: &Index,
    progress: &Progress,
    indexer_config: &IndexerConfig,
    must_stop_processing: &MustStopProcessing,
    ip_policy: &http_client::policy::IpPolicy,
) -> Result<()> {
    let embedder_stats = &Default::default();

    let (metadata, metadata_docid, db_fields_ids_map) = {
        let index_txn = index.read_txn()?;

        let db_fields_ids_map = index.fields_ids_map(&index_txn)?;
        let dsrs = DynamicSearchRulesView::new(index, &index_txn, &db_fields_ids_map);

        let metadata_docid = dsrs.metadata_internal_id()?;
        let metadata = metadata_docid
            .map(|metadata_docid| dsrs.metadata(metadata_docid))
            .transpose()
            .ok()
            .flatten()
            .flatten();

        match metadata.zip(metadata_docid) {
            Some((metadata, metadata_docid)) => (metadata, metadata_docid, db_fields_ids_map),
            // missing or corrupted metadata
            None => {
                tracing::debug!("Missing or corrupted DSR metadata, (re)creating");
                let metadata = Metadata { version: Metadata::LAST_VERSION_WITHOUT_METADATA };

                let mut index_wtxn = index.write_txn()?;

                create_metadata(
                    Metadata::LAST_VERSION_WITHOUT_METADATA,
                    index,
                    &mut index_wtxn,
                    &db_fields_ids_map,
                    progress,
                    indexer_config,
                    must_stop_processing,
                    ip_policy,
                )?;

                let db_fields_ids_map = index.fields_ids_map(&index_wtxn)?;
                let dsrs = DynamicSearchRulesView::new(index, &index_wtxn, &db_fields_ids_map);
                let metadata_docid = dsrs.metadata_internal_id()?.unwrap();

                index_wtxn.commit()?;

                (metadata, metadata_docid, db_fields_ids_map)
            }
        }
    };

    let version = metadata.version;

    let mut index_wtxn = index.write_txn()?;
    let new_fields_ids_map = db_fields_ids_map.clone();
    let target_version = (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);

    match version {
        // DSR as index with older actions => migrate actions + update metadata
        version if ((1, 50, 0)..(1, 54, 0)).contains(&version) => {
            let updater = v1_54::DsrActionsUpdater::new(metadata_docid)
                .or_else(metadata::MetadataUpdater::new(target_version, metadata_docid));
            let updater = DocumentUpdateChanges::new(index.documents_ids(&index_wtxn)?, updater);
            crate::update::new::indexer::index(
                &mut index_wtxn,
                index,
                &indexer_config.thread_pool,
                indexer_config.grenad_parameters(),
                &db_fields_ids_map,
                new_fields_ids_map,
                None,
                &updater,
                Default::default(),
                must_stop_processing,
                progress,
                ip_policy,
                embedder_stats,
            )?;
        }
        // already newer actions => only update metadata
        version if version < target_version => {
            let updater = metadata::MetadataUpdater::new(target_version, metadata_docid);
            let updater = DocumentUpdateChanges::new(std::iter::once(metadata_docid), updater);
            crate::update::new::indexer::index(
                &mut index_wtxn,
                index,
                &indexer_config.thread_pool,
                indexer_config.grenad_parameters(),
                &db_fields_ids_map,
                new_fields_ids_map,
                None,
                &updater,
                Default::default(),
                must_stop_processing,
                progress,
                ip_policy,
                embedder_stats,
            )?;
        }
        _ => {}
    }
    index_wtxn.commit()?;
    Ok(())
}
