use heed::RwTxn;

use crate::dynamic_search_rules::fields;
use crate::progress::Progress;
use crate::update::new::document::{Document, Versions};
use crate::update::new::indexer::{DocumentUpdater, IndexOperations, Payload};
use crate::update::new::Update;
use crate::update::IndexerConfig;
use crate::{DocumentId, FieldsIdsMap, Index, InternalError, MustStopProcessing, Result};

mod metadata_fields {
    pub const VERSION: &str = "version";
}

pub(super) struct Metadata {
    pub version: (u32, u32, u32),
}

/// Reserved UID for the special "metadata" documents.
///
/// - Attempting to create a rule with this UID returns an error.
/// - Attempting to get a rule with this UID never finds a rule.
pub const METADATA_UID: &str = "__meilisearch_metadata";

impl Metadata {
    pub fn to_json_doc(&self) -> serde_json::Value {
        serde_json::json!({
            fields::UID : METADATA_UID,
            metadata_fields::VERSION: self.version,
        })
    }

    pub fn from_doc<'doc>(doc: impl Document<'doc>) -> Result<Self> {
        let version = doc.top_level_field(metadata_fields::VERSION)?.ok_or(
            InternalError::DatabaseMissingEntry { db_name: "DSR index", key: Some(METADATA_UID) },
        )?;

        let version = serde_json::from_str(version.get()).map_err(InternalError::SerdeJson)?;
        Ok(Self { version })
    }

    pub const LAST_VERSION_WITHOUT_METADATA: (u32, u32, u32) = (1, 54, 0);
}

/// Creates the metadata for the specified DSR index with the specified version triple.
#[allow(clippy::too_many_arguments)]
pub fn create_metadata(
    version: (u32, u32, u32),
    index: &Index,
    wtxn: &mut RwTxn<'_>,
    db_fields_ids_map: &FieldsIdsMap,
    progress: &Progress,
    indexer_config: &IndexerConfig,
    must_stop_processing: &MustStopProcessing,
    ip_policy: &http_client::policy::IpPolicy,
) -> Result<()> {
    let rtxn = index.read_txn()?;
    let indexer_alloc = bumpalo::Bump::new();
    let mut new_fields_ids_map = db_fields_ids_map.clone();

    let metadata = Metadata { version };

    let mut indexer = IndexOperations::new();

    let mut buf = bumpalo::collections::Vec::new_in(&indexer_alloc);

    // unwrap: writing to a vec cannot fail
    serde_json::to_writer(&mut buf, &metadata.to_json_doc()).unwrap();

    let buf = buf.into_bump_slice();

    indexer.push_raw_operation(Payload::Replace {
        payload: buf,
        on_missing_document: crate::update::MissingDocumentPolicy::Create,
    });
    let (document_changes, _, _) = indexer.into_changes(
        &indexer_alloc,
        index,
        &rtxn,
        Some(fields::UID),
        &mut new_fields_ids_map,
        must_stop_processing,
        progress.clone(),
        // no sharding for the DSR index: DSR rules are fully replicated on all remotes
        None,
    )?;

    crate::update::new::indexer::index(
        wtxn,
        index,
        &indexer_config.thread_pool,
        indexer_config.grenad_parameters(),
        db_fields_ids_map,
        new_fields_ids_map,
        None,
        &document_changes,
        Default::default(),
        must_stop_processing,
        progress,
        ip_policy,
        &Default::default(),
    )?;
    Ok(())
}

pub(super) struct MetadataUpdater {
    target: (u32, u32, u32),
    metadata_docid: DocumentId,
}

impl MetadataUpdater {
    pub fn new(target: (u32, u32, u32), metadata_docid: DocumentId) -> Self {
        Self { target, metadata_docid }
    }
}

impl DocumentUpdater for MetadataUpdater {
    fn update<'doc, T, D>(
        &self,
        context: &'doc crate::update::new::document::DocumentContext<T>,
        docid: u32,
        _current: D,
    ) -> Result<Option<crate::update::new::DocumentChange<'doc>>>
    where
        T: crate::update::new::thread_local::MostlySend + 'doc,
        D: Document<'doc>,
    {
        if self.metadata_docid != docid {
            return Ok(None);
        }

        // current is the metadata doc
        let mut update = Versions::empty(&context.doc_alloc);
        if let Err(err) =
            update.insert_top_level_field_value(metadata_fields::VERSION, &self.target)
        {
            tracing::error!("Could not write version: {err}");
            return Ok(None);
        }

        Ok(Some(crate::update::new::DocumentChange::Update(Update::create(
            docid,
            METADATA_UID,
            update,
            false,
        ))))
    }
}
