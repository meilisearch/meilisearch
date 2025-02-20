use std::sync::atomic::AtomicBool;

use bstr::ByteSlice as _;
use hashbrown::HashMap;
use heed::RwTxn;
use rand::SeedableRng as _;
use time::OffsetDateTime;

use super::super::channel::*;
use crate::documents::PrimaryKey;
use crate::fields_ids_map::metadata::FieldIdMapWithMetadata;
use crate::index::IndexEmbeddingConfig;
use crate::update::settings::InnerIndexSettings;
use crate::vector::{ArroyWrapper, Embedder, EmbeddingConfigs, Embeddings};
use crate::{Error, Index, InternalError, Result};

pub fn write_to_db(
    mut writer_receiver: WriterBbqueueReceiver<'_>,
    finished_extraction: &AtomicBool,
    index: &Index,
    wtxn: &mut RwTxn<'_>,
    arroy_writers: &HashMap<u8, (&str, &Embedder, ArroyWrapper, usize)>,
) -> Result<ChannelCongestion> {
    // Used by by the ArroySetVector to copy the embedding into an
    // aligned memory area, required by arroy to accept a new vector.
    let mut aligned_embedding = Vec::new();
    let span = tracing::trace_span!(target: "indexing::write_db", "all");
    let _entered = span.enter();
    let span = tracing::trace_span!(target: "indexing::write_db", "post_merge");
    let mut _entered_post_merge = None;
    while let Some(action) = writer_receiver.recv_action() {
        if _entered_post_merge.is_none()
            && finished_extraction.load(std::sync::atomic::Ordering::Relaxed)
        {
            _entered_post_merge = Some(span.enter());
        }

        match action {
            ReceiverAction::WakeUp => (),
            ReceiverAction::LargeEntry(LargeEntry { database, key, value }) => {
                let database_name = database.database_name();
                let database = database.database(index);
                if let Err(error) = database.put(wtxn, &key, &value) {
                    return Err(Error::InternalError(InternalError::StorePut {
                        database_name,
                        key: bstr::BString::from(&key[..]),
                        value_length: value.len(),
                        error,
                    }));
                }
            }
            ReceiverAction::LargeVectors(large_vectors) => {
                let LargeVectors { docid, embedder_id, .. } = large_vectors;
                let (_, _, writer, dimensions) =
                    arroy_writers.get(&embedder_id).expect("requested a missing embedder");
                let mut embeddings = Embeddings::new(*dimensions);
                for embedding in large_vectors.read_embeddings(*dimensions) {
                    embeddings.push(embedding.to_vec()).unwrap();
                }
                writer.del_items(wtxn, *dimensions, docid)?;
                writer.add_items(wtxn, docid, &embeddings)?;
            }
        }

        // Every time the is a message in the channel we search
        // for new entries in the BBQueue buffers.
        write_from_bbqueue(
            &mut writer_receiver,
            index,
            wtxn,
            arroy_writers,
            &mut aligned_embedding,
        )?;
    }

    write_from_bbqueue(&mut writer_receiver, index, wtxn, arroy_writers, &mut aligned_embedding)?;

    Ok(ChannelCongestion {
        attempts: writer_receiver.sent_messages_attempts(),
        blocking_attempts: writer_receiver.blocking_sent_messages_attempts(),
    })
}

/// Stats exposing the congestion of a channel.
#[derive(Debug, Copy, Clone)]
pub struct ChannelCongestion {
    /// Number of attempts to send a message into the bbqueue buffer.
    pub attempts: usize,
    /// Number of blocking attempts which require a retry.
    pub blocking_attempts: usize,
}

impl ChannelCongestion {
    pub fn congestion_ratio(&self) -> f32 {
        self.blocking_attempts as f32 / self.attempts as f32
    }
}

#[tracing::instrument(level = "debug", skip_all, target = "indexing::vectors")]
pub fn build_vectors<MSP>(
    index: &Index,
    wtxn: &mut RwTxn<'_>,
    index_embeddings: Vec<IndexEmbeddingConfig>,
    arroy_writers: &mut HashMap<u8, (&str, &Embedder, ArroyWrapper, usize)>,
    must_stop_processing: &MSP,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync + Send,
{
    if index_embeddings.is_empty() {
        return Ok(());
    }

    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    for (_index, (_embedder_name, _embedder, writer, dimensions)) in arroy_writers {
        let dimensions = *dimensions;
        writer.build_and_quantize(wtxn, &mut rng, dimensions, false, must_stop_processing)?;
    }

    index.put_embedding_configs(wtxn, index_embeddings)?;
    Ok(())
}

pub fn update_index(
    index: &Index,
    wtxn: &mut RwTxn<'_>,
    new_fields_ids_map: FieldIdMapWithMetadata,
    new_primary_key: Option<PrimaryKey<'_>>,
    embedders: EmbeddingConfigs,
    field_distribution: std::collections::BTreeMap<String, u64>,
    document_ids: roaring::RoaringBitmap,
) -> Result<()> {
    index.put_fields_ids_map(wtxn, new_fields_ids_map.as_fields_ids_map())?;
    if let Some(new_primary_key) = new_primary_key {
        index.put_primary_key(wtxn, new_primary_key.name())?;
    }
    let mut inner_index_settings = InnerIndexSettings::from_index(index, wtxn, Some(embedders))?;
    inner_index_settings.recompute_facets(wtxn, index)?;
    inner_index_settings.recompute_searchables(wtxn, index)?;
    index.put_field_distribution(wtxn, &field_distribution)?;
    index.put_documents_ids(wtxn, &document_ids)?;
    index.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;
    Ok(())
}

/// A function dedicated to manage all the available BBQueue frames.
///
/// It reads all the available frames, do the corresponding database operations
/// and stops when no frame are available.
pub fn write_from_bbqueue(
    writer_receiver: &mut WriterBbqueueReceiver<'_>,
    index: &Index,
    wtxn: &mut RwTxn<'_>,
    arroy_writers: &HashMap<u8, (&str, &crate::vector::Embedder, ArroyWrapper, usize)>,
    aligned_embedding: &mut Vec<f32>,
) -> crate::Result<()> {
    while let Some(frame_with_header) = writer_receiver.recv_frame() {
        match frame_with_header.header() {
            EntryHeader::DbOperation(operation) => {
                let database_name = operation.database.database_name();
                let database = operation.database.database(index);
                let frame = frame_with_header.frame();
                match operation.key_value(frame) {
                    (key, Some(value)) => {
                        if let Err(error) = database.put(wtxn, key, value) {
                            return Err(Error::InternalError(InternalError::StorePut {
                                database_name,
                                key: key.into(),
                                value_length: value.len(),
                                error,
                            }));
                        }
                    }
                    (key, None) => match database.delete(wtxn, key) {
                        Ok(false) => {
                            tracing::error!(
                                database_name,
                                key_bytes = ?key,
                                formatted_key = ?key.as_bstr(),
                                "Attempt to delete an unknown key"
                            );
                        }
                        Ok(_) => (),
                        Err(error) => {
                            return Err(Error::InternalError(InternalError::StoreDeletion {
                                database_name,
                                key: key.into(),
                                error,
                            }));
                        }
                    },
                }
            }
            EntryHeader::ArroyDeleteVector(ArroyDeleteVector { docid }) => {
                for (_index, (_name, _embedder, writer, dimensions)) in arroy_writers {
                    let dimensions = *dimensions;
                    writer.del_items(wtxn, dimensions, docid)?;
                }
            }
            EntryHeader::ArroySetVectors(asvs) => {
                let ArroySetVectors { docid, embedder_id, .. } = asvs;
                let frame = frame_with_header.frame();
                let (_, _, writer, dimensions) =
                    arroy_writers.get(&embedder_id).expect("requested a missing embedder");
                let mut embeddings = Embeddings::new(*dimensions);
                let all_embeddings = asvs.read_all_embeddings_into_vec(frame, aligned_embedding);
                embeddings.append(all_embeddings.to_vec()).unwrap();
                writer.del_items(wtxn, *dimensions, docid)?;
                writer.add_items(wtxn, docid, &embeddings)?;
            }
        }
    }

    Ok(())
}
