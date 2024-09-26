use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::File;
use std::sync::Arc;

use grenad::{MergeFunction, Merger};
use heed::RoTxn;
use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
use serde_json::Value;

use super::super::cache::CboCachedSorter;
use super::facet_document::extract_document_facets;
use super::FacetKind;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::new::extract::DocidsExtractor;
use crate::update::new::{DocumentChange, ItemsPool};
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{
    DocumentId, Error, FieldId, GlobalFieldsIdsMap, Index, Result, MAX_FACET_VALUE_LENGTH,
};
pub struct FacetedDocidsExtractor;

impl FacetedDocidsExtractor {
    fn extract_document_change(
        rtxn: &RoTxn,
        index: &Index,
        buffer: &mut Vec<u8>,
        fields_ids_map: &mut GlobalFieldsIdsMap,
        attributes_to_extract: &[&str],
        cached_sorter: &mut CboCachedSorter<MergeDeladdCboRoaringBitmaps>,
        document_change: DocumentChange,
    ) -> Result<()> {
        match document_change {
            DocumentChange::Deletion(inner) => extract_document_facets(
                attributes_to_extract,
                inner.current(rtxn, index)?.unwrap(),
                fields_ids_map,
                &mut |fid, value| {
                    Self::facet_fn_with_options(
                        buffer,
                        cached_sorter,
                        CboCachedSorter::insert_del_u32,
                        inner.docid(),
                        fid,
                        value,
                    )
                },
            ),
            DocumentChange::Update(inner) => {
                extract_document_facets(
                    attributes_to_extract,
                    inner.current(rtxn, index)?.unwrap(),
                    fields_ids_map,
                    &mut |fid, value| {
                        Self::facet_fn_with_options(
                            buffer,
                            cached_sorter,
                            CboCachedSorter::insert_del_u32,
                            inner.docid(),
                            fid,
                            value,
                        )
                    },
                )?;

                extract_document_facets(
                    attributes_to_extract,
                    inner.new(),
                    fields_ids_map,
                    &mut |fid, value| {
                        Self::facet_fn_with_options(
                            buffer,
                            cached_sorter,
                            CboCachedSorter::insert_add_u32,
                            inner.docid(),
                            fid,
                            value,
                        )
                    },
                )
            }
            DocumentChange::Insertion(inner) => extract_document_facets(
                attributes_to_extract,
                inner.new(),
                fields_ids_map,
                &mut |fid, value| {
                    Self::facet_fn_with_options(
                        buffer,
                        cached_sorter,
                        CboCachedSorter::insert_add_u32,
                        inner.docid(),
                        fid,
                        value,
                    )
                },
            ),
        }
    }

    fn facet_fn_with_options<MF>(
        buffer: &mut Vec<u8>,
        cached_sorter: &mut CboCachedSorter<MF>,
        cache_fn: impl Fn(&mut CboCachedSorter<MF>, &[u8], u32) -> grenad::Result<(), MF::Error>,
        docid: DocumentId,
        fid: FieldId,
        value: &Value,
    ) -> Result<()>
    where
        MF: MergeFunction,
        MF::Error: Debug,
        grenad::Error<MF::Error>: Into<crate::Error>,
    {
        // Exists
        // key: fid
        buffer.clear();
        buffer.push(FacetKind::Exists as u8);
        buffer.extend_from_slice(&fid.to_be_bytes());
        cache_fn(cached_sorter, &*buffer, docid).map_err(Into::into)?;

        match value {
            // Number
            // key: fid - level - orderedf64 - orignalf64
            Value::Number(number) => {
                if let Some((n, ordered)) =
                    number.as_f64().and_then(|n| f64_into_bytes(n).map(|ordered| (n, ordered)))
                {
                    buffer.clear();
                    buffer.push(FacetKind::Number as u8);
                    buffer.extend_from_slice(&fid.to_be_bytes());
                    buffer.push(1); // level 0
                    buffer.extend_from_slice(&ordered);
                    buffer.extend_from_slice(&n.to_be_bytes());

                    cache_fn(cached_sorter, &*buffer, docid).map_err(Into::into)
                } else {
                    Ok(())
                }
            }
            // String
            // key: fid - level - truncated_string
            Value::String(s) => {
                let truncated = truncate_str(s);
                buffer.clear();
                buffer.push(FacetKind::String as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                buffer.push(1); // level 0
                buffer.extend_from_slice(truncated.as_bytes());
                cache_fn(cached_sorter, &*buffer, docid).map_err(Into::into)
            }
            // Null
            // key: fid
            Value::Null => {
                buffer.clear();
                buffer.push(FacetKind::Null as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &*buffer, docid).map_err(Into::into)
            }
            // Empty
            // key: fid
            Value::Array(a) if a.is_empty() => {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &*buffer, docid).map_err(Into::into)
            }
            Value::Object(o) if o.is_empty() => {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &*buffer, docid).map_err(Into::into)
            }
            // Otherwise, do nothing
            /// TODO: What about Value::Bool?
            _ => Ok(()),
        }
    }

    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>> {
        index.user_defined_faceted_fields(rtxn)
    }
}

/// Truncates a string to the biggest valid LMDB key size.
fn truncate_str(s: &str) -> &str {
    let index = s
        .char_indices()
        .map(|(idx, _)| idx)
        .chain(std::iter::once(s.len()))
        .take_while(|idx| idx <= &MAX_FACET_VALUE_LENGTH)
        .last();

    &s[..index.unwrap_or(0)]
}

impl DocidsExtractor for FacetedDocidsExtractor {
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::extract::faceted")]
    fn run_extraction(
        index: &Index,
        fields_ids_map: &GlobalFieldsIdsMap,
        indexer: GrenadParameters,
        document_changes: impl IntoParallelIterator<
            Item = std::result::Result<DocumentChange, Arc<Error>>,
        >,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        let max_memory = indexer.max_memory_by_thread();

        let rtxn = index.read_txn()?;
        let attributes_to_extract = Self::attributes_to_extract(&rtxn, index)?;
        let attributes_to_extract: Vec<_> =
            attributes_to_extract.iter().map(|s| s.as_ref()).collect();

        let context_pool = ItemsPool::new(|| {
            Ok((
                index.read_txn()?,
                fields_ids_map.clone(),
                Vec::new(),
                CboCachedSorter::new(
                    // TODO use a better value
                    100.try_into().unwrap(),
                    create_sorter(
                        grenad::SortAlgorithm::Stable,
                        MergeDeladdCboRoaringBitmaps,
                        indexer.chunk_compression_type,
                        indexer.chunk_compression_level,
                        indexer.max_nb_chunks,
                        max_memory,
                    ),
                ),
            ))
        });

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();
            document_changes.into_par_iter().try_for_each(|document_change| {
                context_pool.with(|(rtxn, fields_ids_map, buffer, cached_sorter)| {
                    Self::extract_document_change(
                        &*rtxn,
                        index,
                        buffer,
                        fields_ids_map,
                        &attributes_to_extract,
                        cached_sorter,
                        document_change?,
                    )
                })
            })?;
        }
        {
            let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "merger_building");
            let _entered = span.enter();

            let readers: Vec<_> = context_pool
                .into_items()
                .par_bridge()
                .map(|(_rtxn, _tokenizer, _fields_ids_map, cached_sorter)| {
                    let sorter = cached_sorter.into_sorter()?;
                    sorter.into_reader_cursors()
                })
                .collect();
            for reader in readers {
                builder.extend(reader?);
            }
            Ok(builder.build())
        }
    }
}
