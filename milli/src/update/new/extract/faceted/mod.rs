use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::File;

pub use extract_facets::*;
use grenad::{MergeFunction, Merger};
use heed::RoTxn;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde_json::Value;

use super::cache::CboCachedSorter;
use crate::update::new::{DocumentChange, ItemsPool};
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{DocumentId, FieldId, GlobalFieldsIdsMap, Index, Result};

mod extract_facets;
mod facet_document;

pub trait FacetedExtractor {
    fn run_extraction(
        index: &Index,
        fields_ids_map: &GlobalFieldsIdsMap,
        indexer: GrenadParameters,
        document_changes: impl IntoParallelIterator<Item = Result<DocumentChange>>,
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

        let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
        for (_rtxn, _fields_ids_map, _buffer, cache) in context_pool.into_items() {
            let sorter = cache.into_sorter()?;
            let readers = sorter.into_reader_cursors()?;
            builder.extend(readers);
        }

        Ok(builder.build())
    }

    // TODO Shorten this
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
    {
        buffer.clear();
        match Self::build_key(fid, value, buffer) {
            // TODO manage errors
            Some(key) => Ok(cache_fn(cached_sorter, &key, docid).unwrap()),
            None => Ok(()),
        }
    }

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
            DocumentChange::Deletion(inner) => facet_document::extract_document_facets(
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
                facet_document::extract_document_facets(
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

                facet_document::extract_document_facets(
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
            DocumentChange::Insertion(inner) => facet_document::extract_document_facets(
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

    // TODO avoid owning the strings here.
    fn attributes_to_extract<'a>(rtxn: &'a RoTxn, index: &'a Index) -> Result<HashSet<String>>;

    fn build_key<'b>(field_id: FieldId, value: &Value, output: &'b mut Vec<u8>)
        -> Option<&'b [u8]>;
}
