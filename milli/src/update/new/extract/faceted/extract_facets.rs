use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::File;
use std::ops::DerefMut as _;

use bumpalo::Bump;
use grenad::{MergeFunction, Merger};
use heed::RoTxn;
use rayon::iter::{ParallelBridge as _, ParallelIterator as _};
use serde_json::Value;

use super::super::cache::CboCachedSorter;
use super::facet_document::extract_document_facets;
use super::FacetKind;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::new::extract::DocidsExtractor;
use crate::update::new::indexer::document_changes::{
    for_each_document_change, DocumentChangeContext, DocumentChanges, Extractor, FullySend,
    IndexingContext, ThreadLocal,
};
use crate::update::new::DocumentChange;
use crate::update::{create_sorter, GrenadParameters, MergeDeladdCboRoaringBitmaps};
use crate::{DocumentId, FieldId, Index, Result, MAX_FACET_VALUE_LENGTH};

pub struct FacetedExtractorData<'extractor> {
    attributes_to_extract: &'extractor [&'extractor str],
    grenad_parameters: GrenadParameters,
    max_memory: Option<usize>,
}

impl<'extractor> Extractor<'extractor> for FacetedExtractorData<'extractor> {
    type Data = FullySend<RefCell<CboCachedSorter<MergeDeladdCboRoaringBitmaps>>>;

    fn init_data(
        &self,
        _extractor_alloc: raw_collections::alloc::RefBump<'extractor>,
    ) -> Result<Self::Data> {
        Ok(FullySend(RefCell::new(CboCachedSorter::new(
            // TODO use a better value
            1_000_000.try_into().unwrap(),
            create_sorter(
                grenad::SortAlgorithm::Stable,
                MergeDeladdCboRoaringBitmaps,
                self.grenad_parameters.chunk_compression_type,
                self.grenad_parameters.chunk_compression_level,
                self.grenad_parameters.max_nb_chunks,
                self.max_memory,
            ),
        ))))
    }

    fn process(
        &self,
        change: DocumentChange,
        context: &crate::update::new::indexer::document_changes::DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        FacetedDocidsExtractor::extract_document_change(context, self.attributes_to_extract, change)
    }
}

pub struct FacetedDocidsExtractor;

impl FacetedDocidsExtractor {
    fn extract_document_change(
        context: &DocumentChangeContext<
            FullySend<RefCell<CboCachedSorter<MergeDeladdCboRoaringBitmaps>>>,
        >,
        attributes_to_extract: &[&str],
        document_change: DocumentChange,
    ) -> Result<()> {
        let index = &context.index;
        let rtxn = &context.txn;
        let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut();
        let mut cached_sorter = context.data.0.borrow_mut();
        match document_change {
            DocumentChange::Deletion(inner) => extract_document_facets(
                attributes_to_extract,
                inner.current(rtxn, index, context.db_fields_ids_map)?,
                new_fields_ids_map.deref_mut(),
                &mut |fid, value| {
                    Self::facet_fn_with_options(
                        &context.doc_alloc,
                        cached_sorter.deref_mut(),
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
                    inner.current(rtxn, index, context.db_fields_ids_map)?,
                    new_fields_ids_map.deref_mut(),
                    &mut |fid, value| {
                        Self::facet_fn_with_options(
                            &context.doc_alloc,
                            cached_sorter.deref_mut(),
                            CboCachedSorter::insert_del_u32,
                            inner.docid(),
                            fid,
                            value,
                        )
                    },
                )?;

                extract_document_facets(
                    attributes_to_extract,
                    inner.new(rtxn, index, context.db_fields_ids_map)?,
                    new_fields_ids_map.deref_mut(),
                    &mut |fid, value| {
                        Self::facet_fn_with_options(
                            &context.doc_alloc,
                            cached_sorter.deref_mut(),
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
                new_fields_ids_map.deref_mut(),
                &mut |fid, value| {
                    Self::facet_fn_with_options(
                        &context.doc_alloc,
                        cached_sorter.deref_mut(),
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
        doc_alloc: &Bump,
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
        let mut buffer = bumpalo::collections::Vec::new_in(doc_alloc);
        // Exists
        // key: fid
        buffer.push(FacetKind::Exists as u8);
        buffer.extend_from_slice(&fid.to_be_bytes());
        cache_fn(cached_sorter, &buffer, docid).map_err(Into::into)?;

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
                    buffer.push(0); // level 0
                    buffer.extend_from_slice(&ordered);
                    buffer.extend_from_slice(&n.to_be_bytes());

                    cache_fn(cached_sorter, &buffer, docid).map_err(Into::into)
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
                buffer.push(0); // level 0
                buffer.extend_from_slice(truncated.as_bytes());
                cache_fn(cached_sorter, &buffer, docid).map_err(Into::into)
            }
            // Null
            // key: fid
            Value::Null => {
                buffer.clear();
                buffer.push(FacetKind::Null as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid).map_err(Into::into)
            }
            // Empty
            // key: fid
            Value::Array(a) if a.is_empty() => {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid).map_err(Into::into)
            }
            Value::Object(o) if o.is_empty() => {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid).map_err(Into::into)
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
    fn run_extraction<'pl, 'fid, 'indexer, 'index, DC: DocumentChanges<'pl>>(
        grenad_parameters: GrenadParameters,
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index>,
        extractor_allocs: &mut ThreadLocal<FullySend<RefCell<Bump>>>,
    ) -> Result<Merger<File, MergeDeladdCboRoaringBitmaps>> {
        let max_memory = grenad_parameters.max_memory_by_thread();

        let index = indexing_context.index;

        let rtxn = index.read_txn()?;
        let attributes_to_extract = Self::attributes_to_extract(&rtxn, index)?;
        let attributes_to_extract: Vec<_> =
            attributes_to_extract.iter().map(|s| s.as_ref()).collect();
        let datastore = ThreadLocal::new();

        {
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "docids_extraction");
            let _entered = span.enter();

            let extractor = FacetedExtractorData {
                attributes_to_extract: &attributes_to_extract,
                grenad_parameters,
                max_memory,
            };
            for_each_document_change(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
            )?;
        }
        {
            let mut builder = grenad::MergerBuilder::new(MergeDeladdCboRoaringBitmaps);
            let span =
                tracing::trace_span!(target: "indexing::documents::extract", "merger_building");
            let _entered = span.enter();

            let readers: Vec<_> = datastore
                .into_iter()
                .par_bridge()
                .map(|cached_sorter| {
                    let cached_sorter = cached_sorter.0.into_inner();
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
