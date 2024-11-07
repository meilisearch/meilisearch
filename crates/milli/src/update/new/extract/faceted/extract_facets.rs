use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::DerefMut as _;

use bumpalo::Bump;
use heed::RoTxn;
use serde_json::Value;

use super::super::cache::BalancedCaches;
use super::facet_document::extract_document_facets;
use super::FacetKind;
use crate::facet::value_encoding::f64_into_bytes;
use crate::update::new::extract::DocidsExtractor;
use crate::update::new::indexer::document_changes::{
    extract, DocumentChangeContext, DocumentChanges, Extractor, FullySend, IndexingContext,
    Progress, ThreadLocal,
};
use crate::update::new::ref_cell_ext::RefCellExt as _;
use crate::update::new::DocumentChange;
use crate::update::GrenadParameters;
use crate::{DocumentId, FieldId, Index, Result, MAX_FACET_VALUE_LENGTH};

pub struct FacetedExtractorData<'a> {
    attributes_to_extract: &'a [&'a str],
    grenad_parameters: GrenadParameters,
    buckets: usize,
}

impl<'a, 'extractor> Extractor<'extractor> for FacetedExtractorData<'a> {
    type Data = RefCell<BalancedCaches<'extractor>>;

    fn init_data(&self, extractor_alloc: &'extractor Bump) -> Result<Self::Data> {
        Ok(RefCell::new(BalancedCaches::new_in(
            self.buckets,
            self.grenad_parameters.max_memory,
            extractor_alloc,
        )))
    }

    fn process<'doc>(
        &self,
        changes: impl Iterator<Item = Result<DocumentChange<'doc>>>,
        context: &DocumentChangeContext<Self::Data>,
    ) -> Result<()> {
        for change in changes {
            let change = change?;
            FacetedDocidsExtractor::extract_document_change(
                context,
                self.attributes_to_extract,
                change,
            )?
        }
        Ok(())
    }
}

pub struct FacetedDocidsExtractor;

impl FacetedDocidsExtractor {
    fn extract_document_change(
        context: &DocumentChangeContext<RefCell<BalancedCaches>>,
        attributes_to_extract: &[&str],
        document_change: DocumentChange,
    ) -> Result<()> {
        let index = &context.index;
        let rtxn = &context.rtxn;
        let mut new_fields_ids_map = context.new_fields_ids_map.borrow_mut_or_yield();
        let mut cached_sorter = context.data.borrow_mut_or_yield();
        match document_change {
            DocumentChange::Deletion(inner) => extract_document_facets(
                attributes_to_extract,
                inner.current(rtxn, index, context.db_fields_ids_map)?,
                new_fields_ids_map.deref_mut(),
                &mut |fid, value| {
                    Self::facet_fn_with_options(
                        &context.doc_alloc,
                        cached_sorter.deref_mut(),
                        BalancedCaches::insert_del_u32,
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
                            BalancedCaches::insert_del_u32,
                            inner.docid(),
                            fid,
                            value,
                        )
                    },
                )?;

                extract_document_facets(
                    attributes_to_extract,
                    inner.merged(rtxn, index, context.db_fields_ids_map)?,
                    new_fields_ids_map.deref_mut(),
                    &mut |fid, value| {
                        Self::facet_fn_with_options(
                            &context.doc_alloc,
                            cached_sorter.deref_mut(),
                            BalancedCaches::insert_add_u32,
                            inner.docid(),
                            fid,
                            value,
                        )
                    },
                )
            }
            DocumentChange::Insertion(inner) => extract_document_facets(
                attributes_to_extract,
                inner.inserted(),
                new_fields_ids_map.deref_mut(),
                &mut |fid, value| {
                    Self::facet_fn_with_options(
                        &context.doc_alloc,
                        cached_sorter.deref_mut(),
                        BalancedCaches::insert_add_u32,
                        inner.docid(),
                        fid,
                        value,
                    )
                },
            ),
        }
    }

    fn facet_fn_with_options<'extractor>(
        doc_alloc: &Bump,
        cached_sorter: &mut BalancedCaches<'extractor>,
        cache_fn: impl Fn(&mut BalancedCaches<'extractor>, &[u8], u32) -> Result<()>,
        docid: DocumentId,
        fid: FieldId,
        value: &Value,
    ) -> Result<()> {
        let mut buffer = bumpalo::collections::Vec::new_in(doc_alloc);
        // Exists
        // key: fid
        buffer.push(FacetKind::Exists as u8);
        buffer.extend_from_slice(&fid.to_be_bytes());
        cache_fn(cached_sorter, &buffer, docid)?;

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
                    cache_fn(cached_sorter, &buffer, docid)
                } else {
                    Ok(())
                }
            }
            // String
            // key: fid - level - truncated_string
            Value::String(s) => {
                let normalized = crate::normalize_facet(s);
                let truncated = truncate_str(&normalized);
                buffer.clear();
                buffer.push(FacetKind::String as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                buffer.push(0); // level 0
                buffer.extend_from_slice(truncated.as_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            // Null
            // key: fid
            Value::Null => {
                buffer.clear();
                buffer.push(FacetKind::Null as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            // Empty
            // key: fid
            Value::Array(a) if a.is_empty() => {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
            }
            Value::Object(o) if o.is_empty() => {
                buffer.clear();
                buffer.push(FacetKind::Empty as u8);
                buffer.extend_from_slice(&fid.to_be_bytes());
                cache_fn(cached_sorter, &buffer, docid)
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
    fn run_extraction<'pl, 'fid, 'indexer, 'index, 'extractor, DC: DocumentChanges<'pl>, MSP, SP>(
        grenad_parameters: GrenadParameters,
        document_changes: &DC,
        indexing_context: IndexingContext<'fid, 'indexer, 'index, MSP, SP>,
        extractor_allocs: &'extractor mut ThreadLocal<FullySend<Bump>>,
        finished_steps: u16,
        total_steps: u16,
        step_name: &'static str,
    ) -> Result<Vec<BalancedCaches<'extractor>>>
    where
        MSP: Fn() -> bool + Sync,
        SP: Fn(Progress) + Sync,
    {
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
                buckets: rayon::current_num_threads(),
            };
            extract(
                document_changes,
                &extractor,
                indexing_context,
                extractor_allocs,
                &datastore,
                finished_steps,
                total_steps,
                step_name,
            )?;
        }

        Ok(datastore.into_iter().map(RefCell::into_inner).collect())
    }
}
