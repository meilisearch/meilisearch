use std::collections::{BTreeSet, HashMap, HashSet};

use fst::{SetBuilder, Streamer};
use sdset::{duo::DifferenceByKey, SetBuf, SetOperation};

use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::facets;
use crate::store;
use crate::update::{next_update_id, compute_short_prefixes, Update};
use crate::{DocumentId, Error, MResult, RankedMap, MainWriter, Index};

pub struct DocumentsDeletion {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    external_docids: Vec<String>,
}

impl DocumentsDeletion {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: UpdateEventsEmitter,
    ) -> DocumentsDeletion {
        DocumentsDeletion {
            updates_store,
            updates_results_store,
            updates_notifier,
            external_docids: Vec::new(),
        }
    }

    pub fn delete_document_by_external_docid(&mut self, document_id: String) {
        self.external_docids.push(document_id);
    }

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_documents_deletion(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.external_docids,
        )?;
        Ok(update_id)
    }
}

impl Extend<String> for DocumentsDeletion {
    fn extend<T: IntoIterator<Item=String>>(&mut self, iter: T) {
        self.external_docids.extend(iter)
    }
}

pub fn push_documents_deletion(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    external_docids: Vec<String>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::documents_deletion(external_docids);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_documents_deletion(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    external_docids: Vec<String>,
) -> MResult<()>
{
    let (external_docids, internal_docids) = {
        let new_external_docids = SetBuf::from_dirty(external_docids);
        let mut internal_docids = Vec::new();

        let old_external_docids = index.main.external_docids(writer)?;
        for external_docid in new_external_docids.as_slice() {
            if let Some(id) = old_external_docids.get(external_docid) {
                internal_docids.push(DocumentId(id as u32));
            }
        }

        let new_external_docids = fst::Map::from_iter(new_external_docids.into_iter().map(|k| (k, 0))).unwrap();
        (new_external_docids, SetBuf::from_dirty(internal_docids))
    };

    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let mut ranked_map = match index.main.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    // facet filters deletion
    if let Some(attributes_for_facetting) = index.main.attributes_for_faceting(writer)? {
        let facet_map = facets::facet_map_from_docids(writer, &index, &internal_docids, &attributes_for_facetting)?;
        index.facets.remove(writer, facet_map)?;
    }

    // collect the ranked attributes according to the schema
    let ranked_fields = schema.ranked();

    let mut words_document_ids = HashMap::new();
    for id in internal_docids.iter().cloned() {
        // remove all the ranked attributes from the ranked_map
        for ranked_attr in ranked_fields {
            ranked_map.remove(id, *ranked_attr);
        }

        let words = index.docs_words.doc_words(writer, id)?;
        if !words.is_empty() {
            let mut stream = words.stream();
            while let Some(word) = stream.next() {
                let word = word.to_vec();
                words_document_ids
                    .entry(word)
                    .or_insert_with(Vec::new)
                    .push(id);
            }
        }
    }

    let mut deleted_documents = HashSet::new();
    let mut removed_words = BTreeSet::new();
    for (word, document_ids) in words_document_ids {
        let document_ids = SetBuf::from_dirty(document_ids);

        if let Some(postings) = index.postings_lists.postings_list(writer, &word)? {
            let op = DifferenceByKey::new(&postings.matches, &document_ids, |d| d.document_id, |id| *id);
            let doc_indexes = op.into_set_buf();

            if !doc_indexes.is_empty() {
                index.postings_lists.put_postings_list(writer, &word, &doc_indexes)?;
            } else {
                index.postings_lists.del_postings_list(writer, &word)?;
                removed_words.insert(word);
            }
        }

        for id in document_ids {
            index.documents_fields_counts.del_all_document_fields_counts(writer, id)?;
            if index.documents_fields.del_all_document_fields(writer, id)? != 0 {
                deleted_documents.insert(id);
            }
        }
    }

    let deleted_documents_len = deleted_documents.len() as u64;
    for id in &deleted_documents {
        index.docs_words.del_doc_words(writer, *id)?;
    }

    let removed_words = fst::Set::from_iter(removed_words).unwrap();
    let words = {
        let words_set = index.main.words_fst(writer)?;
        let op = fst::set::OpBuilder::new()
            .add(words_set.stream())
            .add(removed_words.stream())
            .difference();

        let mut words_builder = SetBuilder::memory();
        words_builder.extend_stream(op).unwrap();
        words_builder.into_set()
    };

    index.main.put_words_fst(writer, &words)?;
    index.main.put_ranked_map(writer, &ranked_map)?;
    index.main.put_number_of_documents(writer, |old| old - deleted_documents_len)?;

    // We apply the changes to the user and internal ids
    index.main.remove_external_docids(writer, &external_docids)?;
    index.main.remove_internal_docids(writer, &internal_docids)?;

    compute_short_prefixes(writer, &words, index)?;

    // update is finished; update sorted document id cache with new state
    document_cache_remove_deleted(writer, index, &ranked_map, &deleted_documents)?;

    Ok(())
}

/// rebuilds the document id cache by either removing deleted documents from the existing cache,
/// and generating a new one from docs in store
fn document_cache_remove_deleted(writer: &mut MainWriter, index: &Index, ranked_map: &RankedMap, documents_to_delete: &HashSet<DocumentId>) -> MResult<()> {
    let new_cache = match index.main.sorted_document_ids_cache(writer)? {
        // only keep documents that are not in the list of deleted documents. Order is preserved,
        // no need to resort
        Some(old_cache) => {
            old_cache.iter().filter(|docid| !documents_to_delete.contains(docid)).cloned().collect::<Vec<_>>()
        }
        // couldn't find cached documents, try building a new cache from documents in store
        None => {
            let mut document_ids = index.main.internal_docids(writer)?.to_vec();
            super::cache_document_ids_sorted(writer, ranked_map, index, &mut document_ids)?;
            document_ids
        }
    };
    index.main.put_sorted_document_ids_cache(writer, &new_cache)?;
    Ok(())
}
