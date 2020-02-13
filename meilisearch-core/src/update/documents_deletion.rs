use std::collections::{BTreeSet, HashMap, HashSet};

use fst::{SetBuilder, Streamer};
use meilisearch_schema::Schema;
use sdset::{duo::DifferenceByKey, SetBuf, SetOperation};

use crate::database::{MainT, UpdateT};
use crate::database::{UpdateEvent, UpdateEventsEmitter};
use crate::serde::extract_document_id;
use crate::store;
use crate::update::{next_update_id, compute_short_prefixes, Update};
use crate::{DocumentId, Error, MResult, RankedMap};

pub struct DocumentsDeletion {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: UpdateEventsEmitter,
    documents: Vec<DocumentId>,
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
            documents: Vec::new(),
        }
    }

    pub fn delete_document_by_id(&mut self, document_id: DocumentId) {
        self.documents.push(document_id);
    }

    pub fn delete_document<D>(&mut self, schema: &Schema, document: D) -> MResult<()>
    where
        D: serde::Serialize,
    {
        let identifier = schema.identifier();
        let document_id = match extract_document_id(&identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        self.delete_document_by_id(document_id);

        Ok(())
    }

    pub fn finalize(self, writer: &mut heed::RwTxn<UpdateT>) -> MResult<u64> {
        let _ = self.updates_notifier.send(UpdateEvent::NewUpdate);
        let update_id = push_documents_deletion(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.documents,
        )?;
        Ok(update_id)
    }
}

impl Extend<DocumentId> for DocumentsDeletion {
    fn extend<T: IntoIterator<Item = DocumentId>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}

pub fn push_documents_deletion(
    writer: &mut heed::RwTxn<UpdateT>,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    deletion: Vec<DocumentId>,
) -> MResult<u64> {
    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::documents_deletion(deletion);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_documents_deletion(
    writer: &mut heed::RwTxn<MainT>,
    index: &store::Index,
    deletion: Vec<DocumentId>,
) -> MResult<()> {
    let idset = SetBuf::from_dirty(deletion);

    let schema = match index.main.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let mut ranked_map = match index.main.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    // collect the ranked attributes according to the schema
    let ranked_fields = schema.ranked();

    let mut words_document_ids = HashMap::new();
    for id in idset {
        // remove all the ranked attributes from the ranked_map
        for ranked_attr in ranked_fields {
            ranked_map.remove(id, *ranked_attr);
        }

        if let Some(words) = index.docs_words.doc_words(writer, id)? {
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
    for id in deleted_documents {
        index.docs_words.del_doc_words(writer, id)?;
    }

    let removed_words = fst::Set::from_iter(removed_words).unwrap();
    let words = match index.main.words_fst(writer)? {
        Some(words_set) => {
            let op = fst::set::OpBuilder::new()
                .add(words_set.stream())
                .add(removed_words.stream())
                .difference();

            let mut words_builder = SetBuilder::memory();
            words_builder.extend_stream(op).unwrap();
            words_builder
                .into_inner()
                .and_then(fst::Set::from_bytes)
                .unwrap()
        }
        None => fst::Set::default(),
    };

    index.main.put_words_fst(writer, &words)?;
    index.main.put_ranked_map(writer, &ranked_map)?;
    index.main.put_number_of_documents(writer, |old| old - deleted_documents_len)?;

    compute_short_prefixes(writer, index)?;

    Ok(())
}
