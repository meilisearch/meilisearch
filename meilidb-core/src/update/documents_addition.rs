use std::collections::{HashMap, HashSet};

use fst::{set::OpBuilder, SetBuilder};
use sdset::{duo::Union, SetOperation};
use serde::Serialize;

use crate::raw_indexer::RawIndexer;
use crate::serde::{extract_document_id, RamDocumentStore, Serializer};
use crate::store;
use crate::update::{apply_documents_deletion, next_update_id, Update};
use crate::{Error, MResult, RankedMap};

pub struct DocumentsAddition<D> {
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    updates_notifier: crossbeam_channel::Sender<()>,
    documents: Vec<D>,
}

impl<D> DocumentsAddition<D> {
    pub fn new(
        updates_store: store::Updates,
        updates_results_store: store::UpdatesResults,
        updates_notifier: crossbeam_channel::Sender<()>,
    ) -> DocumentsAddition<D> {
        DocumentsAddition {
            updates_store,
            updates_results_store,
            updates_notifier,
            documents: Vec::new(),
        }
    }

    pub fn update_document(&mut self, document: D) {
        self.documents.push(document);
    }

    pub fn finalize(self, writer: &mut zlmdb::RwTxn) -> MResult<u64>
    where
        D: serde::Serialize,
    {
        let _ = self.updates_notifier.send(());
        let update_id = push_documents_addition(
            writer,
            self.updates_store,
            self.updates_results_store,
            self.documents,
        )?;
        Ok(update_id)
    }
}

impl<D> Extend<D> for DocumentsAddition<D> {
    fn extend<T: IntoIterator<Item = D>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}

pub fn push_documents_addition<D: serde::Serialize>(
    writer: &mut zlmdb::RwTxn,
    updates_store: store::Updates,
    updates_results_store: store::UpdatesResults,
    addition: Vec<D>,
) -> MResult<u64> {
    let mut values = Vec::with_capacity(addition.len());
    for add in addition {
        let vec = serde_json::to_vec(&add)?;
        let add = serde_json::from_slice(&vec)?;
        values.push(add);
    }

    let last_update_id = next_update_id(writer, updates_store, updates_results_store)?;

    let update = Update::DocumentsAddition(values);
    updates_store.put_update(writer, last_update_id, &update)?;

    Ok(last_update_id)
}

pub fn apply_documents_addition(
    writer: &mut zlmdb::RwTxn,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    mut ranked_map: RankedMap,
    addition: Vec<serde_json::Value>,
) -> MResult<()> {
    let mut document_ids = HashSet::new();
    let mut document_store = RamDocumentStore::new();
    let mut document_fields_counts = HashMap::new();
    let mut indexer = RawIndexer::new();

    let schema = match main_store.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let identifier = schema.identifier_name();

    for document in addition {
        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        // 1. store the document id for future deletion
        document_ids.insert(document_id);

        // 2. index the document fields in ram stores
        let serializer = Serializer {
            schema: &schema,
            document_store: &mut document_store,
            document_fields_counts: &mut document_fields_counts,
            indexer: &mut indexer,
            ranked_map: &mut ranked_map,
            document_id,
        };

        document.serialize(serializer)?;
    }

    // 1. remove the previous documents match indexes
    let documents_to_insert = document_ids.iter().cloned().collect();
    apply_documents_deletion(
        writer,
        main_store,
        documents_fields_store,
        documents_fields_counts_store,
        postings_lists_store,
        docs_words_store,
        ranked_map.clone(),
        documents_to_insert,
    )?;

    // 2. insert new document attributes in the database
    for ((id, attr), value) in document_store.into_inner() {
        documents_fields_store.put_document_field(writer, id, attr, &value)?;
    }

    // 3. insert new document attributes counts
    for ((id, attr), count) in document_fields_counts {
        documents_fields_counts_store.put_document_field_count(writer, id, attr, count)?;
    }

    let indexed = indexer.build();
    let mut delta_words_builder = SetBuilder::memory();

    for (word, delta_set) in indexed.words_doc_indexes {
        delta_words_builder.insert(&word).unwrap();

        let set = match postings_lists_store.postings_list(writer, &word)? {
            Some(set) => Union::new(&set, &delta_set).into_set_buf(),
            None => delta_set,
        };

        postings_lists_store.put_postings_list(writer, &word, &set)?;
    }

    for (id, words) in indexed.docs_words {
        docs_words_store.put_doc_words(writer, id, &words)?;
    }

    let delta_words = delta_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    let words = match main_store.words_fst(writer)? {
        Some(words) => {
            let op = OpBuilder::new()
                .add(words.stream())
                .add(delta_words.stream())
                .r#union();

            let mut words_builder = SetBuilder::memory();
            words_builder.extend_stream(op).unwrap();
            words_builder
                .into_inner()
                .and_then(fst::Set::from_bytes)
                .unwrap()
        }
        None => delta_words,
    };

    main_store.put_words_fst(writer, &words)?;
    main_store.put_ranked_map(writer, &ranked_map)?;

    let inserted_documents_len = document_ids.len() as u64;
    main_store.put_number_of_documents(writer, |old| old + inserted_documents_len)?;

    Ok(())
}
