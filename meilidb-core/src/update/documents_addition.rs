use std::collections::HashMap;

use fst::{set::OpBuilder, SetBuilder};
use sdset::{duo::Union, SetOperation};
use serde::Serialize;

use crate::raw_indexer::RawIndexer;
use crate::serde::{extract_document_id, serialize_value, Serializer};
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

    pub fn finalize(self, writer: &mut heed::RwTxn) -> MResult<u64>
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
    writer: &mut heed::RwTxn,
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
    writer: &mut heed::RwTxn,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    addition: Vec<serde_json::Value>,
) -> MResult<()> {
    let mut documents_additions = HashMap::new();

    let schema = match main_store.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let identifier = schema.identifier_name();

    // 1. store documents ids for future deletion
    for document in addition {
        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        documents_additions.insert(document_id, document);
    }

    // 2. remove the documents posting lists
    let number_of_inserted_documents = documents_additions.len();
    let documents_ids = documents_additions.iter().map(|(id, _)| *id).collect();
    apply_documents_deletion(
        writer,
        main_store,
        documents_fields_store,
        documents_fields_counts_store,
        postings_lists_store,
        docs_words_store,
        documents_ids,
    )?;

    let mut ranked_map = match main_store.ranked_map(writer)? {
        Some(ranked_map) => ranked_map,
        None => RankedMap::default(),
    };

    let stop_words = match main_store.stop_words_fst(writer)? {
        Some(stop_words) => stop_words,
        None => fst::Set::default(),
    };

    // 3. index the documents fields in the stores
    let mut indexer = RawIndexer::new(stop_words);

    for (document_id, document) in documents_additions {
        let serializer = Serializer {
            txn: writer,
            schema: &schema,
            document_store: documents_fields_store,
            document_fields_counts: documents_fields_counts_store,
            indexer: &mut indexer,
            ranked_map: &mut ranked_map,
            document_id,
        };

        document.serialize(serializer)?;
    }

    write_documents_addition_index(
        writer,
        main_store,
        postings_lists_store,
        docs_words_store,
        ranked_map,
        number_of_inserted_documents,
        indexer,
    )
}

pub fn reindex_all_documents(
    writer: &mut heed::RwTxn,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    documents_fields_counts_store: store::DocumentsFieldsCounts,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
) -> MResult<()> {
    let schema = match main_store.schema(writer)? {
        Some(schema) => schema,
        None => return Err(Error::SchemaMissing),
    };

    let mut ranked_map = RankedMap::default();

    // 1. retrieve all documents ids
    let mut documents_ids_to_reindex = Vec::new();
    for result in documents_fields_counts_store.documents_ids(writer)? {
        let document_id = result?;
        documents_ids_to_reindex.push(document_id);
    }

    // 2. remove the documents posting lists
    let number_of_inserted_documents = documents_ids_to_reindex.len();
    main_store.put_words_fst(writer, &fst::Set::default())?;
    main_store.put_ranked_map(writer, &ranked_map)?;
    main_store.put_number_of_documents(writer, |_| 0)?;
    postings_lists_store.clear(writer)?;
    docs_words_store.clear(writer)?;

    let stop_words = match main_store.stop_words_fst(writer)? {
        Some(stop_words) => stop_words,
        None => fst::Set::default(),
    };

    // 3. re-index one document by one document (otherwise we make the borrow checker unhappy)
    let mut indexer = RawIndexer::new(stop_words);
    let mut ram_store = HashMap::new();

    for document_id in documents_ids_to_reindex {
        for result in documents_fields_store.document_fields(writer, document_id)? {
            let (attr, bytes) = result?;
            let value: serde_json::Value = serde_json::from_slice(bytes)?;
            ram_store.insert((document_id, attr), value);
        }

        for ((docid, attr), value) in ram_store.drain() {
            serialize_value(
                writer,
                attr,
                schema.props(attr),
                docid,
                documents_fields_store,
                documents_fields_counts_store,
                &mut indexer,
                &mut ranked_map,
                &value,
            )?;
        }
    }

    // 4. write the new index in the main store
    write_documents_addition_index(
        writer,
        main_store,
        postings_lists_store,
        docs_words_store,
        ranked_map,
        number_of_inserted_documents,
        indexer,
    )
}

pub fn write_documents_addition_index(
    writer: &mut heed::RwTxn,
    main_store: store::Main,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    ranked_map: RankedMap,
    number_of_inserted_documents: usize,
    indexer: RawIndexer,
) -> MResult<()> {
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
    main_store.put_number_of_documents(writer, |old| old + number_of_inserted_documents as u64)?;

    Ok(())
}
