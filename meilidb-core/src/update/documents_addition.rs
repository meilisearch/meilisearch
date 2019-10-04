use std::collections::HashSet;

use fst::{SetBuilder, set::OpBuilder};
use meilidb_schema::Schema;
use sdset::{SetOperation, duo::Union};
use serde::Serialize;

use crate::raw_indexer::RawIndexer;
use crate::serde::{extract_document_id, Serializer, RamDocumentStore};
use crate::store;
use crate::update::{push_documents_addition, apply_documents_deletion};
use crate::{Error, RankedMap};

pub struct DocumentsAddition<D> {
    updates_store: store::Updates,
    documents: Vec<D>,
}

impl<D> DocumentsAddition<D> {
    pub fn new(updates_store: store::Updates) -> DocumentsAddition<D> {
        DocumentsAddition { updates_store, documents: Vec::new() }
    }

    pub fn update_document(&mut self, document: D) {
        self.documents.push(document);
    }

    pub fn finalize(self, writer: &mut rkv::Writer) -> Result<u64, Error>
    where D: serde::Serialize
    {
        push_documents_addition(writer, self.updates_store, self.documents)
    }
}

pub fn apply_documents_addition(
    writer: &mut rkv::Writer,
    main_store: store::Main,
    documents_fields_store: store::DocumentsFields,
    postings_lists_store: store::PostingsLists,
    docs_words_store: store::DocsWords,
    schema: &Schema,
    mut ranked_map: RankedMap,
    addition: Vec<rmpv::Value>,
) -> Result<(), Error>
{
    let mut document_ids = HashSet::new();
    let mut document_store = RamDocumentStore::new();
    let mut indexer = RawIndexer::new();

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
            schema,
            document_store: &mut document_store,
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
        postings_lists_store,
        docs_words_store,
        schema,
        ranked_map.clone(),
        documents_to_insert,
    )?;

    // 2. insert new document attributes in the database
    for ((id, attr), value) in document_store.into_inner() {
        documents_fields_store.put_document_field(writer, id, attr, &value)?;
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
        },
        None => delta_words,
    };

    main_store.put_words_fst(writer, &words)?;
    main_store.put_ranked_map(writer, &ranked_map)?;

    let inserted_documents_len = document_ids.len() as u64;
    main_store.put_number_of_documents(writer, |old| old + inserted_documents_len)?;

    Ok(())
}
