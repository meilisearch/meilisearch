use std::collections::HashSet;
use std::sync::Arc;

use fst::{SetBuilder, set::OpBuilder};
use sdset::{SetOperation, duo::Union};
use serde::Serialize;

use crate::RankedMap;
use crate::database::{Error, Index, index::Cache, apply_documents_deletion};
use crate::indexer::Indexer;
use crate::serde::{extract_document_id, Serializer, RamDocumentStore};

pub struct DocumentsAddition<'a, D> {
    index: &'a Index,
    documents: Vec<D>,
}

impl<'a, D> DocumentsAddition<'a, D> {
    pub fn new(index: &'a Index) -> DocumentsAddition<'a, D> {
        DocumentsAddition { index, documents: Vec::new() }
    }

    pub fn update_document(&mut self, document: D) {
        self.documents.push(document);
    }

    pub fn finalize(self) -> Result<u64, Error>
    where D: serde::Serialize
    {
        self.index.push_documents_addition(self.documents)
    }
}

pub fn apply_documents_addition(
    index: &Index,
    mut ranked_map: RankedMap,
    addition: Vec<rmpv::Value>,
) -> Result<(), Error>
{
    let mut document_ids = HashSet::new();
    let mut document_store = RamDocumentStore::new();
    let mut indexer = Indexer::new();

    let schema = &index.schema();
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

    let ref_index = index.as_ref();
    let docs_words = ref_index.docs_words_index;
    let documents = ref_index.documents_index;
    let main = ref_index.main_index;
    let words = ref_index.words_index;

    // 1. remove the previous documents match indexes
    let documents_to_insert = document_ids.iter().cloned().collect();
    apply_documents_deletion(index, ranked_map.clone(), documents_to_insert)?;

    // 2. insert new document attributes in the database
    for ((id, attr), value) in document_store.into_inner() {
        documents.set_document_field(id, attr, value)?;
    }

    let indexed = indexer.build();
    let mut delta_words_builder = SetBuilder::memory();

    for (word, delta_set) in indexed.words_doc_indexes {
        delta_words_builder.insert(&word).unwrap();

        let set = match words.doc_indexes(&word)? {
            Some(set) => Union::new(&set, &delta_set).into_set_buf(),
            None => delta_set,
        };

        words.set_doc_indexes(&word, &set)?;
    }

    for (id, words) in indexed.docs_words {
        docs_words.set_doc_words(id, &words)?;
    }

    let delta_words = delta_words_builder
        .into_inner()
        .and_then(fst::Set::from_bytes)
        .unwrap();

    let words = match main.words_set()? {
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

    main.set_words_set(&words)?;
    main.set_ranked_map(&ranked_map)?;

    let inserted_documents_len = document_ids.len() as u64;
    let number_of_documents = main.set_number_of_documents(|old| old + inserted_documents_len)?;

    // update the "consistent" view of the Index
    let cache = ref_index.cache;
    let words = Arc::new(words);
    let synonyms = cache.synonyms.clone();
    let schema = cache.schema.clone();

    let cache = Cache { words, synonyms, schema, ranked_map, number_of_documents };
    index.cache.store(Arc::new(cache));

    Ok(())
}
