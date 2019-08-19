use std::collections::HashSet;
use std::sync::Arc;

use meilidb_core::DocumentId;
use fst::{SetBuilder, set::OpBuilder};
use sdset::{SetOperation, duo::Union};

use crate::indexer::Indexer;
use crate::serde::{extract_document_id, Serializer, RamDocumentStore};
use crate::RankedMap;

use super::{Error, Index, DocumentsDeletion};
use super::index::Cache;

pub struct DocumentsAddition<'a> {
    inner: &'a Index,
    document_ids: HashSet<DocumentId>,
    document_store: RamDocumentStore,
    indexer: Indexer,
    ranked_map: RankedMap,
}

impl<'a> DocumentsAddition<'a> {
    pub fn new(inner: &'a Index, ranked_map: RankedMap) -> DocumentsAddition<'a> {
        DocumentsAddition {
            inner,
            document_ids: HashSet::new(),
            document_store: RamDocumentStore::new(),
            indexer: Indexer::new(),
            ranked_map,
        }
    }

    pub fn update_document<D>(&mut self, document: D) -> Result<(), Error>
    where D: serde::Serialize,
    {
        let schema = &self.inner.schema();
        let identifier = schema.identifier_name();

        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        // 1. store the document id for future deletion
        self.document_ids.insert(document_id);

        // 2. index the document fields in ram stores
        let serializer = Serializer {
            schema,
            document_store: &mut self.document_store,
            indexer: &mut self.indexer,
            ranked_map: &mut self.ranked_map,
            document_id,
        };

        document.serialize(serializer)?;

        Ok(())
    }

    pub fn finalize(self) -> Result<(), Error> {
        let ref_index = self.inner.as_ref();
        let docs_words = ref_index.docs_words_index;
        let documents = ref_index.documents_index;
        let main = ref_index.main_index;
        let words = ref_index.words_index;

        // 1. remove the previous documents match indexes
        let mut documents_deletion = DocumentsDeletion::new(self.inner, self.ranked_map.clone());
        documents_deletion.extend(self.document_ids);
        documents_deletion.finalize()?;

        // 2. insert new document attributes in the database
        for ((id, attr), value) in self.document_store.into_inner() {
            documents.set_document_field(id, attr, value)?;
        }

        let indexed = self.indexer.build();
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
        main.set_ranked_map(&self.ranked_map)?;

        // update the "consistent" view of the Index
        let cache = ref_index.cache;
        let words = Arc::new(words);
        let ranked_map = self.ranked_map;
        let synonyms = cache.synonyms.clone();
        let schema = cache.schema.clone();

        let cache = Cache { words, synonyms, schema, ranked_map };
        self.inner.cache.store(Arc::new(cache));

        Ok(())
    }
}
