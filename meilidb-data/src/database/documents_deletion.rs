use std::collections::{HashMap, BTreeSet};
use std::sync::Arc;

use fst::{SetBuilder, Streamer};
use meilidb_core::DocumentId;
use sdset::{SetBuf, SetOperation, duo::DifferenceByKey};

use crate::RankedMap;
use crate::serde::extract_document_id;

use super::{Index, Error, InnerIndex};

pub struct DocumentsDeletion<'a> {
    inner: &'a Index,
    documents: Vec<DocumentId>,
    ranked_map: RankedMap,
}

impl<'a> DocumentsDeletion<'a> {
    pub fn new(inner: &'a Index, ranked_map: RankedMap) -> DocumentsDeletion {
        DocumentsDeletion { inner, documents: Vec::new(), ranked_map }
    }

    fn delete_document_by_id(&mut self, id: DocumentId) {
        self.documents.push(id);
    }

    pub fn delete_document<D>(&mut self, document: D) -> Result<(), Error>
    where D: serde::Serialize,
    {
        let schema = &self.inner.lease_inner().schema;
        let identifier = schema.identifier_name();

        let document_id = match extract_document_id(&identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        self.delete_document_by_id(document_id);

        Ok(())
    }

    pub fn finalize(mut self) -> Result<(), Error> {
        let lease_inner = self.inner.lease_inner();
        let docs_words = &lease_inner.raw.docs_words;
        let documents = &lease_inner.raw.documents;
        let main = &lease_inner.raw.main;
        let schema = &lease_inner.schema;
        let words = &lease_inner.raw.words;

        let idset = SetBuf::from_dirty(self.documents);

        // collect the ranked attributes according to the schema
        let ranked_attrs: Vec<_> = schema.iter()
            .filter_map(|(_, attr, prop)| {
                if prop.is_ranked() { Some(attr) } else { None }
            })
            .collect();

        let mut words_document_ids = HashMap::new();
        for id in idset {
            // remove all the ranked attributes from the ranked_map
            for ranked_attr in &ranked_attrs {
                self.ranked_map.remove(id, *ranked_attr);
            }

            if let Some(words) = docs_words.doc_words(id)? {
                let mut stream = words.stream();
                while let Some(word) = stream.next() {
                    let word = word.to_vec();
                    words_document_ids.entry(word).or_insert_with(Vec::new).push(id);
                }
            }
        }

        let mut removed_words = BTreeSet::new();
        for (word, document_ids) in words_document_ids {
            let document_ids = SetBuf::from_dirty(document_ids);

            if let Some(doc_indexes) = words.doc_indexes(&word)? {
                let op = DifferenceByKey::new(&doc_indexes, &document_ids, |d| d.document_id, |id| *id);
                let doc_indexes = op.into_set_buf();

                if !doc_indexes.is_empty() {
                    words.set_doc_indexes(&word, &doc_indexes)?;
                } else {
                    words.del_doc_indexes(&word)?;
                    removed_words.insert(word);
                }
            }

            for id in document_ids {
                documents.del_all_document_fields(id)?;
                docs_words.del_doc_words(id)?;
            }
        }

        let removed_words = fst::Set::from_iter(removed_words).unwrap();
        let words = match main.words_set()? {
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
            },
            None => fst::Set::default(),
        };

        main.set_words_set(&words)?;
        main.set_ranked_map(&self.ranked_map)?;

        // update the "consistent" view of the Index
        let ranked_map = lease_inner.ranked_map.clone();
        let synonyms = fst::Set::from_bytes(lease_inner.synonyms.as_fst().to_vec()).unwrap(); // clone()
        let schema = lease_inner.schema.clone();
        let raw = lease_inner.raw.clone();
        lease_inner.raw.compact();

        let inner = InnerIndex { words, synonyms, schema, ranked_map, raw };
        self.inner.0.store(Arc::new(inner));

        Ok(())
    }
}

impl<'a> Extend<DocumentId> for DocumentsDeletion<'a> {
    fn extend<T: IntoIterator<Item=DocumentId>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}
