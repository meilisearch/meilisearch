use std::collections::{HashMap, HashSet, BTreeSet};
use std::sync::Arc;

use fst::{SetBuilder, Streamer};
use meilidb_core::DocumentId;
use sdset::{SetBuf, SetOperation, duo::DifferenceByKey};

use crate::RankedMap;
use crate::serde::extract_document_id;

use crate::database::{Index, Error, index::Cache};

pub struct DocumentsDeletion<'a> {
    index: &'a Index,
    documents: Vec<DocumentId>,
}

impl<'a> DocumentsDeletion<'a> {
    pub fn new(index: &'a Index) -> DocumentsDeletion<'a> {
        DocumentsDeletion { index, documents: Vec::new() }
    }

    pub fn delete_document_by_id(&mut self, document_id: DocumentId) {
        self.documents.push(document_id);
    }

    pub fn delete_document<D>(&mut self, document: D) -> Result<(), Error>
    where D: serde::Serialize,
    {
        let schema = self.index.schema();
        let identifier = schema.identifier_name();
        let document_id = match extract_document_id(identifier, &document)? {
            Some(id) => id,
            None => return Err(Error::MissingDocumentId),
        };

        self.delete_document_by_id(document_id);

        Ok(())
    }

    pub fn finalize(self) -> Result<u64, Error> {
        self.index.push_documents_deletion(self.documents)
    }
}

impl Extend<DocumentId> for DocumentsDeletion<'_> {
    fn extend<T: IntoIterator<Item=DocumentId>>(&mut self, iter: T) {
        self.documents.extend(iter)
    }
}

pub fn apply_documents_deletion(
    index: &Index,
    mut ranked_map: RankedMap,
    deletion: Vec<DocumentId>,
) -> Result<(), Error>
{
    let ref_index = index.as_ref();
    let schema = index.schema();
    let docs_words = ref_index.docs_words_index;
    let documents = ref_index.documents_index;
    let main = ref_index.main_index;
    let words = ref_index.words_index;

    let idset = SetBuf::from_dirty(deletion);

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
            ranked_map.remove(id, *ranked_attr);
        }

        if let Some(words) = docs_words.doc_words(id)? {
            let mut stream = words.stream();
            while let Some(word) = stream.next() {
                let word = word.to_vec();
                words_document_ids.entry(word).or_insert_with(Vec::new).push(id);
            }
        }
    }

    let mut deleted_documents = HashSet::new();
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
            if documents.del_all_document_fields(id)? != 0 {
                deleted_documents.insert(id);
            }
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
    main.set_ranked_map(&ranked_map)?;

    let deleted_documents_len = deleted_documents.len() as u64;
    let number_of_documents = main.set_number_of_documents(|old| old - deleted_documents_len)?;

    // update the "consistent" view of the Index
    let cache = ref_index.cache;
    let words = Arc::new(words);
    let synonyms = cache.synonyms.clone();
    let schema = cache.schema.clone();

    let cache = Cache { words, synonyms, schema, ranked_map, number_of_documents };
    index.cache.store(Arc::new(cache));

    Ok(())
}
