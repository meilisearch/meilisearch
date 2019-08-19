use std::collections::HashSet;
use std::sync::Arc;

use arc_swap::{ArcSwap, Guard};
use meilidb_core::criterion::Criteria;
use meilidb_core::{DocIndex, Store, DocumentId, QueryBuilder};
use meilidb_schema::Schema;
use sdset::SetBuf;
use serde::de;

use crate::ranked_map::RankedMap;
use crate::serde::{Deserializer, DeserializerError};

use super::Error;

pub use self::custom_settings_index::CustomSettingsIndex;
use self::docs_words_index::DocsWordsIndex;
use self::documents_index::DocumentsIndex;
use self::main_index::MainIndex;
use self::synonyms_index::SynonymsIndex;
use self::updates_index::UpdatesIndex;
use self::words_index::WordsIndex;

use super::{
    DocumentsAddition, DocumentsDeletion,
    SynonymsAddition, SynonymsDeletion,
};

mod custom_settings_index;
mod docs_words_index;
mod documents_index;
mod main_index;
mod synonyms_index;
mod updates_index;
mod words_index;

#[derive(Copy, Clone)]
pub struct IndexStats {
    pub number_of_words: usize,
    pub number_of_documents: usize,
    pub number_attrs_in_ranked_map: usize,
}

#[derive(Clone)]
pub struct Index {
    pub(crate) cache: ArcSwap<Cache>,

    // TODO this will be a snapshot in the future
    main_index: MainIndex,
    synonyms_index: SynonymsIndex,
    words_index: WordsIndex,
    docs_words_index: DocsWordsIndex,
    documents_index: DocumentsIndex,
    custom_settings_index: CustomSettingsIndex,
    updates_index: UpdatesIndex,
}

pub(crate) struct Cache {
    pub words: Arc<fst::Set>,
    pub synonyms: Arc<fst::Set>,
    pub schema: Schema,
    pub ranked_map: RankedMap,
}

impl Index {
    pub fn new(db: &sled::Db, name: &str) -> Result<Index, Error> {
        let main_index = db.open_tree(name).map(MainIndex)?;
        let synonyms_index = db.open_tree(format!("{}-synonyms", name)).map(SynonymsIndex)?;
        let words_index = db.open_tree(format!("{}-words", name)).map(WordsIndex)?;
        let docs_words_index = db.open_tree(format!("{}-docs-words", name)).map(DocsWordsIndex)?;
        let documents_index = db.open_tree(format!("{}-documents", name)).map(DocumentsIndex)?;
        let custom_settings_index = db.open_tree(format!("{}-custom", name)).map(CustomSettingsIndex)?;

        let updates = db.open_tree(format!("{}-updates", name))?;
        let updates_results = db.open_tree(format!("{}-updates-results", name))?;
        let updates_index = UpdatesIndex::new(db.clone(), updates, updates_results);

        let words = match main_index.words_set()? {
            Some(words) => Arc::new(words),
            None => Arc::new(fst::Set::default()),
        };

        let synonyms = match main_index.synonyms_set()? {
            Some(synonyms) => Arc::new(synonyms),
            None => Arc::new(fst::Set::default()),
        };

        let schema = match main_index.schema()? {
            Some(schema) => schema,
            None => return Err(Error::SchemaMissing),
        };

        let ranked_map = match main_index.ranked_map()? {
            Some(map) => map,
            None => RankedMap::default(),
        };

        let cache = Cache { words, synonyms, schema, ranked_map };
        let cache = ArcSwap::from_pointee(cache);

        Ok(Index {
            cache,
            main_index,
            synonyms_index,
            words_index,
            docs_words_index,
            documents_index,
            custom_settings_index,
            updates_index,
        })
    }

    pub fn with_schema(db: &sled::Db, name: &str, schema: Schema) -> Result<Index, Error> {
        let main_index = db.open_tree(name).map(MainIndex)?;
        let synonyms_index = db.open_tree(format!("{}-synonyms", name)).map(SynonymsIndex)?;
        let words_index = db.open_tree(format!("{}-words", name)).map(WordsIndex)?;
        let docs_words_index = db.open_tree(format!("{}-docs-words", name)).map(DocsWordsIndex)?;
        let documents_index = db.open_tree(format!("{}-documents", name)).map(DocumentsIndex)?;
        let custom_settings_index = db.open_tree(format!("{}-custom", name)).map(CustomSettingsIndex)?;

        let updates = db.open_tree(format!("{}-updates", name))?;
        let updates_results = db.open_tree(format!("{}-updates-results", name))?;
        let updates_index = UpdatesIndex::new(db.clone(), updates, updates_results);

        let words = match main_index.words_set()? {
            Some(words) => Arc::new(words),
            None => Arc::new(fst::Set::default()),
        };

        let synonyms = match main_index.synonyms_set()? {
            Some(synonyms) => Arc::new(synonyms),
            None => Arc::new(fst::Set::default()),
        };

        match main_index.schema()? {
            Some(current) => if current != schema {
                return Err(Error::SchemaDiffer)
            },
            None => main_index.set_schema(&schema)?,
        }

        let ranked_map = match main_index.ranked_map()? {
            Some(map) => map,
            None => RankedMap::default(),
        };

        let cache = Cache { words, synonyms, schema, ranked_map };
        let cache = ArcSwap::from_pointee(cache);

        Ok(Index {
            cache,
            main_index,
            synonyms_index,
            words_index,
            docs_words_index,
            documents_index,
            custom_settings_index,
            updates_index,
        })
    }

    pub fn stats(&self) -> sled::Result<IndexStats> {
        let cache = self.cache.load();
        Ok(IndexStats {
            number_of_words: cache.words.len(),
            number_of_documents: self.documents_index.len()?,
            number_attrs_in_ranked_map: cache.ranked_map.len(),
        })
    }

    pub fn query_builder(&self) -> QueryBuilder<RefIndex> {
        let ref_index = self.as_ref();
        QueryBuilder::new(ref_index)
    }

    pub fn query_builder_with_criteria<'c>(
        &self,
        criteria: Criteria<'c>,
    ) -> QueryBuilder<'c, RefIndex>
    {
        let ref_index = self.as_ref();
        QueryBuilder::with_criteria(ref_index, criteria)
    }

    pub fn as_ref(&self) -> RefIndex {
        RefIndex {
            cache: self.cache.load(),
            main_index: &self.main_index,
            synonyms_index: &self.synonyms_index,
            words_index: &self.words_index,
            docs_words_index: &self.docs_words_index,
            documents_index: &self.documents_index,
            custom_settings_index: &self.custom_settings_index,
        }
    }

    pub fn schema(&self) -> Schema {
        self.cache.load().schema.clone()
    }

    pub fn custom_settings(&self) -> CustomSettingsIndex {
        self.custom_settings_index.clone()
    }

    pub fn documents_addition(&self) -> DocumentsAddition {
        let ranked_map = self.cache.load().ranked_map.clone();
        DocumentsAddition::new(self, ranked_map)
    }

    pub fn documents_deletion(&self) -> DocumentsDeletion {
        let ranked_map = self.cache.load().ranked_map.clone();
        DocumentsDeletion::new(self, ranked_map)
    }

    pub fn synonyms_addition(&self) -> SynonymsAddition {
        SynonymsAddition::new(self)
    }

    pub fn synonyms_deletion(&self) -> SynonymsDeletion {
        SynonymsDeletion::new(self)
    }

    pub fn document<T>(
        &self,
        fields: Option<&HashSet<&str>>,
        id: DocumentId,
    ) -> Result<Option<T>, DeserializerError>
    where T: de::DeserializeOwned,
    {
        let schema = self.schema();
        let fields = match fields {
            Some(fields) => fields.into_iter().map(|name| schema.attribute(name)).collect(),
            None => None,
        };

        let mut deserializer = Deserializer {
            document_id: id,
            index: &self,
            fields: fields.as_ref(),
        };

        // TODO: currently we return an error if all document fields are missing,
        //       returning None would have been better
        T::deserialize(&mut deserializer).map(Some)
    }
}

pub struct RefIndex<'a> {
    pub(crate) cache: Guard<'static, Arc<Cache>>,
    pub main_index: &'a MainIndex,
    pub synonyms_index: &'a SynonymsIndex,
    pub words_index: &'a WordsIndex,
    pub docs_words_index: &'a DocsWordsIndex,
    pub documents_index: &'a DocumentsIndex,
    pub custom_settings_index: &'a CustomSettingsIndex,
}

impl Store for RefIndex<'_> {
    type Error = Error;

    fn words(&self) -> Result<&fst::Set, Self::Error> {
        Ok(&self.cache.words)
    }

    fn word_indexes(&self, word: &[u8]) -> Result<Option<SetBuf<DocIndex>>, Self::Error> {
        Ok(self.words_index.doc_indexes(word)?)
    }

    fn synonyms(&self) -> Result<&fst::Set, Self::Error> {
        Ok(&self.cache.synonyms)
    }

    fn alternatives_to(&self, word: &[u8]) -> Result<Option<fst::Set>, Self::Error> {
        Ok(self.synonyms_index.alternatives_to(word)?)
    }
}
